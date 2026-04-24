package dev.parser.checker
import javax.script.ScriptEngineManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeout
import okhttp3.Cookie
import okhttp3.CookieJar
import okhttp3.Headers
import okhttp3.HttpUrl
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import org.json.JSONObject
import org.koitharu.kotatsu.parsers.MangaLoaderContext
import org.koitharu.kotatsu.parsers.MangaParser
import org.koitharu.kotatsu.parsers.bitmap.Bitmap
import org.koitharu.kotatsu.parsers.config.ConfigKey
import org.koitharu.kotatsu.parsers.config.MangaSourceConfig
import org.koitharu.kotatsu.parsers.exception.ContentUnavailableException
import org.koitharu.kotatsu.parsers.model.Manga
import org.koitharu.kotatsu.parsers.model.MangaChapter
import org.koitharu.kotatsu.parsers.model.MangaListFilter
import org.koitharu.kotatsu.parsers.model.MangaParserSource
import org.koitharu.kotatsu.parsers.model.MangaSource
import org.koitharu.kotatsu.parsers.model.SortOrder
import org.koitharu.kotatsu.parsers.network.CloudFlareHelper
import org.koitharu.kotatsu.parsers.network.UserAgents
import java.io.IOException
import java.net.SocketTimeoutException
import java.net.UnknownHostException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.system.measureTimeMillis

private const val STATUS_WORKING = "working"
private const val STATUS_BROKEN = "broken"
private const val STATUS_BLOCKED = "blocked"
private const val STATUS_UNKNOWN = "unknown"
private const val STATUS_SKIPPED = "skipped"

private const val CHECK_LIST = "list"
private const val CHECK_SEARCH = "search"
private const val CHECK_DETAILS = "details"
private const val CHECK_CHAPTERS = "chapters"
private const val CHECK_IMAGES = "images"

private val CHECK_LABELS = linkedMapOf(
    CHECK_LIST to "List",
    CHECK_SEARCH to "Search",
    CHECK_DETAILS to "Manga details",
    CHECK_CHAPTERS to "Chapters",
    CHECK_IMAGES to "Images",
)

data class StageResult(
    val status: String,
    val reason: String,
    val latencyMs: Long? = null,
    val count: Int? = null,
    val details: String? = null,
) {
    fun toJson(): JSONObject {
        return JSONObject().apply {
            put("status", status)
            put("reason", reason)
            if (latencyMs == null) put("latencyMs", JSONObject.NULL) else put("latencyMs", latencyMs)
            if (count == null) put("count", JSONObject.NULL) else put("count", count)
            if (details == null) put("details", JSONObject.NULL) else put("details", details.take(420))
        }
    }
}

data class SourceHealth(
    val status: String,
    val reason: String,
    val checkedAt: String = Instant.now().toString(),
    val latencyMs: Long? = null,
    val httpStatus: Int? = null,
    val finalUrl: String? = null,
    val resultCount: Int? = null,
    val details: String? = null,
    val checks: Map<String, StageResult> = emptyMap(),
) {
    fun toJson(): JSONObject {
        return JSONObject().apply {
            put("status", status)
            put("reason", reason)
            put("checkedAt", checkedAt)
            if (latencyMs == null) put("latencyMs", JSONObject.NULL) else put("latencyMs", latencyMs)
            if (httpStatus == null) put("httpStatus", JSONObject.NULL) else put("httpStatus", httpStatus)
            if (finalUrl == null) put("finalUrl", JSONObject.NULL) else put("finalUrl", finalUrl)
            if (resultCount == null) put("resultCount", JSONObject.NULL) else put("resultCount", resultCount)
            if (details == null) put("details", JSONObject.NULL) else put("details", details.take(420))

            val checksJson = JSONObject()
            for ((key, value) in checks) {
                checksJson.put(key, value.toJson())
            }
            put("checks", checksJson)
        }
    }
}

class ParserBlockedException(
    message: String,
    val targetUrl: String? = null,
) : IOException(message)

class ParserTooManyRequestsException(
    val targetUrl: String? = null,
    val retryAfterMs: Long = 0L,
) : IOException("Too many requests. Try again later")

class InMemoryCookieJar : CookieJar {
    private data class CookieKey(val host: String, val name: String)
    private val cache = ConcurrentHashMap<CookieKey, Cookie>()

    override fun loadForRequest(url: HttpUrl): List<Cookie> {
        val now = System.currentTimeMillis()
        return cache.values.filter { cookie ->
            cookie.matches(url) && cookie.expiresAt >= now
        }
    }

    override fun saveFromResponse(url: HttpUrl, cookies: List<Cookie>) {
        for (cookie in cookies) {
            cache[CookieKey(url.host, cookie.name)] = cookie
        }
    }
}

class DefaultParserSourceConfig : MangaSourceConfig {
    override fun <T> get(key: ConfigKey<T>): T = key.defaultValue
}

class CommonHeadersInterceptor(
    private val context: MangaLoaderContext,
) : Interceptor {

    override fun intercept(chain: Interceptor.Chain): Response {
        val request = chain.request()
        val source = request.tag(MangaSource::class.java)
        val parser = if (source is MangaParserSource) {
            runCatching { context.newParserInstance(source) }.getOrNull()
        } else {
            null
        }

        val headersBuilder = request.headers.newBuilder()
        val sourceHeaders: Headers? = parser?.getRequestHeaders()

        if (sourceHeaders != null) {
            for (name in sourceHeaders.names()) {
                if (request.header(name) == null) {
                    for (value in sourceHeaders.values(name)) {
                        headersBuilder.add(name, value)
                    }
                }
            }
        }

        if (request.header("User-Agent") == null) {
            headersBuilder.set("User-Agent", context.getDefaultUserAgent())
        }

        if (request.header("Referer") == null && parser != null) {
            headersBuilder.set("Referer", "https://${parser.domain}/")
        }

        val newRequest = request.newBuilder()
            .headers(headersBuilder.build())
            .build()

        return if (parser != null) {
            try {
                parser.intercept(ProxyChain(chain, newRequest))
            } catch (e: IOException) {
                throw e
            } catch (e: Throwable) {
                throw IOException("Parser interceptor failed: ${e.message}", e)
            }
        } else {
            chain.proceed(newRequest)
        }
    }

    private class ProxyChain(
        private val delegate: Interceptor.Chain,
        private val request: Request,
    ) : Interceptor.Chain by delegate {
        override fun request(): Request = request
    }
}

class ProtectionInterceptor : Interceptor {
    override fun intercept(chain: Interceptor.Chain): Response {
        val response = chain.proceed(chain.request())
        val protection = runCatching {
            CloudFlareHelper.checkResponseForProtection(response)
        }.getOrDefault(CloudFlareHelper.PROTECTION_NOT_DETECTED)

        if (protection != CloudFlareHelper.PROTECTION_NOT_DETECTED) {
            val url = response.request.url.toString()
            response.close()
            throw ParserBlockedException("Additional action required", url)
        }

        if (response.code == 403) {
            val url = response.request.url.toString()
            response.close()
            throw ParserBlockedException("Access denied (403)", url)
        }

        return response
    }
}

class RetryAndRateLimitInterceptor : Interceptor {
    private val hostLocks = ConcurrentHashMap<String, Any>()
    private val hostLastRequest = ConcurrentHashMap<String, Long>()
    private val delayMs = envLong("PARSER_CHECKER_RATE_DELAY_MS", 450L).coerceAtLeast(0L)
    private val retries = envInt("PARSER_CHECKER_RETRIES", 2).coerceAtLeast(0)
    private val backoffMs = envLong("PARSER_CHECKER_BACKOFF_MS", 800L).coerceAtLeast(0L)

    override fun intercept(chain: Interceptor.Chain): Response {
        val request = chain.request()
        var attempt = 0

        while (true) {
            waitForHost(request.url.host)
            val response = chain.proceed(request)

            if (response.code == 429) {
                val retryAfter = response.header("Retry-After")?.parseRetryAfterMs() ?: 0L
                val url = response.request.url.toString()
                response.close()

                if (attempt < retries) {
                    sleepForRetry(attempt, retryAfter)
                    attempt += 1
                    continue
                }

                throw ParserTooManyRequestsException(url, retryAfter)
            }

            if (response.code in setOf(500, 502, 503, 504) && attempt < retries) {
                response.close()
                sleepForRetry(attempt, 0L)
                attempt += 1
                continue
            }

            return response
        }
    }

    private fun waitForHost(host: String) {
        if (delayMs <= 0L) return

        val lock = hostLocks.getOrPut(host) { Any() }
        synchronized(lock) {
            val now = System.currentTimeMillis()
            val last = hostLastRequest[host] ?: 0L
            val wait = delayMs - (now - last)
            if (wait > 0L) {
                Thread.sleep(wait)
            }
            hostLastRequest[host] = System.currentTimeMillis()
        }
    }

    private fun sleepForRetry(attempt: Int, retryAfterMs: Long) {
        val computed = if (retryAfterMs > 0L) retryAfterMs else backoffMs * (1L shl attempt.coerceAtMost(4))
        Thread.sleep(computed.coerceIn(0L, 8_000L))
    }

    private fun String.parseRetryAfterMs(): Long {
        toLongOrNull()?.let { return TimeUnit.SECONDS.toMillis(it) }
        return runCatching {
            val epochMs = ZonedDateTime.parse(this, DateTimeFormatter.RFC_1123_DATE_TIME)
                .toInstant()
                .toEpochMilli()
            (epochMs - System.currentTimeMillis()).coerceAtLeast(0L)
        }.getOrDefault(0L)
    }
}

class ParserRuntimeContext : MangaLoaderContext() {
    override val cookieJar: CookieJar = InMemoryCookieJar()
    private val scriptEngineManager = ScriptEngineManager()

    override val httpClient: OkHttpClient = OkHttpClient.Builder()
        .cookieJar(cookieJar)
        .addInterceptor(RetryAndRateLimitInterceptor())
        .addInterceptor(ProtectionInterceptor())
        .addInterceptor(CommonHeadersInterceptor(this))
        .connectTimeout(20, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(20, TimeUnit.SECONDS)
        .callTimeout(75, TimeUnit.SECONDS)
        .build()

    override suspend fun evaluateJs(script: String): String? {
        return runInterruptible(Dispatchers.Default) {
            val engine = scriptEngineManager.getEngineByName("nashorn")
                ?: return@runInterruptible null

            engine.eval(script)
                ?.toString()
                ?.takeUnless { it.isBlank() || it == "null" }
        }
    }

    override fun getConfig(source: MangaSource): MangaSourceConfig = DefaultParserSourceConfig()

    override fun getDefaultUserAgent(): String = UserAgents.FIREFOX_DESKTOP

    override fun redrawImageResponse(response: Response, redraw: (image: Bitmap) -> Bitmap): Response {
        throw UnsupportedOperationException("Parser checker does not download image bodies")
    }

    override fun createBitmap(width: Int, height: Int): Bitmap {
        throw UnsupportedOperationException("Parser checker does not create images")
    }
}

class ParserHealthChecker(
    private val context: MangaLoaderContext,
    private val timeoutMs: Long,
) {

    suspend fun check(sourceKey: String): SourceHealth {
        val checkedAt = Instant.now().toString()
        val started = System.currentTimeMillis()
        var result: SourceHealth? = null

        val elapsed = measureTimeMillis {
            result = try {
                withTimeout(timeoutMs) {
                    checkUnsafe(sourceKey, checkedAt, started)
                }
            } catch (e: Throwable) {
                val stage = throwableToStage(e, System.currentTimeMillis() - started)
                SourceHealth(
                    status = if (stage.status == STATUS_SKIPPED) STATUS_UNKNOWN else stage.status,
                    reason = stage.reason,
                    checkedAt = checkedAt,
                    latencyMs = System.currentTimeMillis() - started,
                    details = stage.details,
                    checks = skippedChecks(stage.reason),
                )
            }
        }

        return result?.copy(latencyMs = result?.latencyMs ?: elapsed)
            ?: SourceHealth(
                status = STATUS_UNKNOWN,
                reason = "Parser error",
                checkedAt = checkedAt,
                latencyMs = elapsed,
                details = "Parser check ended without a result.",
                checks = skippedChecks("Parser check ended without a result."),
            )
    }

    private suspend fun checkUnsafe(sourceKey: String, checkedAt: String, started: Long): SourceHealth {
        val source = runCatching { MangaParserSource.valueOf(sourceKey) }.getOrElse { error ->
            return SourceHealth(
                status = STATUS_UNKNOWN,
                reason = "Parser source not available",
                checkedAt = checkedAt,
                latencyMs = System.currentTimeMillis() - started,
                details = error.message,
                checks = skippedChecks("Parser source not available"),
            )
        }

        val parser = context.newParserInstance(source)
        val order = chooseOrder(parser.availableSortOrders)

        if (order == null) {
            return SourceHealth(
                status = STATUS_UNKNOWN,
                reason = "Parser has no supported sort order",
                checkedAt = checkedAt,
                latencyMs = System.currentTimeMillis() - started,
                details = "availableSortOrders is empty.",
                checks = skippedChecks("Parser has no supported sort order"),
            )
        }

        val checks = linkedMapOf<String, StageResult>()

        val listResult = runStage(CHECK_LIST) {
            parser.getList(
                offset = 0,
                order = order,
                filter = MangaListFilter.EMPTY,
            )
        }

        checks[CHECK_LIST] = if (listResult.value.isNullOrEmpty()) {
            listResult.stage.copy(
                status = if (listResult.stage.status == STATUS_WORKING) STATUS_UNKNOWN else listResult.stage.status,
                reason = if (listResult.stage.status == STATUS_WORKING) "No Parser list results found" else listResult.stage.reason,
                count = listResult.value?.size ?: 0,
            )
        } else {
            listResult.stage.copy(
                status = STATUS_WORKING,
                reason = "List returned ${listResult.value.size} result(s)",
                count = listResult.value.size,
            )
        }

        val firstManga = listResult.value?.firstOrNull()

        if (firstManga == null) {
            checks[CHECK_SEARCH] = skipped("Search skipped because List failed")
            checks[CHECK_DETAILS] = skipped("Manga details skipped because List failed")
            checks[CHECK_CHAPTERS] = skipped("Chapters skipped because Manga details failed")
            checks[CHECK_IMAGES] = skipped("Images skipped because Chapters failed")
            return finalize(sourceKey, checkedAt, started, checks)
        }

        val query = searchQueryFrom(firstManga.title)
        val searchOrder = if (SortOrder.RELEVANCE in parser.availableSortOrders) SortOrder.RELEVANCE else order

        val searchResult = runStage(CHECK_SEARCH) {
            parser.getList(
                offset = 0,
                order = searchOrder,
                filter = MangaListFilter(query = query),
            )
        }

        checks[CHECK_SEARCH] = if (searchResult.value.isNullOrEmpty()) {
            searchResult.stage.copy(
                status = if (searchResult.stage.status == STATUS_WORKING) STATUS_UNKNOWN else searchResult.stage.status,
                reason = if (searchResult.stage.status == STATUS_WORKING) "No Parser search results found" else searchResult.stage.reason,
                count = searchResult.value?.size ?: 0,
                details = "Search query: $query",
            )
        } else {
            searchResult.stage.copy(
                status = STATUS_WORKING,
                reason = "Search returned ${searchResult.value.size} result(s)",
                count = searchResult.value.size,
                details = "Search query: $query",
            )
        }

        val detailsResult = runStage(CHECK_DETAILS) {
            parser.getDetails(firstManga)
        }

        val detailedManga = detailsResult.value

        checks[CHECK_DETAILS] = if (detailedManga == null) {
            detailsResult.stage.copy(
                status = if (detailsResult.stage.status == STATUS_WORKING) STATUS_UNKNOWN else detailsResult.stage.status,
                reason = if (detailsResult.stage.status == STATUS_WORKING) "Manga details not returned" else detailsResult.stage.reason,
            )
        } else {
            detailsResult.stage.copy(
                status = STATUS_WORKING,
                reason = "Manga details loaded",
                details = "Title: ${detailedManga.title.take(80)}",
            )
        }

        if (detailedManga == null) {
            checks[CHECK_CHAPTERS] = skipped("Chapters skipped because Manga details failed")
            checks[CHECK_IMAGES] = skipped("Images skipped because Chapters failed")
            return finalize(sourceKey, checkedAt, started, checks)
        }

        val chapters = detailedManga.chapters.orEmpty()

        checks[CHECK_CHAPTERS] = if (chapters.isEmpty()) {
            StageResult(
                status = STATUS_UNKNOWN,
                reason = "No chapters found",
                count = 0,
                details = "Manga details loaded but chapters list is empty.",
            )
        } else {
            StageResult(
                status = STATUS_WORKING,
                reason = "Chapters returned ${chapters.size} result(s)",
                count = chapters.size,
            )
        }

        val firstChapter = chapters.firstOrNull()

        if (firstChapter == null) {
            checks[CHECK_IMAGES] = skipped("Images skipped because Chapters failed")
            return finalize(sourceKey, checkedAt, started, checks)
        }

        val pagesResult = runStage(CHECK_IMAGES) {
            val pages = parser.getPages(firstChapter)
            val firstPage = pages.firstOrNull()

            if (firstPage == null) {
                ImageProbeResult(pageCount = 0, pageUrl = null)
            } else {
                val pageUrl = parser.getPageUrl(firstPage).takeUnless { it.isBlank() }
                ImageProbeResult(pageCount = pages.size, pageUrl = pageUrl)
            }
        }

        val imageProbe = pagesResult.value

        checks[CHECK_IMAGES] = when {
            imageProbe == null -> pagesResult.stage.copy(
                status = if (pagesResult.stage.status == STATUS_WORKING) STATUS_UNKNOWN else pagesResult.stage.status,
                reason = if (pagesResult.stage.status == STATUS_WORKING) "Images not returned" else pagesResult.stage.reason,
            )

            imageProbe.pageCount <= 0 -> pagesResult.stage.copy(
                status = STATUS_UNKNOWN,
                reason = "No image pages found",
                count = 0,
            )

            imageProbe.pageUrl.isNullOrBlank() -> pagesResult.stage.copy(
                status = STATUS_UNKNOWN,
                reason = "Image URL not returned",
                count = imageProbe.pageCount,
                details = "Parser returned pages but did not return a direct image URL.",
            )

            else -> pagesResult.stage.copy(
                status = STATUS_WORKING,
                reason = "Images returned ${imageProbe.pageCount} page(s)",
                count = imageProbe.pageCount,
                details = "Image URL was resolved. Image body was not downloaded.",
            )
        }

        return finalize(sourceKey, checkedAt, started, checks)
    }

    private data class ImageProbeResult(
        val pageCount: Int,
        val pageUrl: String?,
    )

    private data class StageRun<T>(
        val stage: StageResult,
        val value: T?,
    )

    private suspend fun <T> runStage(name: String, block: suspend () -> T): StageRun<T> {
        var value: T? = null
        var thrown: Throwable? = null
        val elapsed = measureTimeMillis {
            try {
                value = block()
            } catch (e: Throwable) {
                thrown = e
            }
        }

        val error = thrown
        if (error != null) {
            return StageRun(throwableToStage(error, elapsed), null)
        }

        return StageRun(
            StageResult(
                status = STATUS_WORKING,
                reason = "${CHECK_LABELS[name] ?: name} loaded",
                latencyMs = elapsed,
            ),
            value,
        )
    }

    private fun finalize(
        sourceKey: String,
        checkedAt: String,
        started: Long,
        checks: LinkedHashMap<String, StageResult>,
    ): SourceHealth {
        for ((key, label) in CHECK_LABELS) {
            checks.putIfAbsent(key, skipped("$label was not checked"))
        }

        val readingPath = listOf(CHECK_LIST, CHECK_DETAILS, CHECK_CHAPTERS, CHECK_IMAGES)
        val firstReadingFailure = readingPath
            .mapNotNull { checkName -> checks[checkName]?.let { checkName to it } }
            .firstOrNull { (_, stage) -> stage.status != STATUS_WORKING }

        val search = checks[CHECK_SEARCH]

        val overall = when {
            firstReadingFailure == null -> SourceHealth(
                status = STATUS_WORKING,
                reason = "Functional",
                checkedAt = checkedAt,
                latencyMs = System.currentTimeMillis() - started,
                resultCount = checks[CHECK_LIST]?.count,
                details = if (search?.status == STATUS_WORKING) {
                    "Parser runtime passed List, Manga details, Chapters, Images, and Search for $sourceKey."
                } else {
                    "Parser runtime passed List, Manga details, Chapters, and Images for $sourceKey. Search has issues."
                },
                checks = checks,
            )

            else -> {
                val (failedCheckName, failedStage) = firstReadingFailure
                SourceHealth(
                    status = if (failedStage.status == STATUS_SKIPPED) STATUS_UNKNOWN else failedStage.status,
                    reason = "${CHECK_LABELS[failedCheckName] ?: failedCheckName}: ${failedStage.reason}",
                    checkedAt = checkedAt,
                    latencyMs = System.currentTimeMillis() - started,
                    resultCount = checks[CHECK_LIST]?.count,
                    details = "Parser runtime failed at ${CHECK_LABELS[failedCheckName] ?: failedCheckName} for $sourceKey.",
                    checks = checks,
                )
            }
        }

        return overall
    }

    private fun chooseOrder(orders: Set<SortOrder>): SortOrder? {
        if (orders.isEmpty()) return null

        val preferred = listOf(
            SortOrder.UPDATED,
            SortOrder.NEWEST,
            SortOrder.POPULARITY,
            SortOrder.RATING,
            SortOrder.ALPHABETICAL,
        )

        return preferred.firstOrNull { it in orders } ?: orders.first()
    }

    private fun searchQueryFrom(title: String): String {
        val cleaned = title
            .replace(Regex("\\s+"), " ")
            .trim()

        if (cleaned.length <= 32) return cleaned

        return cleaned
            .split(' ')
            .filter { it.length >= 2 }
            .take(4)
            .joinToString(" ")
            .ifBlank { cleaned.take(32) }
    }

    private fun skipped(reason: String): StageResult {
        return StageResult(
            status = STATUS_SKIPPED,
            reason = reason,
        )
    }

    private fun skippedChecks(reason: String): Map<String, StageResult> {
        return CHECK_LABELS.keys.associateWith { skipped(reason) }
    }

    private fun throwableToStage(throwable: Throwable, latencyMs: Long): StageResult {
        val root = rootCause(throwable)
        val className = root::class.simpleName.orEmpty()
        val message = listOfNotNull(root.message, root.cause?.message, throwable.message)
            .joinToString(" ")
            .replace(Regex("\\s+"), " ")
            .trim()
        val lowered = message.lowercase(Locale.ROOT)

        return when {
            root is ParserTooManyRequestsException || className.contains("TooMany", ignoreCase = true) || lowered.contains("429") -> {
                StageResult(
                    status = STATUS_BLOCKED,
                    reason = "Too many requests. Try again later",
                    latencyMs = latencyMs,
                    details = compactThrowable(root),
                )
            }

            root is ParserBlockedException -> {
                StageResult(
                    status = STATUS_BLOCKED,
                    reason = root.message ?: "Additional action required",
                    latencyMs = latencyMs,
                    details = compactThrowable(root),
                )
            }

            lowered.contains("captcha") || lowered.contains("cloudflare") || lowered.contains("browser is not available") ||
                lowered.contains("additional action") || lowered.contains("challenge") -> {
                StageResult(
                    status = STATUS_BLOCKED,
                    reason = "Additional action required",
                    latencyMs = latencyMs,
                    details = compactThrowable(root),
                )
            }

            lowered.contains("403") || lowered.contains("access denied") -> {
                StageResult(
                    status = STATUS_BLOCKED,
                    reason = "Access denied (403)",
                    latencyMs = latencyMs,
                    details = compactThrowable(root),
                )
            }

            root is ContentUnavailableException || lowered.contains("404") || lowered.contains("not found") -> {
                StageResult(
                    status = STATUS_BROKEN,
                    reason = "Content not found or removed",
                    latencyMs = latencyMs,
                    details = compactThrowable(root),
                )
            }

            root is UnknownHostException || root is SocketTimeoutException || root is IOException ||
                lowered.contains("timeout") || lowered.contains("network") || lowered.contains("connection") -> {
                StageResult(
                    status = STATUS_UNKNOWN,
                    reason = "Network error",
                    latencyMs = latencyMs,
                    details = compactThrowable(root),
                )
            }

            className.contains("Parse", ignoreCase = true) || lowered.contains("parse") || lowered.contains("selector") || lowered.contains("cannot find") -> {
                StageResult(
                    status = STATUS_UNKNOWN,
                    reason = "Parser error",
                    latencyMs = latencyMs,
                    details = compactThrowable(root),
                )
            }

            else -> {
                StageResult(
                    status = STATUS_UNKNOWN,
                    reason = "Parser error",
                    latencyMs = latencyMs,
                    details = compactThrowable(root),
                )
            }
        }
    }

    private fun rootCause(throwable: Throwable): Throwable {
        var current = throwable
        val seen = Collections.newSetFromMap(ConcurrentHashMap<Throwable, Boolean>())

        while (current.cause != null && current.cause !in seen) {
            seen.add(current)
            current = current.cause ?: break
        }

        return current
    }

    private fun compactThrowable(throwable: Throwable): String {
        val type = throwable::class.simpleName ?: "Throwable"
        val message = throwable.message.orEmpty().replace(Regex("\\s+"), " ").trim()
        return if (message.isBlank()) type else "$type: $message".take(420)
    }
}

fun main(args: Array<String>) = runBlocking {
    if (args.size < 2) {
        error("Usage: parser-checker <sources.json> <health-output.json>")
    }

    val inputPath = Path.of(args[0]).toAbsolutePath()
    val outputPath = Path.of(args[1]).toAbsolutePath()

    val workers = envInt("PARSER_CHECKER_WORKERS", 4).coerceAtLeast(1)
    val timeoutMs = envLong("PARSER_CHECKER_TIMEOUT_MS", 75_000L).coerceAtLeast(5_000L)

    val root = JSONObject(Files.readString(inputPath))
    val sources = root.getJSONArray("sources")

    val keys = buildList {
        for (i in 0 until sources.length()) {
            val item = sources.getJSONObject(i)
            if (!item.optBoolean("broken", false)) {
                add(item.getString("key"))
            }
        }
    }.distinct()

    progress("Parser runtime checks: 0/${keys.size}")

    val checker = ParserHealthChecker(
        context = ParserRuntimeContext(),
        timeoutMs = timeoutMs,
    )

    val semaphore = Semaphore(workers)
    val healthByKey = ConcurrentHashMap<String, SourceHealth>()

    keys.mapIndexed { index, key ->
        async(Dispatchers.IO) {
            semaphore.withPermit {
                val health = checker.check(key)
                healthByKey[key] = health

                val done = index + 1
                if (done == 1 || done % 25 == 0 || done == keys.size) {
                    progress("Parser runtime checks: $done/${keys.size}")
                }
            }
        }
    }.awaitAll()

    val healthJson = JSONObject()

    for (key in keys.sorted()) {
        healthJson.put(
            key,
            healthByKey[key]?.toJson() ?: SourceHealth(
                status = STATUS_UNKNOWN,
                reason = "Parser runtime unavailable",
                details = "No result was produced for this source.",
            ).toJson(),
        )
    }

    val output = JSONObject().apply {
        put("generatedAt", Instant.now().toString())
        put("health", healthJson)
    }

    Files.createDirectories(outputPath.parent)
    Files.writeString(outputPath, output.toString(2))

    progress("Parser runtime wrote: $outputPath")
}

fun envInt(name: String, default: Int): Int {
    return System.getenv(name)?.toIntOrNull() ?: default
}

fun envLong(name: String, default: Long): Long {
    return System.getenv(name)?.toLongOrNull() ?: default
}

fun progress(message: String) {
    println(message)
    System.out.flush()
}
