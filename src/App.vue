<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import MetricCard from '@/components/MetricCard.vue'
import SourceCard from '@/components/SourceCard.vue'

import { formatDate, formatNumber } from '@/lib/format'
import { sampleData } from '@/sample-data'
import type { SourceDataset, SourceItem, SourceStatus } from '@/types'

const dataset = ref<SourceDataset>(sampleData)
const loading = ref(true)
const error = ref<string | null>(null)

const rawQuery = ref('')
const query = ref('')
const status = ref<'all' | SourceStatus>('all')
const language = ref('all')
const contentType = ref('all')
const nsfw = ref<'all' | 'safe' | 'nsfw'>('all')
const sort = ref<'title' | 'language' | 'status' | 'domains'>('status')
const view = ref<'grid' | 'list'>('grid')
const isScrolled = ref(false)

const LANGUAGE_NAMES: Record<string, string> = {
  en: 'English',
  de: 'German',
  ru: 'Russian',
  fr: 'French',
  es: 'Spanish',
  it: 'Italian',
  pt: 'Portuguese',
  tr: 'Turkish',
  vi: 'Vietnamese',
  id: 'Indonesian',
  th: 'Thai',
  ar: 'Arabic',
  pl: 'Polish',
  uk: 'Ukrainian',
  ja: 'Japanese',
  ko: 'Korean',
  zh: 'Chinese',
  multi: 'Multiple',
}

const statusOrder: Record<SourceStatus, number> = {
  working: 0,
  blocked: 1,
  unknown: 2,
  broken: 3,
}

let searchDebounce: number | undefined

watch(rawQuery, (value) => {
  window.clearTimeout(searchDebounce)
  searchDebounce = window.setTimeout(() => {
    query.value = value.trim().toLowerCase()
  }, 120)
})

function onScroll() {
  isScrolled.value = window.scrollY > 16
}

function withSearchText(source: SourceItem): SourceItem {
  const languageName =
    source.languageName || LANGUAGE_NAMES[source.language] || source.language.toUpperCase()

  return {
    ...source,
    languageName,
    searchText: source.searchText
      ? source.searchText
      : [
          source.title,
          source.key,
          source.language,
          languageName,
          source.engine ?? '',
          source.contentType ?? '',
          source.path,
          source.brokenReason ?? '',
          ...(source.domains ?? []),
        ]
          .join(' ')
          .toLowerCase(),
  }
}

function normalizeDataset(next: SourceDataset): SourceDataset {
  return {
    ...next,
    generatedBy: next.generatedBy ?? 'Static bundle',
    byLocale: next.byLocale ?? {},
    byType: next.byType ?? {},
    duplicatesSkipped: next.duplicatesSkipped ?? [],
    summary: {
      ...next.summary,
      nsfw:
        next.summary.nsfw ??
        next.sources.reduce((count, source) => count + (source.nsfw ? 1 : 0), 0),
    },
    sources: next.sources.map(withSearchText),
  }
}

const languages = computed(() => {
  const values = new Map<string, string>()

  for (const source of dataset.value.sources) {
    if (!source.language) continue

    values.set(
      source.language,
      source.languageName || LANGUAGE_NAMES[source.language] || source.language.toUpperCase(),
    )
  }

  return [
    { value: 'all', label: 'All languages' },
    ...Array.from(values.entries())
      .sort((a, b) => a[1].localeCompare(b[1]))
      .map(([value, label]) => ({ value, label })),
  ]
})

const contentTypes = computed(() => {
  const values = new Set(dataset.value.sources.map((source) => source.contentType ?? 'MANGA'))
  return ['all', ...Array.from(values).sort((a, b) => a.localeCompare(b))]
})

const topLocales = computed(() => {
  const localeSummary = dataset.value.byLocale ?? {}

  return Object.entries(localeSummary)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 8)
})

const topTypes = computed(() => {
  const typeSummary = dataset.value.byType ?? {}

  return Object.entries(typeSummary)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 6)
})

const filteredSources = computed<SourceItem[]>(() => {
  const filtered = dataset.value.sources.filter((source) => {
    const matchesStatus = status.value === 'all' || source.health.status === status.value
    const matchesLanguage = language.value === 'all' || source.language === language.value
    const sourceType = source.contentType ?? 'MANGA'
    const matchesType = contentType.value === 'all' || sourceType === contentType.value

    const matchesNsfw =
      nsfw.value === 'all' ||
      (nsfw.value === 'nsfw' && !!source.nsfw) ||
      (nsfw.value === 'safe' && !source.nsfw)

    const matchesQuery = !query.value || (source.searchText?.includes(query.value) ?? false)

    return matchesStatus && matchesLanguage && matchesType && matchesNsfw && matchesQuery
  })

  return filtered.sort((left, right) => {
    switch (sort.value) {
      case 'title':
        return left.title.localeCompare(right.title)

      case 'language':
        return (
          (left.languageName || left.language).localeCompare(right.languageName || right.language) ||
          left.title.localeCompare(right.title)
        )

      case 'domains':
        return right.domains.length - left.domains.length || left.title.localeCompare(right.title)

      case 'status':
      default:
        return (
          statusOrder[left.health.status] - statusOrder[right.health.status] ||
          left.title.localeCompare(right.title)
        )
    }
  })
})

const qualityScore = computed(() => {
  const total = dataset.value.summary.total || dataset.value.sources.length
  if (!total) return 0
  return Math.round((dataset.value.summary.working / total) * 100)
})

const brokenShare = computed(() => {
  const total = dataset.value.summary.total || dataset.value.sources.length
  if (!total) return 0
  return Math.round((dataset.value.summary.broken / total) * 100)
})

function applyStatus(next: 'all' | SourceStatus) {
  status.value = next
}

function resetFilters() {
  rawQuery.value = ''
  query.value = ''
  status.value = 'all'
  language.value = 'all'
  contentType.value = 'all'
  nsfw.value = 'all'
  sort.value = 'status'
}

onMounted(async () => {
  window.addEventListener('scroll', onScroll, { passive: true })
  onScroll()

  try {
    const response = await fetch(`${import.meta.env.BASE_URL}data/sources.json`, {
      cache: 'no-store',
    })

    if (!response.ok) {
      throw new Error(`Dataset request failed with ${response.status}`)
    }

    const liveData = (await response.json()) as SourceDataset

    if (liveData.sources.length > 0) {
      dataset.value = normalizeDataset(liveData)
    } else {
      dataset.value = normalizeDataset(sampleData)
    }
  } catch (reason) {
    dataset.value = normalizeDataset(sampleData)
    error.value = reason instanceof Error ? reason.message : 'Unknown data loading error'
  } finally {
    loading.value = false
  }
})

onBeforeUnmount(() => {
  window.clearTimeout(searchDebounce)
  window.removeEventListener('scroll', onScroll)
})
</script>

<template>
  <div class="shell">
    <div class="shell__noise"></div>

    <header :class="['topbar', { 'topbar--compact': isScrolled }]" id="top">
      <div class="topbar__brand">
        <div class="topbar__logo">📚</div>

        <div>
          <p class="topbar__eyebrow">Parser source catalog</p>
          <strong>Catalog browser</strong>
        </div>
      </div>

      <nav class="topbar__nav">
        <a href="#catalog">📚 <span class="nav-label">Catalog</span></a>
        <a href="#filters">🔎 <span class="nav-label">Filters</span></a>
        <a href="#distribution">📊 <span class="nav-label">Overview</span></a>
        <a href="#safety">⚠️ <span class="nav-label">Notice</span></a>
      </nav>

      <div class="topbar__actions">
        <a
          class="button button--ghost"
          :href="`https://github.com/Usagi-App/Parser`"
          target="_blank"
          rel="noreferrer noopener"
        >
          🧩 <span class="nav-label">Repo</span>
        </a>
      </div>
    </header>

    <section class="hero card">
      <div class="hero__copy">
        <p class="hero__eyebrow">Parser / Source Catalog</p>

        <h1 class="hero__title">Browse source entries in a clean catalog</h1>

        <p class="hero__text">
          This website serves only as an informational catalog of parser sources,
          extracted metadata, and availability indicators.
        </p>

        <div class="hero__actions">
          <a class="button button--primary" href="#catalog">Browse sources</a>
          <a class="button button--ghost" href="#filters">Open filters</a>
        </div>

        <div class="hero__warning" id="safety">
          <strong>Third-party website warning</strong>

          <p>
            Website buttons open external domains run by other parties. Availability, ads,
            redirects, and content are outside your control.
          </p>
        </div>

        <div class="hero__warning">
          <strong>
            Catalog only. This website lists source metadata for reference and discovery.
            No reader application is provided here, and no source content is hosted,
            cached, or proxied by this website.
          </strong>
        </div>
      </div>

      <aside class="hero__panel">
        <ul class="hero__facts">
          <li>
            <span>Generated</span>
            <strong>{{ formatDate(dataset.generatedAt) }}</strong>
          </li>

          <li>
            <span>Upstream</span>
            <strong>{{ dataset.sourceRepo.owner }}/{{ dataset.sourceRepo.repo }}</strong>
          </li>

          <li>
            <span>Healthy share</span>
            <strong>{{ qualityScore }}%</strong>
          </li>

          <li>
            <span>Broken share</span>
            <strong>{{ brokenShare }}%</strong>
          </li>

          <li>
            <span>Builder</span>
            <strong>{{ dataset.generatedBy ?? 'Static bundle' }}</strong>
          </li>
        </ul>
      </aside>
    </section>

    <section class="metrics-grid" id="distribution">
      <MetricCard
        label="Total sources"
        :value="formatNumber(dataset.summary.total || dataset.sources.length)"
        hint="Unique parser entries extracted from upstream source files"
      />

      <MetricCard
        label="Available"
        :value="formatNumber(dataset.summary.working)"
        hint="Not marked broken upstream"
      />

      <MetricCard
        label="Broken"
        :value="formatNumber(dataset.summary.broken)"
        hint="Explicitly flagged as broken upstream"
      />

      <MetricCard
        label="NSFW"
        :value="formatNumber(dataset.summary.nsfw ?? 0)"
        hint="Entries tagged as adult / explicit"
      />
    </section>

    <section class="info-banner card">
      <div>
        <p>{{ dataset.disclaimer }}</p>

        <p v-if="dataset.duplicatesSkipped?.length" class="info-banner__meta">
          Duplicate parser keys skipped: {{ dataset.duplicatesSkipped.join(', ') }}
        </p>
      </div>

      <p v-if="error" class="info-banner__error">Live dataset failed to load: {{ error }}</p>
    </section>

    <div class="layout">
      <aside class="sidebar card" id="filters">
        <div class="sidebar__section">
          <div class="sidebar__section-head">
            <p class="sidebar__eyebrow">Sticky controls</p>
            <h2>Find the source fast</h2>
          </div>

          <label class="field field--search">
            <span>Search</span>
            <input
              v-model="rawQuery"
              type="search"
              placeholder="Title, language, domain, path, reason…"
              enterkeyhint="search"
              autocomplete="off"
              autocapitalize="off"
              spellcheck="false"
            />
          </label>
        </div>

        <div class="sidebar__section">
          <div class="sidebar__label">Status</div>

          <div class="sidebar__chips">
            <button :class="['chip-button', { 'is-active': status === 'all' }]" @click="applyStatus('all')">All</button>
            <button :class="['chip-button', { 'is-active': status === 'working' }]" @click="applyStatus('working')">Working</button>
            <button :class="['chip-button', { 'is-active': status === 'broken' }]" @click="applyStatus('broken')">Broken</button>
            <button :class="['chip-button', { 'is-active': status === 'blocked' }]" @click="applyStatus('blocked')">Blocked</button>
            <button :class="['chip-button', { 'is-active': status === 'unknown' }]" @click="applyStatus('unknown')">Unknown</button>
          </div>
        </div>

        <div class="sidebar__section sidebar__section--stacked">
          <label class="field">
            <span>Language</span>

            <select v-model="language">
              <option v-for="option in languages" :key="option.value" :value="option.value">
                {{ option.label }}
              </option>
            </select>
          </label>

          <label class="field">
            <span>Content type</span>

            <select v-model="contentType">
              <option v-for="option in contentTypes" :key="option" :value="option">
                {{ option === 'all' ? 'All content types' : option }}
              </option>
            </select>
          </label>

          <label class="field">
            <span>Content safety</span>

            <select v-model="nsfw">
              <option value="all">All entries</option>
              <option value="safe">Safe only</option>
              <option value="nsfw">NSFW only</option>
            </select>
          </label>

          <label class="field">
            <span>Sort</span>

            <select v-model="sort">
              <option value="status">Status</option>
              <option value="title">Title</option>
              <option value="language">Language</option>
              <option value="domains">Domain count</option>
            </select>
          </label>
        </div>

        <div class="sidebar__section">
          <div class="sidebar__label">Quick metrics</div>

          <div class="sidebar__metrics">
            <div>
              <strong>{{ formatNumber(filteredSources.length) }}</strong>
              <span>Shown</span>
            </div>

            <div>
              <strong>{{ formatNumber(dataset.summary.working) }}</strong>
              <span>Working</span>
            </div>

            <div>
              <strong>{{ formatNumber(dataset.summary.broken) }}</strong>
              <span>Broken</span>
            </div>
          </div>
        </div>

        <div class="sidebar__section">
          <div class="sidebar__label">Top locales</div>

          <div class="sidebar__chips">
            <span v-for="[code, count] in topLocales" :key="code" class="sidebar-chip">
              {{ code.toUpperCase() }} · {{ formatNumber(count) }}
            </span>
          </div>
        </div>

        <div class="sidebar__section">
          <div class="sidebar__label">Top content types</div>

          <div class="sidebar__chips">
            <span v-for="[type, count] in topTypes" :key="type" class="sidebar-chip">
              {{ type }} · {{ formatNumber(count) }}
            </span>
          </div>
        </div>

        <div class="sidebar__section sidebar__warning">
          <strong>External link warning</strong>
          <p>Opening a website button leaves this catalog and visits a third-party domain.</p>
        </div>

        <div class="sidebar__section sidebar__actions">
          <button class="button button--ghost button--block" @click="resetFilters">Reset filters</button>
        </div>
      </aside>

      <main class="catalog" id="catalog">
        <section class="catalog-toolbar card">
          <div>
            <p class="catalog-toolbar__eyebrow">Catalog</p>
            <h2>Browse source entries</h2>
          </div>

          <div class="catalog-toolbar__controls">
            <div class="segmented">
              <button :class="['segmented__item', { 'is-active': view === 'grid' }]" @click="view = 'grid'">
                Grid
              </button>

              <button :class="['segmented__item', { 'is-active': view === 'list' }]" @click="view = 'list'">
                List
              </button>
            </div>

            <p class="controls__count">
              Showing {{ formatNumber(filteredSources.length) }} source<span v-if="filteredSources.length !== 1">s</span>
            </p>
          </div>
        </section>

        <section v-if="loading" class="loading card">
          <div class="loading__pulse"></div>
          <p>Loading catalog…</p>
        </section>

        <section v-else-if="filteredSources.length === 0" class="empty-state card">
          <h2>No sources matched your filters.</h2>
          <p>Reset the filters or regenerate the dataset from the upstream repository to repopulate the catalog.</p>
        </section>

        <section v-else :class="['sources', `sources--${view}`]">
          <SourceCard
            v-for="source in filteredSources"
            :key="source.id"
            :source="source"
            :compact="view === 'list'"
          />
        </section>
      </main>
    </div>
  </div>
</template>
