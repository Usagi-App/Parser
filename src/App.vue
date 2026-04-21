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

const status = ref<'all' | 'working' | 'blocked'>('all')
const language = ref('all')
const contentType = ref('all')
const nsfw = ref<'all' | 'safe' | 'nsfw'>('all')
const sort = ref<'title' | 'language' | 'status' | 'domains'>('status')
const view = ref<'grid' | 'list'>('grid')

const page = ref(1)
const perPage = ref(50)
const perPageOptions = [25, 50, 100, 200]

const drawerOpen = ref(false)
const parallaxY = ref(0)
const activeNav = ref('home')
const layoutNavPreference = ref<'filters' | 'catalog'>('catalog')

const navItems = [
  { id: 'home', label: 'Home' },
  { id: 'catalog', label: 'Catalog' },
  { id: 'filters', label: 'Filters' },
  { id: 'distribution', label: 'Overview' },
  { id: 'notices', label: 'Notice' },
]

const heroNotices = [
  {
    id: 'catalog-only',
    title: 'Catalog only',
    body:
      'This website lists source metadata for reference and discovery. No reader application is provided here, and no source content is hosted, cached, or proxied by this website.',
  },
]

const skeletonMetricCount = [1, 2, 3, 4]
const skeletonCardCount = [1, 2, 3, 4, 5, 6]

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
  be: 'Belarusian',
  cs: 'Czech',
  multi: 'Multiple',
}

const statusOrder: Record<SourceStatus, number> = {
  working: 0,
  blocked: 1,
  unknown: 2,
  broken: 3,
}

let searchDebounce: number | undefined
let scrollFrame: number | undefined

watch(rawQuery, (value) => {
  window.clearTimeout(searchDebounce)
  searchDebounce = window.setTimeout(() => {
    query.value = value.trim().toLowerCase()
  }, 120)
})

watch([query, status, language, contentType, nsfw, sort, perPage], () => {
  page.value = 1
})

watch(drawerOpen, (open) => {
  document.body.style.overflow = open ? 'hidden' : ''
})

function formatContentType(value?: string) {
  const normalized = (value ?? 'MANGA').trim().toUpperCase()

  switch (normalized) {
    case 'MANGA':
      return 'Manga'
    case 'MANHWA':
      return 'Manhwa'
    case 'MANHUA':
      return 'Manhua'
    case 'COMICS':
      return 'Comics'
    case 'NOVEL':
      return 'Novel'
    default:
      return normalized.charAt(0) + normalized.slice(1).toLowerCase()
  }
}

function getLanguageLabel(source: SourceItem) {
  return source.languageName || LANGUAGE_NAMES[source.language] || source.language.toUpperCase()
}

function buildSearchText(source: SourceItem) {
  return [
    source.title,
    source.key,
    source.language,
    getLanguageLabel(source),
    source.engine ?? '',
    source.contentType ?? '',
    source.path,
    source.brokenReason ?? '',
    ...(source.domains ?? []),
  ]
    .join(' ')
    .toLowerCase()
}

function withSearchText(source: SourceItem): SourceItem {
  return {
    ...source,
    languageName: getLanguageLabel(source),
    searchText: source.searchText || buildSearchText(source),
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

function setView(next: 'grid' | 'list') {
  view.value = next
}

function openDrawer() {
  drawerOpen.value = true
}

function closeDrawer() {
  drawerOpen.value = false
}

function handleKeydown(event: KeyboardEvent) {
  if (event.key === 'Escape') {
    closeDrawer()
  }
}

function updateActiveNav() {
  const topbar = document.querySelector('.topbar') as HTMLElement | null
  const offset = (topbar?.offsetHeight ?? 0) + 28
  const currentY = window.scrollY + offset

  const layout = document.querySelector('.layout') as HTMLElement | null
  if (layout) {
    const layoutTop = layout.offsetTop
    const layoutBottom = layoutTop + layout.offsetHeight

    if (currentY >= layoutTop && currentY < layoutBottom) {
      activeNav.value = layoutNavPreference.value
      return
    }
  }

  const orderedSections = ['home', 'notices', 'distribution']
  let current = 'home'

  for (const id of orderedSections) {
    if (id === 'home') continue
    const element = document.getElementById(id)
    if (element && currentY >= element.offsetTop) {
      current = id
    }
  }

  activeNav.value = current
}

function handleScroll() {
  if (scrollFrame) return

  scrollFrame = window.requestAnimationFrame(() => {
    parallaxY.value = Math.min(window.scrollY, 180)
    updateActiveNav()
    scrollFrame = undefined
  })
}

function scrollToSection(id: string) {
  closeDrawer()

  if (id === 'home') {
    window.scrollTo({ top: 0, behavior: 'smooth' })
    return
  }

  if (id === 'filters' || id === 'catalog') {
    layoutNavPreference.value = id
  }

  const element = document.getElementById(id)
  const topbar = document.querySelector('.topbar') as HTMLElement | null
  const offset = (topbar?.offsetHeight ?? 0) + 24

  if (!element) return

  const y = element.getBoundingClientRect().top + window.scrollY - offset
  window.scrollTo({
    top: Math.max(y, 0),
    behavior: 'smooth',
  })

  activeNav.value = id
}

const parallaxStyle = computed(() => ({
  transform: `translate3d(0, ${Math.round(parallaxY.value * 0.08)}px, 0)`,
}))

const languageOptions = computed(() => {
  const counts = dataset.value.byLocale ?? {}
  const seen = new Set(dataset.value.sources.map((source) => source.language))

  const items = Array.from(seen).map((code) => {
    const count =
      counts[code] ??
      dataset.value.sources.filter((source) => source.language === code).length

    return {
      value: code,
      count,
      label: `${LANGUAGE_NAMES[code] || code.toUpperCase()} (${formatNumber(count)})`,
    }
  })

  items.sort((a, b) => b.count - a.count || a.label.localeCompare(b.label))

  return [{ value: 'all', label: 'All languages' }, ...items]
})

const contentTypeOptions = computed(() => {
  const counts = dataset.value.byType ?? {}
  const seen = new Set(dataset.value.sources.map((source) => source.contentType ?? 'MANGA'))

  const items = Array.from(seen).map((type) => {
    const count =
      counts[type] ??
      dataset.value.sources.filter((source) => (source.contentType ?? 'MANGA') === type).length

    return {
      value: type,
      count,
      label: `${formatContentType(type)} (${formatNumber(count)})`,
    }
  })

  items.sort((a, b) => b.count - a.count || a.label.localeCompare(b.label))

  return [{ value: 'all', label: 'All content types' }, ...items]
})

const filteredSources = computed<SourceItem[]>(() => {
  const filtered = dataset.value.sources.filter((source) => {
    const sourceStatus = source.health.status
    const sourceType = source.contentType ?? 'MANGA'

    const matchesStatus = status.value === 'all' || sourceStatus === status.value
    const matchesLanguage = language.value === 'all' || source.language === language.value
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
        return getLanguageLabel(left).localeCompare(getLanguageLabel(right)) || left.title.localeCompare(right.title)
      case 'domains':
        return (right.domains?.length ?? 0) - (left.domains?.length ?? 0) || left.title.localeCompare(right.title)
      case 'status':
      default:
        return statusOrder[left.health.status] - statusOrder[right.health.status] || left.title.localeCompare(right.title)
    }
  })
})

const totalPages = computed(() => Math.max(1, Math.ceil(filteredSources.value.length / perPage.value)))

const paginatedSources = computed<SourceItem[]>(() => {
  const start = (page.value - 1) * perPage.value
  return filteredSources.value.slice(start, start + perPage.value)
})

const canGoPrev = computed(() => page.value > 1)
const canGoNext = computed(() => page.value < totalPages.value)

watch(totalPages, (nextTotal) => {
  if (page.value > nextTotal) {
    page.value = nextTotal
  }
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

function applyStatus(next: 'all' | 'working' | 'blocked') {
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
  view.value = 'grid'
  page.value = 1
  perPage.value = 50
}

function goToPage(next: number) {
  page.value = Math.min(Math.max(1, next), totalPages.value)
}

onMounted(async () => {
  window.addEventListener('scroll', handleScroll, { passive: true })
  window.addEventListener('keydown', handleKeydown)
  handleScroll()

  try {
    const response = await fetch(`${import.meta.env.BASE_URL}data/sources.json`, {
      cache: 'force-cache',
    })

    if (!response.ok) {
      throw new Error(`Dataset request failed with ${response.status}`)
    }

    const liveData = (await response.json()) as SourceDataset
    dataset.value = normalizeDataset(liveData.sources.length > 0 ? liveData : sampleData)
  } catch (reason) {
    dataset.value = normalizeDataset(sampleData)
    error.value = reason instanceof Error ? reason.message : 'Unknown data loading error'
  } finally {
    loading.value = false
  }
})

onBeforeUnmount(() => {
  window.clearTimeout(searchDebounce)
  window.removeEventListener('scroll', handleScroll)
  window.removeEventListener('keydown', handleKeydown)

  if (scrollFrame) {
    window.cancelAnimationFrame(scrollFrame)
  }

  document.body.style.overflow = ''
})
</script>

<template>
  <div class="shell">
    <div id="home" class="page-anchor"></div>

    <header class="topbar">
      <div class="topbar__brand">
        <span class="topbar__brand-mark" aria-hidden="true">
          <svg viewBox="0 0 24 24" fill="none">
            <path d="M4 6.5C4 5.4 4.9 4.5 6 4.5H18C19.1 4.5 20 5.4 20 6.5V17.5C20 18.6 19.1 19.5 18 19.5H6C4.9 19.5 4 18.6 4 17.5V6.5Z" />
            <path d="M8 4.5V19.5" />
            <path d="M11 8.5H16" />
            <path d="M11 12H16" />
          </svg>
        </span>

        <div>
          <p class="topbar__eyebrow">Parser source catalog</p>
          <strong>Directory only</strong>
        </div>
      </div>

      <nav class="topbar__nav" aria-label="Primary">
        <button
          v-for="item in navItems"
          :key="item.id"
          type="button"
          :class="['topbar__nav-button', { 'is-active': activeNav === item.id }]"
          @click="scrollToSection(item.id)"
        >
          {{ item.label }}
        </button>
      </nav>

      <div class="topbar__actions">
        <a
          class="button button--ghost"
          href="https://github.com/Usagi-App/Parser"
          target="_blank"
          rel="noreferrer noopener"
        >
          Repo
        </a>

        <button
          class="icon-button topbar__menu"
          type="button"
          aria-label="Open navigation drawer"
          @click="openDrawer"
        >
          <svg viewBox="0 0 24 24" fill="none">
            <path d="M4 7H20" />
            <path d="M4 12H20" />
            <path d="M4 17H14" />
          </svg>
        </button>
      </div>
    </header>

    <transition name="fade">
      <div v-if="drawerOpen" class="drawer">
        <button class="drawer__overlay" type="button" aria-label="Close drawer" @click="closeDrawer"></button>

        <aside class="drawer__panel">
          <div class="drawer__head">
            <strong>Navigate</strong>

            <button class="icon-button" type="button" aria-label="Close drawer" @click="closeDrawer">
              <svg viewBox="0 0 24 24" fill="none">
                <path d="M6 6L18 18" />
                <path d="M18 6L6 18" />
              </svg>
            </button>
          </div>

          <nav class="drawer__nav">
            <button
              v-for="item in navItems"
              :key="item.id"
              type="button"
              :class="['drawer__nav-button', { 'is-active': activeNav === item.id }]"
              @click="scrollToSection(item.id)"
            >
              {{ item.label }}
            </button>
          </nav>

          <a
            class="button button--ghost button--block"
            href="https://github.com/Usagi-App/Parser"
            target="_blank"
            rel="noreferrer noopener"
          >
            Open repository
          </a>
        </aside>
      </div>
    </transition>

    <section class="hero card">
      <div class="hero__copy">
        <p class="hero__eyebrow">Vue / Vite catalog</p>
        <h1 class="hero__title">Clean parser directory with faster search and lighter rendering</h1>

        <p class="hero__text">
          Browse parser entries, domains, languages, and health state without reader logic,
          proxying, or hosted source content.
        </p>

        <div class="hero__actions">
          <button class="button button--primary" type="button" @click="scrollToSection('catalog')">
            Browse catalog
          </button>

          <button class="button button--ghost" type="button" @click="scrollToSection('filters')">
            Open filters
          </button>
        </div>

        <div id="notices" class="hero__notices">
          <div
            v-for="notice in heroNotices"
            :key="notice.id"
            class="hero__warning hero__warning--danger"
          >
            <strong>{{ notice.title }}</strong>
            <p>{{ notice.body }}</p>
          </div>
        </div>
      </div>

      <aside class="hero__art">
        <div class="hero__decor" :style="parallaxStyle" aria-hidden="true"></div>

        <div class="hero__glass-grid">
          <div class="hero__glass">
            <span class="hero__glass-label">Generated</span>
            <strong>{{ formatDate(dataset.generatedAt) }}</strong>
          </div>

          <div class="hero__glass">
            <span class="hero__glass-label">Healthy share</span>
            <strong>{{ qualityScore }}%</strong>
          </div>

          <div class="hero__glass">
            <span class="hero__glass-label">Broken share</span>
            <strong>{{ brokenShare }}%</strong>
          </div>
        </div>

        <ul class="hero__facts">
          <li>
            <span>Upstream</span>
            <strong>{{ dataset.sourceRepo.owner }}/{{ dataset.sourceRepo.repo }}</strong>
          </li>

          <li>
            <span>Builder</span>
            <a
              v-if="dataset.generatedBy === 'scripts/build_catalog.py'"
              class="meta-link"
              href="https://github.com/Usagi-App/Parser/blob/main/scripts/build_catalog.py"
              target="_blank"
              rel="noreferrer noopener"
            >
              scripts/build_catalog
            </a>
            <strong v-else>{{ dataset.generatedBy ?? 'Static bundle' }}</strong>
          </li>

          <li>
            <span>Entries</span>
            <strong>{{ formatNumber(dataset.summary.total || dataset.sources.length) }}</strong>
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
            <p class="sidebar__eyebrow">Filters</p>
            <h2>Find a source fast</h2>
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
            <button :class="['chip-button', { 'is-active': status === 'blocked' }]" @click="applyStatus('blocked')">Blocked</button>
          </div>
        </div>

        <div class="sidebar__section sidebar__section--stacked">
          <label class="field">
            <span>Language</span>
            <select v-model="language">
              <option v-for="option in languageOptions" :key="option.value" :value="option.value">
                {{ option.label }}
              </option>
            </select>
          </label>

          <label class="field">
            <span>Content type</span>
            <select v-model="contentType">
              <option v-for="option in contentTypeOptions" :key="option.value" :value="option.value">
                {{ option.label }}
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
              <option value="title">Title A–Z</option>
              <option value="language">Language</option>
              <option value="domains">Domain count</option>
            </select>
          </label>
        </div>

        <div class="sidebar__section sidebar__warning">
          <strong>About this catalog</strong>
          <p>
            This website is an informational catalog of parser source metadata.
            “Working” means the parser is not marked <code>@Broken</code> in the upstream source code.
            It is not a live site health check.
          </p>
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
              <button
                :class="['segmented__item', { 'is-active': view === 'grid' }]"
                @click="setView('grid')"
              >
                Grid
              </button>

              <button
                :class="['segmented__item', { 'is-active': view === 'list' }]"
                @click="setView('list')"
              >
                List
              </button>
            </div>

            <label class="field field--inline catalog-toolbar__per-page">
              <span>Per page</span>
              <select v-model.number="perPage">
                <option v-for="option in perPageOptions" :key="option" :value="option">
                  {{ option }}
                </option>
              </select>
            </label>

            <p class="controls__count">
              Showing {{ formatNumber(paginatedSources.length) }} of
              {{ formatNumber(filteredSources.length) }} source<span v-if="filteredSources.length !== 1">s</span>
            </p>
          </div>

          <div class="pagination">
            <button
              class="button button--ghost button--small"
              :disabled="!canGoPrev"
              @click="goToPage(page - 1)"
            >
              Prev
            </button>

            <span class="pagination__status">
              Page {{ formatNumber(page) }} / {{ formatNumber(totalPages) }}
            </span>

            <button
              class="button button--ghost button--small"
              :disabled="!canGoNext"
              @click="goToPage(page + 1)"
            >
              Next
            </button>
          </div>
        </section>

        <section v-if="loading" class="loading-shell">
          <div class="metrics-grid">
            <article v-for="item in skeletonMetricCount" :key="item" class="metric-card card skeleton-block"></article>
          </div>

          <div class="catalog-toolbar card skeleton-toolbar"></div>

          <div class="sources sources--grid">
            <article v-for="item in skeletonCardCount" :key="item" class="source-card skeleton-card">
              <div class="skeleton-line skeleton-line--lg"></div>
              <div class="skeleton-line skeleton-line--md"></div>
              <div class="skeleton-chips">
                <span class="skeleton-pill"></span>
                <span class="skeleton-pill"></span>
                <span class="skeleton-pill"></span>
              </div>
              <div class="skeleton-line"></div>
              <div class="skeleton-line skeleton-line--sm"></div>
              <div class="skeleton-actions">
                <span class="skeleton-pill skeleton-pill--btn"></span>
                <span class="skeleton-pill skeleton-pill--btn"></span>
              </div>
            </article>
          </div>
        </section>

        <section v-else-if="paginatedSources.length === 0" class="empty-state card">
          <h2>No sources matched your filters.</h2>
          <p>Reset the filters or broaden the search to repopulate the catalog.</p>
        </section>

        <section
          v-else
          :class="[
            'sources',
            `sources--${view}`
          ]"
        >
          <SourceCard
            v-for="source in paginatedSources"
            :key="source.id"
            :source="source"
            :compact="view === 'list'"
          />
        </section>
      </main>
    </div>
  </div>
</template>
