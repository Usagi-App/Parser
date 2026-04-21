<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import SourceCard from '@/components/SourceCard.vue'

import { formatDate, formatNumber } from '@/lib/format'
import { sampleData } from '@/sample-data'
import type { SourceDataset, SourceItem, SourceStatus } from '@/types'

const dataset = ref<SourceDataset>(sampleData)
const loading = ref(true)
const error = ref<string | null>(null)

const rawQuery = ref('')
const query = ref('')

const status = ref<'all' | 'working' | 'broken'>('all')
const language = ref('all')
const contentType = ref('all')
const nsfw = ref<'all' | 'safe' | 'nsfw'>('all')
const sort = ref<'title' | 'language' | 'status' | 'domains'>('status')
const view = ref<'grid' | 'list'>('grid')

const page = ref(1)
const pageInput = ref('1')
const perPage = ref(50)
const perPageOptions = [25, 50, 100, 200]

const drawerOpen = ref(false)
const parallaxY = ref(0)
const activeNav = ref('home')
const isScrolled = ref(false)
const showBackToTop = ref(false)
const layoutNavPreference = ref<'filters' | 'catalog'>('catalog')

const VIEW_PREFS_KEY = 'usagi.viewPrefs'

const navItems = [
  { id: 'home', label: 'Home' },
  { id: 'catalog', label: 'Catalog' },
  { id: 'filters', label: 'Filters' },
  { id: 'distribution', label: 'Overview' },
  { id: 'notices', label: 'Notice' },
]

const sidebarNotices = [
  {
    id: 'catalog-only',
    variant: 'danger' as const,
    title: 'Catalog only',
    body:
      'This website lists source metadata for reference and discovery. No reader application is provided here, and no source content is hosted, cached, or proxied by this website.',
  },
  {
    id: 'third-party-websites',
    variant: 'warning' as const,
    title: 'Third-party websites',
    body:
      'Website buttons open external domains run by other parties. Availability, redirects, ads, and content are outside your control.',
  },
]

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

watch(page, (value) => {
  pageInput.value = String(value)
})

watch([view, perPage], persistViewPrefs)

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

  const distribution = document.getElementById('distribution')
  const notices = document.getElementById('notices')
  const layout = document.querySelector('.layout') as HTMLElement | null

  if (distribution && currentY >= distribution.offsetTop) {
    activeNav.value = 'distribution'
  }

  if (notices && currentY >= notices.offsetTop) {
    activeNav.value = 'notices'
  }

  if (layout) {
    const layoutTop = layout.offsetTop
    const layoutBottom = layoutTop + layout.offsetHeight

    if (currentY >= layoutTop && currentY < layoutBottom) {
      activeNav.value = layoutNavPreference.value
      return
    }
  }

  if (notices && currentY >= notices.offsetTop) {
    return
  }

  if (distribution && currentY >= distribution.offsetTop) {
    return
  }

  activeNav.value = 'home'
}

function handleScroll() {
  if (scrollFrame) return

  scrollFrame = window.requestAnimationFrame(() => {
    const y = window.scrollY
    parallaxY.value = Math.min(y, 180)
    isScrolled.value = y > 10
    showBackToTop.value = y > 420
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
  window.scrollTo({ top: Math.max(y, 0), behavior: 'smooth' })
  activeNav.value = id
}

function goToTop() {
  window.scrollTo({ top: 0, behavior: 'smooth' })
}

function persistViewPrefs() {
  try {
    localStorage.setItem(
      VIEW_PREFS_KEY,
      JSON.stringify({ view: view.value, perPage: perPage.value }),
    )
  } catch {
    // ignore
  }
}

function hydrateViewPrefs() {
  try {
    const raw = localStorage.getItem(VIEW_PREFS_KEY)
    if (!raw) return

    const parsed = JSON.parse(raw) as { view?: 'grid' | 'list'; perPage?: number }
    if (parsed.view === 'grid' || parsed.view === 'list') {
      view.value = parsed.view
    }
    if (typeof parsed.perPage === 'number' && perPageOptions.includes(parsed.perPage)) {
      perPage.value = parsed.perPage
    }
  } catch {
    // ignore
  }
}

function updateUrlParams() {
  const params = new URLSearchParams(window.location.search)

  const setOrDelete = (key: string, value: string, fallback: string) => {
    if (value === fallback) params.delete(key)
    else params.set(key, value)
  }

  setOrDelete('q', rawQuery.value.trim(), '')
  setOrDelete('status', status.value, 'all')
  setOrDelete('lang', language.value, 'all')
  setOrDelete('type', contentType.value, 'all')
  setOrDelete('nsfw', nsfw.value, 'all')
  setOrDelete('sort', sort.value, 'status')
  setOrDelete('view', view.value, 'grid')
  setOrDelete('page', String(page.value), '1')
  setOrDelete('perPage', String(perPage.value), '50')

  const next = `${window.location.pathname}${params.toString() ? `?${params}` : ''}${window.location.hash}`
  window.history.replaceState({}, '', next)
}

function hydrateFromUrl() {
  const params = new URLSearchParams(window.location.search)

  const q = params.get('q')
  const nextStatus = params.get('status')
  const nextLang = params.get('lang')
  const nextType = params.get('type')
  const nextNsfw = params.get('nsfw')
  const nextSort = params.get('sort')
  const nextView = params.get('view')
  const nextPage = params.get('page')
  const nextPerPage = params.get('perPage')

  if (q) {
    rawQuery.value = q
    query.value = q.trim().toLowerCase()
  }

  if (nextStatus === 'all' || nextStatus === 'working' || nextStatus === 'blocked') {
    status.value = nextStatus
  }

  if (nextLang) language.value = nextLang
  if (nextType) contentType.value = nextType
  if (nextNsfw === 'all' || nextNsfw === 'safe' || nextNsfw === 'nsfw') nsfw.value = nextNsfw
  if (nextSort === 'title' || nextSort === 'language' || nextSort === 'status' || nextSort === 'domains') {
    sort.value = nextSort
  }
  if (nextView === 'grid' || nextView === 'list') {
    view.value = nextView
  }

  const parsedPerPage = Number(nextPerPage)
  if (Number.isFinite(parsedPerPage) && perPageOptions.includes(parsedPerPage)) {
    perPage.value = parsedPerPage
  }

  const parsedPage = Number(nextPage)
  if (Number.isFinite(parsedPage) && parsedPage > 0) {
    page.value = parsedPage
    pageInput.value = String(parsedPage)
  }
}

function commitPageInput() {
  const next = Number(pageInput.value)
  if (!Number.isFinite(next) || next < 1) {
    pageInput.value = String(page.value)
    return
  }
  goToPage(next)
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
  pageInput.value = String(page.value)
})

watch([rawQuery, status, language, contentType, nsfw, sort, view, page, perPage], updateUrlParams)

const overviewBars = computed(() => {
  const total = dataset.value.summary.total || dataset.value.sources.length || 1

  return [
    { label: 'Available', value: dataset.value.summary.working, percent: Math.round((dataset.value.summary.working / total) * 100), tone: 'working' },
    { label: 'Broken', value: dataset.value.summary.broken, percent: Math.round((dataset.value.summary.broken / total) * 100), tone: 'broken' },
    { label: 'NSFW', value: dataset.value.summary.nsfw ?? 0, percent: Math.round(((dataset.value.summary.nsfw ?? 0) / total) * 100), tone: 'nsfw' },
  ]
})

function applyStatus(next: 'all' | 'working' | 'broken') {
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
  hydrateViewPrefs()
  hydrateFromUrl()

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
    updateActiveNav()
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
    <a class="skip-link" href="#catalog">Skip to catalog</a>
    <div id="home" class="page-anchor"></div>

    <header :class="['topbar', { 'topbar--scrolled': isScrolled }]">
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

    <section class="overview-card card" id="distribution">
      <div class="overview-card__copy">
        <p class="hero__eyebrow">Vue / Vite catalog</p>
        <h1 class="overview-card__title">Clean parser directory with faster search and lighter rendering</h1>

        <p class="overview-card__text">
          Browse parser entries, domains, languages, and health state without reader logic,
          proxying, or hosted source content.
        </p>

        <p class="overview-card__meta">
          Last updated: {{ formatDate(dataset.generatedAt) }}<span v-if="updatedRelative"> · {{ updatedRelative }}</span>
        </p>

        <div class="overview-card__actions">
          <button class="button button--primary" type="button" @click="scrollToSection('catalog')">
            Browse catalog
          </button>

          <button class="button button--ghost" type="button" @click="scrollToSection('filters')">
            Open filters
          </button>
        </div>
      </div>

      <div class="overview-card__stats overview-card__stats--bars">
        <div class="overview-card__decor" :style="parallaxStyle" aria-hidden="true"></div>

        <div class="overview-card__bars overview-card__bars--panel" aria-label="Catalog summary bars">
          <article
            v-for="(bar, index) in overviewBars"
            :key="bar.label"
            class="overview-bar"
          >
            <div class="overview-bar__head">
              <span>{{ bar.label }}</span>
              <strong>{{ bar.percent }}%</strong>
            </div>

            <div class="overview-bar__track">
              <span
                :class="['overview-bar__fill', `is-${bar.tone}`]"
                :style="{
                  width: `${bar.percent}%`,
                  '--bar-delay': `${index * 120}ms`,
                }"
              ></span>
            </div>

            <small class="overview-bar__meta">{{ formatNumber(bar.value) }}</small>
          </article>
        </div>
      </div>
    </section>

    <section class="info-banner card" id="notices">
      <div class="info-banner__notices">
        <article
          v-for="notice in sidebarNotices"
          :key="notice.id"
          :class="['notice-card', `notice-card--${notice.variant}`]"
        >
          <strong>{{ notice.title }}</strong>
          <p>{{ notice.body }}</p>
        </article>
      </div>
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
              inputmode="search"
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
              <option value="nsfw">NSFW only ({{ formatNumber(dataset.summary.nsfw ?? 0) }})</option>
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
                aria-label="Grid view"
                @click="setView('grid')"
              >
                Grid
              </button>

              <button
                :class="['segmented__item', { 'is-active': view === 'list' }]"
                aria-label="List view"
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

            <p class="controls__count" aria-live="polite">
              Showing {{ formatNumber(paginatedSources.length) }} of
              {{ formatNumber(filteredSources.length) }} source<span v-if="filteredSources.length !== 1">s</span>
            </p>
          </div>

          <div class="pagination pagination--android">
            <button
              class="button button--ghost button--small"
              :disabled="!canGoPrev"
              aria-label="Previous page"
              @click="goToPage(page - 1)"
            >
              Prev
            </button>

            <div class="pagination__center">
              <span class="pagination__label">Page</span>
              <input
                v-model="pageInput"
                class="pagination__input"
                type="number"
                min="1"
                :max="totalPages"
                inputmode="numeric"
                @change="commitPageInput"
                @keyup.enter="commitPageInput"
              />
              <span class="pagination__status">/ {{ formatNumber(totalPages) }}</span>
            </div>

            <button
              class="button button--ghost button--small"
              :disabled="!canGoNext"
              aria-label="Next page"
              @click="goToPage(page + 1)"
            >
              Next
            </button>
          </div>
        </section>

        <section v-if="loading" class="loading-shell">
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

    <button
      v-show="showBackToTop"
      class="back-to-top"
      type="button"
      aria-label="Back to top"
      @click="goToTop"
    >
      ↑
    </button>
  </div>
</template>
