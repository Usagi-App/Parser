<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import MetricCard from '@/components/MetricCard.vue'
import SourceCard from '@/components/SourceCard.vue'
import { formatDate, formatNumber } from '@/lib/format'
import { sampleData } from '@/sample-data'
import type { SourceDataset, SourceItem, SourceStatus } from '@/types'

const dataset = ref<SourceDataset>(sampleData)
const loading = ref(true)
const error = ref<string | null>(null)

const query = ref('')
const status = ref<'all' | SourceStatus>('all')
const language = ref('all')
const contentType = ref('all')
const sort = ref<'title' | 'language' | 'status' | 'domains'>('status')
const view = ref<'grid' | 'list'>('grid')

const statusOrder: Record<SourceStatus, number> = {
  working: 0,
  blocked: 1,
  unknown: 2,
  broken: 3,
}

const languages = computed(() => {
  const values = new Set(dataset.value.sources.map((source) => source.language).filter(Boolean))
  return ['all', ...Array.from(values).sort((a, b) => a.localeCompare(b))]
})

const contentTypes = computed(() => {
  const values = new Set(dataset.value.sources.map((source) => source.contentType ?? 'MANGA'))
  return ['all', ...Array.from(values).sort((a, b) => a.localeCompare(b))]
})

const topLocales = computed(() => {
  const source = dataset.value.byLocale ?? {}
  return Object.entries(source)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 8)
})

const topTypes = computed(() => {
  const source = dataset.value.byType ?? {}
  return Object.entries(source)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 6)
})

const filteredSources = computed<SourceItem[]>(() => {
  const normalizedQuery = query.value.trim().toLowerCase()

  const filtered = dataset.value.sources.filter((source) => {
    const matchesStatus = status.value === 'all' || source.health.status === status.value
    const matchesLanguage = language.value === 'all' || source.language === language.value
    const sourceType = source.contentType ?? 'MANGA'
    const matchesType = contentType.value === 'all' || sourceType === contentType.value
    const haystack = [
      source.title,
      source.key,
      source.language,
      source.engine ?? '',
      source.contentType ?? '',
      source.path,
      source.brokenReason ?? '',
      ...source.domains,
    ]
      .join(' ')
      .toLowerCase()
    const matchesQuery = !normalizedQuery || haystack.includes(normalizedQuery)

    return matchesStatus && matchesLanguage && matchesType && matchesQuery
  })

  return filtered.sort((left, right) => {
    switch (sort.value) {
      case 'title':
        return left.title.localeCompare(right.title)
      case 'language':
        return left.language.localeCompare(right.language) || left.title.localeCompare(right.title)
      case 'domains':
        return right.domains.length - left.domains.length || left.title.localeCompare(right.title)
      case 'status':
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
  query.value = ''
  status.value = 'all'
  language.value = 'all'
  contentType.value = 'all'
  sort.value = 'status'
}

onMounted(async () => {
  try {
    const response = await fetch(`${import.meta.env.BASE_URL}data/sources.json`, { cache: 'no-store' })
    if (!response.ok) {
      throw new Error(`Dataset request failed with ${response.status}`)
    }

    const liveData = (await response.json()) as SourceDataset
    if (liveData.sources.length > 0) {
      dataset.value = liveData
    }
  } catch (reason) {
    error.value = reason instanceof Error ? reason.message : 'Unknown data loading error'
  } finally {
    loading.value = false
  }
})
</script>

<template>
  <div class="shell">
    <div class="shell__noise"></div>

    <header class="topbar card" id="top">
      <div class="topbar__brand">
        <div class="topbar__logo">U</div>
        <div>
          <p class="topbar__eyebrow">Usagi parser catalog</p>
          <strong>Parser source browser</strong>
        </div>
      </div>

      <nav class="topbar__nav">
        <a href="#catalog">Catalog</a>
        <a href="#filters">Filters</a>
        <a href="#distribution">Distribution</a>
        <a href="#safety">Safety</a>
      </nav>

      <div class="topbar__actions">
        <a class="button button--ghost" :href="`https://github.com/${dataset.sourceRepo.owner}/${dataset.sourceRepo.repo}`" target="_blank" rel="noreferrer noopener">
          Source repo
        </a>
        <a class="button button--ghost" href="https://github.com/AgentKush/kotatsu-parsers/tree/master/docs" target="_blank" rel="noreferrer noopener">
          Design reference
        </a>
      </div>
    </header>

    <section class="hero card">
      <div class="hero__copy">
        <p class="hero__eyebrow">Professional catalog, not fake uptime theater</p>
        <h1>Professional parser dashboard with real catalog metadata and cleaner navigation.</h1>
        <p class="hero__text">
          Inspired by the structured feel of the Yumemi site, but rebuilt for your parser use case:
          a fixed dark interface, sticky filtering, clearer actions, and dataset generation from
          <code>YakaTeam/kotatsu-parsers</code> instead of pretending live website checks are trustworthy.
        </p>

        <div class="hero__actions">
          <a class="button button--primary" href="#catalog">Browse sources</a>
          <a class="button button--ghost" href="#filters">Open filter sidebar</a>
          <a class="button button--ghost" href="https://github.com/YumemiProject/yumemiproject.github.io" target="_blank" rel="noreferrer noopener">
            UI inspiration
          </a>
        </div>

        <div class="hero__warning" id="safety">
          <strong>Third-party website warning</strong>
          <p>
            Website buttons open external domains run by other parties. Availability, ads, redirects,
            and content are outside your control.
          </p>
        </div>
      </div>

      <aside class="hero__panel">
        <div class="hero__score-ring">
          <span>{{ qualityScore }}%</span>
          <small>Not marked broken</small>
        </div>

        <ul class="hero__facts">
          <li>
            <span>Generated</span>
            <strong>{{ formatDate(dataset.generatedAt) }}</strong>
          </li>
          <li>
            <span>Repo</span>
            <strong>{{ dataset.sourceRepo.owner }}/{{ dataset.sourceRepo.repo }}</strong>
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
        hint="Unique parser entries extracted from Kotlin source files"
      />
      <MetricCard
        label="Available"
        :value="formatNumber(dataset.summary.working)"
        hint="Not marked @Broken upstream"
      />
      <MetricCard
        label="Broken"
        :value="formatNumber(dataset.summary.broken)"
        hint="Explicitly flagged as broken upstream"
      />
      <MetricCard
        label="Locales"
        :value="formatNumber(Object.keys(dataset.byLocale ?? {}).length)"
        hint="Language buckets present in the catalog"
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
            <input v-model="query" type="search" placeholder="Title, key, domain, path, reason…" />
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
              <option v-for="option in languages" :key="option" :value="option">
                {{ option === 'all' ? 'All languages' : option.toUpperCase() }}
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
          <a class="button button--ghost button--block" :href="`https://github.com/${dataset.sourceRepo.owner}/${dataset.sourceRepo.repo}`" target="_blank" rel="noreferrer noopener">
            Open upstream repo
          </a>
        </div>
      </aside>

      <main class="catalog" id="catalog">
        <section class="catalog-toolbar card">
          <div>
            <p class="catalog-toolbar__eyebrow">Source catalog</p>
            <h2>Original top navigation plus a proper sticky filter rail</h2>
            <p class="catalog-toolbar__text">
              The top bar stays for navigation and identity. The left sidebar handles the real work:
              searching, filtering, sorting, and keeping the warning visible while people scroll.
            </p>
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
          <p>Reset the filters or regenerate the dataset from YakaTeam to repopulate the catalog.</p>
        </section>

        <section v-else :class="['sources', `sources--${view}`]">
          <SourceCard v-for="source in filteredSources" :key="source.id" :source="source" />
        </section>
      </main>
    </div>
  </div>
</template>
