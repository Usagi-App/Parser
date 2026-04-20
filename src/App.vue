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
const sort = ref<'title' | 'language' | 'status' | 'domains'>('status')
const view = ref<'grid' | 'list'>('grid')

const statusOrder: Record<SourceStatus, number> = {
  working: 0,
  blocked: 1,
  unknown: 2,
  broken: 3,
}

const languages = computed(() => {
  const values = new Set(dataset.value.sources.map((source) => source.language))
  return ['all', ...Array.from(values).sort((a, b) => a.localeCompare(b))]
})

const filteredSources = computed<SourceItem[]>(() => {
  const normalizedQuery = query.value.trim().toLowerCase()

  const filtered = dataset.value.sources.filter((source) => {
    const matchesStatus = status.value === 'all' || source.health.status === status.value
    const matchesLanguage = language.value === 'all' || source.language === language.value
    const haystack = [
      source.title,
      source.key,
      source.language,
      source.engine ?? '',
      source.path,
      ...source.domains,
    ]
      .join(' ')
      .toLowerCase()
    const matchesQuery = !normalizedQuery || haystack.includes(normalizedQuery)

    return matchesStatus && matchesLanguage && matchesQuery
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

const healthScore = computed(() => {
  const total = dataset.value.summary.total || dataset.value.sources.length
  if (!total) return 0

  return Math.round((dataset.value.summary.working / total) * 100)
})

onMounted(async () => {
  try {
    const response = await fetch('/data/sources.json', { cache: 'no-store' })
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

    <header class="hero card">
      <div class="hero__copy">
        <p class="hero__eyebrow">Source dashboard</p>
        <h1>Tracking Site for parsers sources.</h1>
        <p class="hero__text">
          Directory only. No manga content is hosted, cached, or proxied here. Visiting a source's site from a card takes you to the third-party site directly. 
        </p>
        <div class="hero__actions">
          <a class="button button--primary" href="https://github.com/YakaTeam/kotatsu-parsers" target="_blank" rel="noreferrer">
            Inspect source repo
          </a>
          <a class="button button--ghost" href="https://github.com/YumemiProject/yumemiproject.github.io" target="_blank" rel="noreferrer">
            Design reference
          </a>
        </div>
      </div>

      <aside class="hero__panel">
        <div class="hero__score-ring">
          <span>{{ healthScore }}%</span>
          <small>Healthy</small>
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
            <span>Branch</span>
            <strong>{{ dataset.sourceRepo.branch }}</strong>
          </li>
        </ul>
      </aside>
    </header>

    <section class="metrics-grid">
      <MetricCard
        label="Total sources"
        :value="formatNumber(dataset.summary.total || dataset.sources.length)"
        hint="Parsed from annotated Kotlin parser files"
      />
      <MetricCard
        label="Working"
        :value="formatNumber(dataset.summary.working)"
        hint="Those working ones"
      />
      <MetricCard
        label="Broken"
        :value="formatNumber(dataset.summary.broken)"
        hint="Network failures or dead endpoints"
      />
      <MetricCard
        label="Blocked / unknown"
        :value="formatNumber(dataset.summary.blocked + dataset.summary.unknown)"
        hint="Wrong domain, anti-bot, cloud security, no access"
      />
    </section>

    <section class="info-banner card">
      <p>
        {{ dataset.disclaimer }}
      </p>
      <p v-if="error" class="info-banner__error">
        Live dataset failed to load: {{ error }}
      </p>
    </section>

    <section class="controls card">
      <div class="controls__row controls__row--grow">
        <label class="field field--search">
          <span>Search</span>
          <input v-model="query" type="search" placeholder="Find by key, title, domain, path..." />
        </label>

        <label class="field">
          <span>Status</span>
          <select v-model="status">
            <option value="all">All statuses</option>
            <option value="working">Working</option>
            <option value="blocked">Blocked</option>
            <option value="unknown">Unknown</option>
            <option value="broken">Broken</option>
          </select>
        </label>

        <label class="field">
          <span>Language</span>
          <select v-model="language">
            <option v-for="option in languages" :key="option" :value="option">
              {{ option === 'all' ? 'All languages' : option.toUpperCase() }}
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

      <div class="controls__row controls__row--compact">
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
      <p>Loading dataset…</p>
    </section>

    <section v-else-if="filteredSources.length === 0" class="empty-state card">
      <h2>No sources matched your filters.</h2>
      <p>Reset the filters or generate the live dataset so the dashboard has real source records to render.</p>
    </section>

    <section v-else :class="['sources', `sources--${view}`]">
      <SourceCard v-for="source in filteredSources" :key="source.id" :source="source" />
    </section>
  </div>
</template>
