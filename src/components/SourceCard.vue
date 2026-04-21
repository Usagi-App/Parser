<script setup lang="ts">
import { computed } from 'vue'
import type { SourceItem } from '@/types'
import StatusPill from './StatusPill.vue'

const props = defineProps<{
  source: SourceItem
  compact?: boolean
}>()

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

const websiteUrl = computed(() => {
  const domain = props.source.domains[0]
  return domain ? `https://${domain}` : null
})

const visibleDomains = computed(() => {
  return props.source.domains.slice(0, props.compact ? 2 : 4)
})

const hiddenDomainCount = computed(() => {
  return Math.max(props.source.domains.length - visibleDomains.value.length, 0)
})

const displayLanguage = computed(() => {
  return props.source.languageName || props.source.language.toUpperCase()
})

const contentTypeLabel = computed(() => {
  return formatContentType(props.source.contentType)
})

const brokenReason = computed(() => {
  return props.source.brokenReason || props.source.health.reason
})
</script>

<template>
  <article :class="['source-card', { 'source-card--compact': compact }]">
    <div class="source-card__top">
      <div class="source-card__title-wrap">
        <p class="source-card__eyebrow">{{ contentTypeLabel }}</p>
        <h3>{{ source.title }}</h3>
        <p v-if="!compact" class="source-card__path">{{ source.path }}</p>
      </div>

      <div class="source-card__badges">
        <StatusPill :status="source.health.status" />
        <span v-if="source.nsfw" class="source-card__tag source-card__tag--nsfw">NSFW</span>
      </div>
    </div>

    <div class="source-card__meta">
      <span>{{ displayLanguage }}</span>
      <span>{{ source.engine ?? 'Unknown engine' }}</span>
      <span>{{ source.key }}</span>
      <span>{{ source.domains.length }} domain<span v-if="source.domains.length !== 1">s</span></span>
    </div>

    <p v-if="brokenReason && !compact" class="source-card__reason">
      {{ brokenReason }}
    </p>

    <div v-if="visibleDomains.length && !compact" class="domain-list">
      <span v-for="domain in visibleDomains" :key="domain" class="domain-chip">{{ domain }}</span>
      <span v-if="hiddenDomainCount > 0" class="domain-chip domain-chip--more">
        +{{ hiddenDomainCount }} more
      </span>
    </div>

    <div class="source-card__footer">
      <div class="source-card__actions">
        <a
          v-if="websiteUrl"
          class="button button--website button--small"
          :href="websiteUrl"
          target="_blank"
          rel="noreferrer noopener"
        >
          Website
        </a>

        <a
          class="button button--ghost button--small"
          :href="source.repoUrl"
          target="_blank"
          rel="noreferrer noopener"
        >
          File
        </a>

        <a
          v-if="!compact"
          class="button button--ghost button--small"
          :href="source.rawUrl"
          target="_blank"
          rel="noreferrer noopener"
        >
          Raw
        </a>
      </div>
    </div>
  </article>
</template>
