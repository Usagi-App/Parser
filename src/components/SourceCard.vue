<script setup lang="ts">
import { computed } from 'vue'
import type { SourceItem } from '@/types'
import StatusPill from './StatusPill.vue'

const props = defineProps<{
  source: SourceItem
  compact?: boolean
}>()

const websiteUrl = computed(() => {
  const domain = props.source.domains[0]
  if (!domain) return null

  return domain.startsWith('http://') || domain.startsWith('https://')
    ? domain
    : `https://${domain}`
})

const contentTypeLabel = computed(() => props.source.contentType ?? 'MANGA')
const reason = computed(() => props.source.brokenReason || props.source.health.reason)
const languageLabel = computed(() => props.source.languageName || props.source.language.toUpperCase())
const visibleDomains = computed(() =>
  props.compact ? props.source.domains.slice(0, 2) : props.source.domains.slice(0, 4),
)
</script>

<template>
  <article :class="['source-card', { 'source-card--compact': compact }]">
    <div class="source-card__top">
      <div class="source-card__title-wrap">
        <div class="source-card__eyebrow">{{ source.key }}</div>
        <h3 :title="source.title">{{ source.title }}</h3>
      </div>

      <div class="source-card__badges">
        <span v-if="source.nsfw" class="source-card__tag source-card__tag--nsfw">NSFW</span>
        <StatusPill :status="source.health.status" />
      </div>
    </div>

    <div class="source-card__meta">
      <span>{{ languageLabel }}</span>
      <span>{{ contentTypeLabel }}</span>
      <span v-if="!compact">{{ source.engine ?? 'Custom engine' }}</span>
      <span>{{ source.domains.length }} domain<span v-if="source.domains.length !== 1">s</span></span>
    </div>

    <p v-if="!compact" class="source-card__path">{{ source.path }}</p>

    <div class="domain-list">
      <span v-for="domain in visibleDomains" :key="domain" class="domain-chip">
        {{ domain }}
      </span>

      <span
        v-if="source.domains.length > visibleDomains.length"
        class="domain-chip domain-chip--more"
      >
        +{{ source.domains.length - visibleDomains.length }} more
      </span>

      <span v-if="!source.domains.length" class="domain-chip domain-chip--more">
        No domain extracted yet
      </span>
    </div>

    <p v-if="reason && !compact" class="source-card__reason">
      {{ reason }}
    </p>

    <div class="source-card__footer">
      <p v-if="!compact" class="source-card__warning">Website opens a third-party domain.</p>

      <div class="source-card__actions">
        <a
          v-if="websiteUrl"
          class="button button--primary button--small"
          :href="websiteUrl"
          target="_blank"
          rel="noreferrer noopener"
        >
          🌐 Website
        </a>

        <a
          v-if="!compact"
          class="button button--ghost button--small"
          :href="source.repoUrl"
          target="_blank"
          rel="noreferrer noopener"
        >
          📄 Parser
        </a>

        <a
          v-if="!compact"
          class="button button--ghost button--small"
          :href="source.rawUrl"
          target="_blank"
          rel="noreferrer noopener"
        >
          🧩 Raw
        </a>
      </div>
    </div>
  </article>
</template>
