<script setup lang="ts">
import { computed } from 'vue'
import type { SourceItem } from '@/types'
import StatusPill from './StatusPill.vue'

const props = defineProps<{
  source: SourceItem
}>()

const websiteUrl = computed(() => {
  const domain = props.source.domains[0]
  if (!domain) return null
  return domain.startsWith('http://') || domain.startsWith('https://') ? domain : `https://${domain}`
})

const contentTypeLabel = computed(() => props.source.contentType ?? 'MANGA')
const reason = computed(() => props.source.brokenReason || props.source.health.reason)
</script>

<template>
  <article class="source-card">
    <div class="source-card__top">
      <div>
        <div class="source-card__eyebrow">{{ source.key }}</div>
        <h3>{{ source.title }}</h3>
      </div>
      <StatusPill :status="source.health.status" />
    </div>

    <div class="source-card__meta">
      <span>{{ source.language.toUpperCase() }}</span>
      <span>{{ contentTypeLabel }}</span>
      <span>{{ source.engine ?? 'Custom engine' }}</span>
      <span>{{ source.domains.length }} domain<span v-if="source.domains.length !== 1">s</span></span>
    </div>

    <p class="source-card__path">{{ source.path }}</p>

    <div class="domain-list">
      <span v-for="domain in source.domains.slice(0, 4)" :key="domain" class="domain-chip">
        {{ domain }}
      </span>
      <span v-if="source.domains.length > 4" class="domain-chip domain-chip--more">
        +{{ source.domains.length - 4 }} more
      </span>
      <span v-if="!source.domains.length" class="domain-chip domain-chip--more">
        No domain extracted yet
      </span>
    </div>

    <p v-if="reason" class="source-card__reason">
      {{ reason }}
    </p>

    <div class="source-card__footer">
      <p class="source-card__warning">Website opens a third-party domain.</p>
      <div class="source-card__actions">
        <a v-if="websiteUrl" class="button button--primary button--small" :href="websiteUrl" target="_blank" rel="noreferrer noopener">Website</a>
        <a class="button button--ghost button--small" :href="source.repoUrl" target="_blank" rel="noreferrer noopener">Parser file</a>
        <a class="button button--ghost button--small" :href="source.rawUrl" target="_blank" rel="noreferrer noopener">Raw</a>
      </div>
    </div>
  </article>
</template>
