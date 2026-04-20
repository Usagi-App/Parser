<script setup lang="ts">
defineProps<{
  label: string
  value: string | number
  hint?: string
}>()
</script>

<template>
  <article class="metric-card card">
    <p class="metric-card__label">{{ label }}</p>
    <strong class="metric-card__value">{{ value }}</strong>
    <p v-if="hint" class="metric-card__hint">{{ hint }}</p>
  </article>
</template>
