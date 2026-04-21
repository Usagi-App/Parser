<script setup lang="ts">
import { computed } from 'vue'
import type { SourceStatus } from '@/types'

const props = defineProps<{
  status: SourceStatus
}>()

const label = computed(() => {
  switch (props.status) {
    case 'working':
      return 'Working'
    case 'blocked':
      return 'Blocked'
    case 'unknown':
      return 'Unknown'
    case 'broken':
    default:
      return 'Broken'
  }
})
</script>

<template>
  <span :class="['status-pill', `status-pill--${status}`]">
    {{ label }}
  </span>
</template>
