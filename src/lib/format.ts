import type { SourceStatus } from '@/types'

const LOCALE = 'en-US'

export function formatNumber(value: number): string {
  return new Intl.NumberFormat(LOCALE).format(value)
}

export function formatDate(value: string | null): string {
  if (!value) return 'Not generated yet'

  const date = new Date(value)

  if (Number.isNaN(date.getTime())) {
    return 'Invalid date'
  }

  return new Intl.DateTimeFormat(LOCALE, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(date)
}

export function titleCaseStatus(status: SourceStatus): string {
  switch (status) {
    case 'working':
      return 'Working'
    case 'broken':
      return 'Broken'
    case 'blocked':
      return 'Blocked'
    case 'unknown':
    default:
      return 'Unknown'
  }
}

export function toLatencyLabel(value: number | null): string {
  if (value === null) return '—'
  return `${Math.round(value)} ms`
}
