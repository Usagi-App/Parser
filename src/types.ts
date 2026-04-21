export type SourceStatus = 'working' | 'broken' | 'blocked' | 'unknown'

export interface HealthCheck {
  status: SourceStatus
  checkedAt: string | null
  latencyMs: number | null
  httpStatus: number | null
  finalUrl: string | null
  reason: string | null
}

export interface SourceItem {
  id: string
  key: string
  title: string
  language: string
  languageName?: string
  engine: string | null
  path: string
  repoUrl: string
  rawUrl: string
  domains: string[]
  health: HealthCheck

  contentType?: string | null
  brokenReason?: string | null
  nsfw?: boolean
  searchText?: string
}

export interface DataSummary {
  total: number
  working: number
  broken: number
  blocked: number
  unknown: number
  nsfw?: number
}

export interface SourceDataset {
  generatedAt: string | null
  generatedBy?: string | null
  sourceRepo: {
    owner: string
    repo: string
    branch: string
  }
  summary: DataSummary
  sources: SourceItem[]
  disclaimer: string
  byLocale?: Record<string, number>
  byType?: Record<string, number>
  duplicatesSkipped?: string[]
}
