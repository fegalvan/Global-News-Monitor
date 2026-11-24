export type Window = { start: string; end: string }
export type Filters = Window & { country?: string; category?: string }
export type Bucket = 'hour' | 'day'
export type Sample = { bucket_start: string; count: number }
export type Coverage = { bucket_start: string; expected_exports: number; completed_exports: number }
export type Country = { country_code: string | null; country_name: string; count: number }
export type Cell = { latitude: number; longitude: number; count: number }

export type Analytics = {
  summary: { total_events: number; geolocated_events: number; countries_with_events: number }
  series: Sample[]
  coverage: Coverage[]
  country_counts: Country[]
  country_counts_truncated: boolean
  category_counts: { primary_category: string; count: number }[]
  map: { cell_degrees: number; cells: Cell[]; omitted_events: number }
}

export type Event = {
  event_id: number
  event_time_utc: string
  event_date: string | null
  event_label: string
  original_event_code: string | null
  event_code: string | null
  actor1_name: string | null
  actor2_name: string | null
  country_code: string | null
  country_name: string
  location_name: string | null
  primary_category: string
  source_url: string | null
}

export type Cursor = { before_time: string; before_id: number }
export type EventPage = { rows: Event[]; next_cursor: Cursor | null }
export type Spike = {
  country_name: string; category: string; recent_count: number
  baseline_avg: number; z_score: number | null; lift_ratio: number
}
export type Tension = {
  actor1: string; actor2: string; category: string
  event_count: number; avg_tone: number; worst_tone: number
}
export type Freshness = {
  latest_export_time_utc: string | null
  latest_ingestion_completed_at_utc: string | null
  latest_event_occurrence_date: string | null
  status: 'healthy' | 'no_successful_ingest' | 'source_stale' | 'ingestion_stale'
}

export function queryParams(filters: Filters, extra: Record<string, string | number> = {}) {
  const result = new URLSearchParams({ start: filters.start, end: filters.end })
  if (filters.country) result.set('country', filters.country)
  if (filters.category) result.set('category', filters.category)
  for (const [key, value] of Object.entries(extra)) result.set(key, String(value))
  return result.toString()
}

export async function request<T>(path: string, signal: AbortSignal): Promise<T> {
  const base = (import.meta.env.VITE_API_BASE_URL || '/api').replace(/\/+$/, '')
  const response = await fetch(base + path, {
    signal: AbortSignal.any([signal, AbortSignal.timeout(20_000)]), cache: 'no-store',
  })
  if (!response.ok) throw new Error(`API returned HTTP ${response.status}`)
  if (!response.headers.get('content-type')?.includes('application/json')) {
    throw new Error('The API did not return JSON')
  }
  return response.json()
}

export function getEvents(filters: Filters, signal: AbortSignal, cursor?: Cursor) {
  return request<EventPage>(`/events?${queryParams(filters, { limit: 20, ...cursor })}`, signal)
}
