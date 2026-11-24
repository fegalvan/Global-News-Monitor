import type { Bucket, Coverage, Freshness, Sample, Window } from './api'

const HOUR = 3_600_000
const DAY = 24 * HOUR
const dateTime = new Intl.DateTimeFormat('en-GB', {
  day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit',
  hourCycle: 'h23', timeZone: 'UTC',
})
const numberFormat = new Intl.NumberFormat('en-US')

export const categories = ['conflict', 'crisis', 'cyber', 'diplomacy', 'economics', 'politics', 'protest']
export const categoryColors: Record<string, string> = {
  conflict: '#df8a83', crisis: '#daa66c', cyber: '#b49adc', diplomacy: '#62cbb7',
  economics: '#83aef0', politics: '#b0bdcf', protest: '#d5bf73',
}
export const capitalize = (label: string) => label.charAt(0).toUpperCase() + label.slice(1)
export const count = (value: number) => numberFormat.format(value)
export const utc = (iso: string | null) => iso ? `${dateTime.format(new Date(iso))} UTC` : '—'

const freshnessLabels = {
  healthy: 'Fresh data',
  source_stale: 'Source data stale',
  ingestion_stale: 'Ingestion stale',
  no_successful_ingest: 'Awaiting ingestion',
  unavailable: 'API unavailable',
  checking: 'Checking data',
}

export function freshnessStatus(status: Freshness['status'] | undefined, error: boolean) {
  const tone = error ? 'unavailable' : status ?? 'checking'
  return { tone, label: freshnessLabels[tone] }
}

export function liveWindow(days: number, sourceExport: string | null, now = Date.now()): Window {
  // The upstream export clock can be slightly ahead of this browser.
  const end = Math.max(now, sourceExport ? Date.parse(sourceExport) + 1 : now)
  return { start: new Date(end - days * DAY).toISOString(), end: new Date(end).toISOString() }
}

export function customWindow(startInput: string, endInput: string): Window | null {
  if (!startInput || !endInput) return null
  const start = Date.parse(`${startInput}Z`)
  const end = Date.parse(`${endInput}Z`)
  if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || end <= start || end - start > 30 * DAY) {
    return null
  }
  return { start: new Date(start).toISOString(), end: new Date(end).toISOString() }
}

export function activitySeries(rows: Sample[], window: Window, bucket: Bucket, coverage: Coverage[]) {
  const size = bucket === 'hour' ? HOUR : DAY
  const first = Math.floor(Date.parse(window.start) / size) * size
  const end = Date.parse(window.end)
  const counts = new Map(rows.map((row) => [Date.parse(row.bucket_start), row.count]))
  const observed = new Set(coverage.filter(
    (row) => row.expected_exports > 0 && row.completed_exports === row.expected_exports,
  ).map((row) => Date.parse(row.bucket_start)))
  const series = []
  for (let time = first; time < end; time += size) {
    series.push({ time, count: counts.get(time) ?? (observed.has(time) ? 0 : null) })
  }
  return series
}

export function sourceLink(value: string | null) {
  if (!value) return null
  try {
    const url = new URL(value)
    return !url.username && !url.password && (url.protocol === 'https:' || url.protocol === 'http:') ? url.href : null
  } catch {
    return null
  }
}
