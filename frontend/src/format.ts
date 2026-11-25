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
  healthy: 'Updates arriving',
  source_stale: 'Source updates delayed',
  ingestion_stale: 'Dashboard updates delayed',
  no_successful_ingest: 'Waiting for the first update',
  unavailable: 'Update status unavailable',
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

function validWindow(start: number, end: number): Window | null {
  if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || end <= start || end - start > 30 * DAY) {
    return null
  }
  return { start: new Date(start).toISOString(), end: new Date(end).toISOString() }
}

export function customWindow(startInput: string, endInput: string): Window | null {
  return validWindow(Date.parse(`${startInput}Z`), Date.parse(`${endInput}Z`))
}

export function readSelection(params: URLSearchParams) {
  const country = params.get('country')?.toUpperCase() ?? ''
  const category = params.get('category') ?? ''
  const start = params.get('start') ?? ''
  const end = params.get('end') ?? ''
  const custom = start.endsWith('Z') && end.endsWith('Z') ? validWindow(Date.parse(start), Date.parse(end)) : null
  const mode = custom ? 'custom' : params.get('days') === '7' ? '7' : '1'
  return {
    country: /^[A-Z]{2,3}$/.test(country) ? country : '', category: categories.includes(category) ? category : '',
    custom, mode,
  } as const
}

export function activitySeries(rows: Sample[], window: Window, bucket: Bucket, coverage: Coverage[]) {
  const size = bucket === 'hour' ? HOUR : DAY
  const start = Date.parse(window.start)
  const first = Math.floor(start / size) * size
  const end = Date.parse(window.end)
  const counts = new Map(rows.map((row) => [Date.parse(row.bucket_start), row.count]))
  const observed = new Set(coverage.filter(
    (row) => row.expected_exports === size / (15 * 60_000) && row.completed_exports === row.expected_exports,
  ).map((row) => Date.parse(row.bucket_start)))
  const series = []
  for (let time = first; time < end; time += size) {
    const complete = time >= start && time + size <= end && observed.has(time)
    series.push({ time, count: complete ? counts.get(time) ?? 0 : null })
  }
  return series
}

export function sourceLink(value: string | null, domain?: string) {
  const trimmed = value?.trim()
  if (!trimmed || !/^https?:\/\//i.test(trimmed) || /[\s\\\u0000-\u001f\u007f]/.test(trimmed)) return null
  try {
    const url = new URL(trimmed)
    return !url.username && !url.password && (!domain || url.hostname === domain)
      && (url.protocol === 'https:' || url.protocol === 'http:') ? url.href : null
  } catch {
    return null
  }
}
