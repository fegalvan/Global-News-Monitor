import assert from 'node:assert/strict'
import test from 'node:test'
import { queryParams } from '../src/api.ts'
import { activitySeries, customWindow, freshnessStatus, liveWindow, sourceLink } from '../src/format.ts'
import { cellOrigin, mapPolygon } from '../src/mapGeometry.ts'

test('shared query retains exclusive end, filters, and both cursor fields', () => {
  const window = { start: '2026-11-23T22:00:00.000Z', end: '2026-11-24T00:00:00.000Z' }
  const params = new URLSearchParams(queryParams({ ...window, country: 'US', category: 'diplomacy' }, {
    limit: 20, before_time: '2026-11-23T23:45:00+00:00', before_id: 123,
  }))
  assert.deepEqual(Object.fromEntries(params), {
    ...window, country: 'US', category: 'diplomacy', limit: '20',
    before_time: '2026-11-23T23:45:00+00:00', before_id: '123',
  })
})

test('custom ranges use UTC and reject reversed or oversized ranges', () => {
  assert.equal(customWindow('2026-11-22T03:00', '2026-11-22T04:00')?.start, '2026-11-22T03:00:00.000Z')
  assert.equal(customWindow('2026-11-24T00:00', '2026-11-23T00:00'), null)
  assert.equal(customWindow('2026-01-01T00:00', '2026-02-01T00:00'), null)
})

test('a live end includes a processed export with an ahead-of-browser clock', () => {
  const source = '2026-11-23T23:45:00Z'
  const window = liveWindow(1, source, Date.parse(source) - 60_000)
  assert.equal(window.end, '2026-11-23T23:45:00.001Z')
})

test('source staleness, ingestion staleness, and API outages have distinct labels', () => {
  assert.equal(freshnessStatus('source_stale', false).label, 'Source data stale')
  assert.equal(freshnessStatus('ingestion_stale', false).label, 'Ingestion stale')
  assert.equal(freshnessStatus('healthy', true).label, 'API unavailable')
})

test('time-series leaves partial edges and incomplete export coverage out of the trend', () => {
  const window = { start: '2026-11-23T22:30:00Z', end: '2026-11-24T01:15:00Z' }
  const rows = [
    { bucket_start: '2026-11-23T23:00:00Z', count: 12 },
    { bucket_start: '2026-11-24T01:00:00Z', count: 17 },
  ]
  const coverage = [
    { bucket_start: '2026-11-23T22:00:00Z', expected_exports: 2, completed_exports: 2 },
    { bucket_start: '2026-11-23T23:00:00Z', expected_exports: 4, completed_exports: 1 },
    { bucket_start: '2026-11-24T00:00:00Z', expected_exports: 4, completed_exports: 4 },
    { bucket_start: '2026-11-24T01:00:00Z', expected_exports: 1, completed_exports: 1 },
  ]
  assert.deepEqual(activitySeries(rows, window, 'hour', coverage).map(({ count }) => count), [null, null, 0, null])
})

test('cell lookup agrees with the API at the poles, date line, and fractional grid', () => {
  assert.equal(cellOrigin(90, -90, 2), 88)
  assert.equal(cellOrigin(180, -180, 2), 178)
  assert.equal(cellOrigin(-180, -180, 2), -180)
  assert.equal(cellOrigin(-0.1, -180, 0.25), -0.25)
})

test('event links only allow HTTP and HTTPS URLs', () => {
  assert.equal(sourceLink('https://example.com/story'), 'https://example.com/story')
  assert.equal(sourceLink('javascript:alert(1)'), null)
  assert.equal(sourceLink('/relative'), null)
  assert.equal(sourceLink('https://example.com@other.example/story'), null)
})

test('a polygon crossing the date line is drawn on both edges without a transglobal line', () => {
  const [east, west] = mapPolygon([[[179, 40], [-179, 40], [-179, 42], [179, 40]]])
  assert.deepEqual(east[0].map(([longitude]) => longitude), [179, 181, 181, 179])
  assert.deepEqual(west[0].map(([longitude]) => longitude), [-181, -179, -179, -181])
})
