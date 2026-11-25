import { lazy, Suspense, useCallback, useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { Activity, ChartNoAxesColumn, CircleAlert, Globe2, List, MapPin, Newspaper, Radio, RefreshCw } from 'lucide-react'
import type { Bucket, Window } from './api'
import { capitalize, categories, count, customWindow, freshnessStatus, liveWindow, readSelection, utc } from './format'
import { CategoryChart, CountryChart, EventFeed, PanelHeading } from './Panels'
import { CountryOverview, HistoricalAnalysis } from './Intelligence'
import { Coverage } from './Coverage'
import { useDashboard, useFreshness } from './useDashboard'

const ActivityMap = lazy(() => import('./ActivityMap'))
const VolumeChart = lazy(() => import('./VolumeChart'))
const screens = [
  { id: 'overview', label: 'Overview', icon: Globe2 },
  { id: 'indicators', label: 'Trends', icon: Activity },
  { id: 'coverage', label: 'News coverage', icon: Newspaper },
  { id: 'events', label: 'Event records', icon: List },
]

export default function App() {
  const [navigation, setNavigation] = useState(() => location.hash.slice(1) || 'overview')
  const [initial] = useState(() => readSelection(new URLSearchParams(location.search)))
  const [country, setCountry] = useState(initial.country)
  const [category, setCategory] = useState(initial.category)
  const [mode, setMode] = useState(initial.mode)
  const [custom, setCustom] = useState<Window | null>(initial.custom)
  const [customOpen, setCustomOpen] = useState(false)
  const [rangeError, setRangeError] = useState(false)
  const [refresh, setRefresh] = useState(0)
  const [profileFocus, setProfileFocus] = useState(0)
  const freshness = useFreshness(refresh)
  const exportTime = mode === 'custom' ? null : freshness.data?.latest_export_time_utc ?? null
  const window = useMemo(
    () => mode === 'custom' && custom ? custom : liveWindow(mode === '7' ? 7 : 1, exportTime),
    // Manual refresh resolves a new live window even when the export is unchanged.
    [mode, custom, exportTime, refresh, freshness.checked, freshness.error],
  )
  const hours = (Date.parse(window.end) - Date.parse(window.start)) / 3_600_000
  const bucket: Bucket = hours <= 48 ? 'hour' : 'day'
  const scope = JSON.stringify([country, category, mode, mode === 'custom' ? custom : null])
  const board = useDashboard({ ...window, country, category }, bucket, scope, freshness.checked, refresh)
  const analytics = board.data?.analytics
  const displayedWindow = board.data?.filters ?? window
  const countries = useMemo(() => board.countries.filter((row) => row.country_code).sort(
    (a, b) => a.country_name.localeCompare(b.country_name) || a.country_code!.localeCompare(b.country_code!),
  ), [board.countries])
  const toggleCountry = useCallback((code: string) => setCountry((previous) => previous === code ? '' : code), [])
  const openCountry = useCallback((code: string) => {
    setCountry(code)
    setProfileFocus((current) => current + 1)
  }, [])
  const toggleCategory = (code: string) => setCategory((previous) => previous === code ? '' : code)
  const countryName = countries.find((row) => row.country_code === country)?.country_name ?? country

  useEffect(() => {
    if (profileFocus) document.getElementById('country-profile')?.focus()
  }, [profileFocus])

  useEffect(() => {
    const updateNavigation = () => setNavigation(location.hash.slice(1) || 'overview')
    globalThis.addEventListener('hashchange', updateNavigation)
    return () => globalThis.removeEventListener('hashchange', updateNavigation)
  }, [])

  useEffect(() => {
    if (location.hash) document.getElementById(navigation)?.scrollIntoView()
  }, [navigation])

  useEffect(() => {
    const url = new URL(location.href)
    const params = { country, category, days: mode === '7' ? '7' : '',
      start: mode === 'custom' ? custom?.start : '', end: mode === 'custom' ? custom?.end : '' }
    for (const [key, value] of Object.entries(params)) {
      if (value) url.searchParams.set(key, value)
      else url.searchParams.delete(key)
    }
    if (url.href !== location.href) history.replaceState(history.state, '', url)
  }, [country, category, mode, custom])

  function changeMode(value: string) {
    setRangeError(false)
    if (value === 'custom') {
      setCustomOpen(true)
    } else {
      setMode(value === '7' ? '7' : '1')
      setCustomOpen(false)
    }
  }

  function applyCustom(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const values = new FormData(event.currentTarget)
    const parsed = customWindow(String(values.get('start')), String(values.get('end')))
    setRangeError(!parsed)
    if (parsed) {
      setCustom(parsed)
      setMode('custom')
      setCustomOpen(false)
    }
  }

  function exploreCoverage({ start, end }: Window, nextCategory?: string) {
    setCustom({ start: new Date(start).toISOString(), end: new Date(end).toISOString() })
    setMode('custom')
    setCustomOpen(false)
    setRangeError(false)
    if (nextCategory) setCategory(nextCategory)
    location.hash = 'coverage'
    document.getElementById('coverage')?.focus()
  }

  function refreshNow() {
    setRefresh((current) => current + 1)
  }

  const status = freshnessStatus(freshness.data?.status, freshness.error)
  const topCategory = analytics?.category_counts[0]
  const metrics = [
    { label: 'Reported events', value: analytics && count(analytics.summary.total_events), icon: Activity,
      detail: 'GDELT records in this selection' },
    { label: 'Countries with reports', value: analytics && count(analytics.summary.countries_with_events), icon: Globe2,
      detail: 'Distinct event locations by country' },
    { label: 'Events on the map', value: analytics && count(analytics.summary.geolocated_events), icon: MapPin,
      detail: analytics?.summary.total_events
        ? `${(100 * analytics.summary.geolocated_events / analytics.summary.total_events).toFixed(1)}% of selected events`
        : 'Events with valid coordinates' },
    { label: 'Most reported category', value: topCategory && capitalize(topCategory.primary_category), icon: ChartNoAxesColumn,
      detail: topCategory ? `${count(topCategory.count)} events` : 'By event count' },
  ]

  return (
    <div className="shell">
      <a className="skip-link" href="#main">Skip to dashboard</a>
      <aside className="sidebar">
        <a href="#overview" className="brand">
          <div className="brand-icon"><Globe2 size={22} strokeWidth={1.8} /></div>
          <span>Global News<br />{' '}Monitor</span>
        </a>
        <nav aria-label="Dashboard sections">
          {screens.map(({ id, label, icon: Icon }) => (
            <a key={id} href={`#${id}`} aria-current={navigation === id ? 'location' : undefined}>
              <Icon size={17} />{label}
            </a>
          ))}
        </nav>
        <div className="sidebar-foot">
          <Radio size={16} />
          <div><strong>GDELT Project</strong><span>Event database</span></div>
        </div>
      </aside>

      <main id="main" className="workspace" tabIndex={-1}>
        <header className="overview-header" id="overview">
          <div className="intro">
            <h1>Global reporting activity</h1>
            <p>See where events are reported, how coverage changes, and which articles are linked to the records.</p>
            <p className="intro-note">GDELT automatically codes events from news reports. The records can be incomplete, repeated, or misclassified.</p>
          </div>
          <div className="feed-status">
            <div className="feed-status-heading">
              <span className={`status-pill ${status.tone}`} role="status"><i />{status.label}</span>
              <button type="button" className="icon-button" title="Refresh dashboard" aria-label="Refresh dashboard"
                disabled={board.loading} onClick={refreshNow}><RefreshCw size={16} /></button>
            </div>
            <dl>
              <div><dt>Latest source batch</dt><dd>{utc(freshness.data?.latest_export_time_utc ?? null)}</dd></div>
              <div><dt>Added to dashboard</dt><dd>{utc(freshness.data?.latest_export_completed_at_utc ?? null)}</dd></div>
            </dl>
            <small className="feed-clock-note">GDELT labels batches by scheduled slot. A file can arrive before its slot time.</small>
          </div>
        </header>

        <section className="filters-panel" aria-label="Dashboard filters">
          <div className="filter-grid">
            <label>Country<select aria-label="Country" value={country} onChange={(event) => setCountry(event.target.value)}>
              <option value="">All countries</option>
              {country && !countries.some((row) => row.country_code === country) && <option value={country}>{country}</option>}
              {countries.map((row) => <option key={row.country_code} value={row.country_code ?? ''}>
                {row.country_name} ({row.country_code})
              </option>)}
            </select></label>
            <label>Category<select aria-label="Category" value={category} onChange={(event) => setCategory(event.target.value)}>
              <option value="">All categories</option>
              {categories.map((item) => <option key={item} value={item}>{capitalize(item)}</option>)}
            </select></label>
            <label>Time window<select aria-label="Time window" value={customOpen ? 'custom' : mode} onChange={(event) => changeMode(event.target.value)}>
              <option value="1">Last 24 hours</option>
              <option value="7">Last 7 days</option>
              <option value="custom">Custom range (UTC)</option>
            </select></label>
            <button className="text-button reset-filters" type="button"
              disabled={!country && !category && mode === '1' && !customOpen}
              onClick={() => { setCountry(''); setCategory(''); changeMode('1') }}>
              Reset filters
            </button>
          </div>
          {country && <a className="country-profile-link" href="#country-profile">View {countryName} profile</a>}
          {mode === 'custom' && !customOpen && <button type="button" className="text-button country-profile-link"
            onClick={() => setCustomOpen(true)}>Edit dates</button>}
          {customOpen && (
            <form className="custom-range" onSubmit={applyCustom}>
              <label>Start (UTC)<input type="datetime-local" name="start" defaultValue={window.start.slice(0, 16)} required /></label>
              <label>End (UTC)<input type="datetime-local" name="end" defaultValue={window.end.slice(0, 16)} required /></label>
              <button type="submit" className="outline-button">Apply range</button>
              {rangeError && <span className="range-error" role="alert">Choose an end after the start and a range of at most 30 days.</span>}
            </form>
          )}
        </section>

        <div className="window-line">
          <span>Showing {utc(displayedWindow.start)} to {utc(displayedWindow.end)}</span>
          <span role="status">
            {board.loading ? 'Loading dashboard…' : board.data ? `Dashboard refreshed ${utc(board.data.refreshedAt)}` : ''}
          </span>
        </div>
        {board.error && (
          <div className="api-error" role="alert">
            <CircleAlert size={18} />
            <span>{board.data ? 'Refresh failed. Showing the last successful result for this selection.'
              : 'Dashboard data is temporarily unavailable.'}</span>
            <button type="button" className="outline-button" onClick={refreshNow}>Try again</button>
          </div>
        )}

        <section className="metrics" aria-label="Selected activity summary">
          {metrics.map(({ label, value, detail, icon: Icon }) => (
            <div className="metric" key={label}>
              <div className="metric-top"><span>{label}</span><Icon size={17} /></div>
              <strong>{value ?? '—'}</strong>
              <small>{detail}</small>
            </div>
          ))}
        </section>

        {country && <CountryOverview name={countryName} filters={board.data?.filters ?? null}
          analytics={analytics} refresh={refresh} unavailable={board.error && !board.data} />}

        <div className="overview-grid">
          <section className="panel map-panel">
            <PanelHeading title="Where events are reported" subtitle="Brighter cells show more records. Zoom for detail or select a country to explore." />
            <Suspense fallback={<div className="map-frame map-fallback">Loading world map…</div>}>
              <ActivityMap filters={board.data?.filters ?? null} countries={board.countries} activeCountry={country}
                onCountry={openCountry} pending={board.loading} geolocated={analytics?.summary.geolocated_events} refresh={refresh} />
            </Suspense>
          </section>
          <div className="overview-side">
            <Suspense fallback={<section className="panel volume-panel"><PanelHeading title="Reporting over time" />
              <p className="panel-empty">Loading activity chart…</p></section>}>
              <VolumeChart analytics={analytics} window={displayedWindow} bucket={board.data?.bucket ?? bucket} loading={board.loading} />
            </Suspense>
            <CountryChart analytics={analytics} country={country} onCountry={toggleCountry} loading={board.loading} />
          </div>
        </div>

        <div className="analysis-grid">
          <CategoryChart analytics={analytics} category={category} onCategory={toggleCategory} loading={board.loading} />
          <HistoricalAnalysis filters={board.data?.filters ?? null} refresh={refresh}
            unavailable={board.error && !board.data} exploreCoverage={exploreCoverage} />
        </div>
        <Coverage filters={board.data?.filters ?? null} placeName={countryName || 'Worldwide'} scope={scope} refresh={refresh}
          requested={navigation === 'coverage'} unavailable={board.error && !board.data} />
        <EventFeed page={board.data?.events} loading={board.loading} loadingPage={board.loadingPage}
          pageError={board.pageError} loadMore={board.loadMore} />
        <footer className="workspace-footer">
          <span>Source coverage varies by place and time. Treat changes as reporting signals.</span>
          <span>Latest reported event date {freshness.data?.latest_event_occurrence_date ?? '—'}</span>
        </footer>
      </main>
    </div>
  )
}
