import type { Analytics, Comparison, CountryProfile, Filters, History, Window } from './api'
import { queryParams } from './api'
import { categoryLabel, count, utc } from './format'
import { PanelHeading } from './Panels'
import { useResource } from './useDashboard'
import { lazy, Suspense } from 'react'

const HistoryChart = lazy(() => import('./HistoryChart'))

const anomalyLabels: Record<History['anomaly']['status'], string> = {
  incomplete_observation: 'Some source updates are missing from this 24-hour period',
  insufficient_history: 'More comparable weeks are needed',
  low_baseline_volume: 'Too few typical records to flag an increase reliably',
  elevated: 'Unusually high reporting activity',
  within_threshold: 'No unusual increase flagged',
  category_rules_changed: 'This period uses older category rules',
}

function signed(value: number, digits = 0) {
  return new Intl.NumberFormat(undefined, { signDisplay: 'exceptZero', maximumFractionDigits: digits }).format(value)
}

type ExploreCoverage = (period: Window, category?: string) => void

function Change({ title, comparison, exploreCoverage }: {
  title: string; comparison: Comparison; exploreCoverage: ExploreCoverage
}) {
  const { current, previous, absolute_change, percent_change } = comparison
  const observed = ({ completed, expected, observed }: Comparison['current']) =>
    completed === 0 && observed === 0 ? 'no data' : `${count(observed)}${completed === expected ? '' : ' available'}`
  const shifts = comparison.category_changes?.filter((row) => row.change !== 0) ?? []
  return (
    <div className="history-change">
      <h3>{title}</h3>
      {absolute_change === null
        ? <strong>{comparison.category_rules_changed && current.completed === current.expected
          && previous.completed === previous.expected ? 'Category rules differ' : 'Incomplete coverage'}</strong>
        : <strong>{signed(absolute_change)} records{percent_change === null ? '' : ` · ${signed(percent_change, 1)}%`}</strong>}
      <p>Current {observed(current)} · Previous {observed(previous)}</p>
      {absolute_change === null && (current.completed !== current.expected || previous.completed !== previous.expected)
        && <small>Source updates {current.completed}/{current.expected} current · {previous.completed}/{previous.expected} previous</small>}
      {comparison.category_rules_changed && <small>Older records use different category rules. Changes by category are unavailable for this comparison.</small>}
      {absolute_change !== null && percent_change === null && <small>Percent change needs at least 20 previous records.</small>}
      {shifts.length > 0 && <details>
        <summary>Changes by category</summary>
        <ul className="trend-shifts">{shifts.map((row) => <li key={row.category}>
          <span>{categoryLabel(row.category)} · {count(row.previous)} → {count(row.current)}</span>
          <button type="button" className="text-button" onClick={() => exploreCoverage(row.change > 0 ? current : previous, row.category)}
            aria-label={`See ${categoryLabel(row.category)} coverage in the ${row.change > 0 ? 'current' : 'previous'} period`}>{signed(row.change)}</button>
        </li>)}</ul>
        <small>Select a change to see links from the period with more records.</small>
      </details>}
      <div className="trend-links">
        {current.observed > 0 && <button type="button" className="text-button" onClick={() => exploreCoverage(current)}>Current coverage</button>}
        {previous.observed > 0 && <button type="button" className="text-button" onClick={() => exploreCoverage(previous)}>Previous coverage</button>}
      </div>
    </div>
  )
}

export function HistoricalAnalysis({ filters, refresh, unavailable, exploreCoverage }: {
  filters: Filters | null; refresh: number; unavailable: boolean; exploreCoverage: ExploreCoverage
}) {
  const query = useResource<History>(filters ? `/history?${queryParams(filters)}` : null, refresh)
  const data = query.data
  return (
    <section className="panel history-panel" id="indicators" tabIndex={-1}>
      <PanelHeading title="Reporting trends" subtitle="See how record counts changed from the preceding period of the same length." />
      {data ? (
        <div className="history-content">
          <p className="history-time">Comparisons end at {utc(data.comparison_24h.current.end)}.</p>
          {filters?.category && data.category_history_from && data.daily.some((day) => Date.parse(day.start) < Date.parse(data.category_history_from!))
            && <p className="history-time">The category chart starts with reclassified history on {utc(data.category_history_from)}.</p>}
          <div className="history-comparisons">
            <Change title="24 hours vs. prior 24 hours" comparison={data.comparison_24h} exploreCoverage={exploreCoverage} />
            <Change title="7 days vs. prior 7 days" comparison={data.comparison_7d} exploreCoverage={exploreCoverage} />
          </div>
          <div className="anomaly-card">
            <h3>Unusual increases</h3>
            <strong className={data.anomaly.status === 'elevated' ? 'elevated-activity' : undefined}>
              {anomalyLabels[data.anomaly.status]}
            </strong>
            <p>{data.anomaly.baseline_samples} complete weekly comparisons · {data.anomaly.minimum_samples} needed
              {data.anomaly.expected === null ? '' : ` · Typical count ${count(data.anomaly.expected)}`}
              {data.anomaly.observed === null ? '' : ` · Current count ${count(data.anomaly.observed)}`}
            </p>
            {data.anomaly.status === 'elevated' && <button type="button" className="outline-button"
              onClick={() => exploreCoverage(data.comparison_24h.current)}>Explore links from this increase</button>}
            <details>
              <summary>How this indicator works</summary>
              <p>The baseline uses up to 12 complete 24-hour periods, ending on the same UTC weekday and hour.
                A flag requires a score of at least 3.5, at least 20 more records, and at least 50% more activity
                than the baseline median of 20 or more. Scores are descriptive. Source gaps, uneven reporting,
                and correlated stories can affect them.</p>
              {data.anomaly.score !== null && <p>Score {data.anomaly.score.toFixed(2)} · Baseline scale {data.anomaly.robust_scale?.toFixed(1)}</p>}
            </details>
          </div>
          {data.daily.some((day) => day.count !== null)
            ? <Suspense fallback={<p className="panel-empty">Loading historical chart…</p>}><HistoryChart daily={data.daily} /></Suspense>
            : <p className="panel-empty">The daily chart needs at least one complete UTC day of source updates.</p>}
        </div>
      ) : <p className="panel-empty" role={query.error || unavailable ? 'alert' : 'status'}>{query.error || unavailable
        ? 'Reporting history is unavailable. Refresh to retry.' : 'Loading reporting history…'}</p>}
    </section>
  )
}

export function CountryOverview({ filters, analytics, name, refresh, unavailable }: {
  filters: Filters | null; analytics?: Analytics; name: string; refresh: number; unavailable: boolean
}) {
  const query = useResource<CountryProfile>(filters?.country
    ? `/country-profile?${queryParams(filters, { include_sources: 'false' })}` : null, refresh)
  const coverage = analytics?.coverage.reduce((sum, row) =>
    ({ expected: sum.expected + row.expected_exports, completed: sum.completed + row.completed_exports }),
  { expected: 0, completed: 0 })
  return (
    <section className="panel country-profile" id="country-profile" tabIndex={-1}>
      <PanelHeading title={name} subtitle="Country profile based on the reported location of each event">
        <a href="#coverage" className="outline-button">Explore news coverage</a>
      </PanelHeading>
      <div className="profile-content profile-lists">
        {analytics && <div>
          <div className="profile-summary">
            <span><strong>{count(analytics.summary.total_events)}</strong> reported events</span>
          </div>
          <p className="profile-coverage">{count(analytics.summary.geolocated_events)} records with coordinates
            {' · '}{coverage?.completed ?? 0}/{coverage?.expected ?? 0} expected source updates processed.
            {' '}A map point can represent a country, region, or locality.</p>
        </div>}
        <div>
          <h3>Most reported locations</h3>
          {query.data ? query.data.locations.length ? <ol>{query.data.locations.map((row) => <li key={row.label}>
            <span>{row.label}</span><strong>{count(row.count)}</strong>
          </li>)}</ol> : <p>No location detail in this selection.</p>
            : <p role={query.error || unavailable ? 'alert' : 'status'}>{query.error || unavailable
              ? 'Country details are unavailable. Refresh to retry.' : 'Loading country details…'}</p>}
        </div>
      </div>
    </section>
  )
}
