import type { Analytics, Comparison, CountryProfile, Filters, History } from './api'
import { queryParams } from './api'
import { count, utc } from './format'
import { PanelHeading } from './Panels'
import { useResource } from './useDashboard'
import { lazy, Suspense } from 'react'

const HistoryChart = lazy(() => import('./HistoryChart'))

const anomalyLabels: Record<History['anomaly']['status'], string> = {
  incomplete_observation: 'Current 24-hour coverage is incomplete',
  insufficient_history: 'Insufficient comparable history',
  low_baseline_volume: 'Typical activity is too low to score reliably',
  elevated: 'Elevated reporting activity',
  within_threshold: 'Activity within the reporting threshold',
}

function signed(value: number, digits = 0) {
  return new Intl.NumberFormat(undefined, { signDisplay: 'exceptZero', maximumFractionDigits: digits }).format(value)
}

function Change({ title, comparison }: { title: string; comparison: Comparison }) {
  const { current, previous, absolute_change, percent_change } = comparison
  const observed = ({ completed, expected, observed }: Comparison['current']) =>
    completed === 0 && observed === 0 ? 'no observations'
      : `${count(observed)}${completed === expected ? '' : ' observed'}`
  return (
    <div className="history-change">
      <h3>{title}</h3>
      {absolute_change === null
        ? <strong>Incomplete coverage</strong>
        : <strong>{signed(absolute_change)} events{percent_change === null ? '' : ` · ${signed(percent_change, 1)}%`}</strong>}
      <p>Current {observed(current)} · Previous {observed(previous)}</p>
      <small>Exports {current.completed}/{current.expected} current · {previous.completed}/{previous.expected} previous</small>
      {absolute_change !== null && percent_change === null && <small>Percentage unavailable because the previous count was zero.</small>}
    </div>
  )
}

export function HistoricalAnalysis({ filters, refresh, unavailable }: {
  filters: Filters | null; refresh: number; unavailable: boolean
}) {
  const query = useResource<History>(filters ? `/history?${queryParams(filters)}` : null, refresh)
  const data = query.data
  return (
    <section className="panel history-panel" id="indicators">
      <PanelHeading title="Reporting trends" subtitle="Equivalent time periods and unusual activity in the selected country and category" />
      {data ? (
        <div className="history-content">
          <p className="history-time">Comparisons end at {utc(data.comparison_24h.current.end)}.</p>
          <div className="history-comparisons">
            <Change title="24 hours" comparison={data.comparison_24h} />
            <Change title="Seven days" comparison={data.comparison_7d} />
          </div>
          <div className="anomaly-card">
            <h3>Unusual reporting</h3>
            <strong className={data.anomaly.status === 'elevated' ? 'elevated-activity' : undefined}>
              {anomalyLabels[data.anomaly.status]}
            </strong>
            <p>{data.anomaly.baseline_samples} comparable periods · {data.anomaly.minimum_samples} required
              {' · '}Observed {data.anomaly.observed === null ? '—' : count(data.anomaly.observed)}
              {' · '}Expected {data.anomaly.expected === null ? '—' : count(data.anomaly.expected)}
              {data.anomaly.score === null ? '' : ` · Score ${data.anomaly.score.toFixed(2)}`}
              {data.anomaly.robust_scale === null ? '' : ` · Baseline scale ${data.anomaly.robust_scale.toFixed(1)}`}
            </p>
            <details>
              <summary>How this indicator works</summary>
              <p>The baseline uses up to 12 complete 24-hour periods, ending on the same UTC weekday and hour.
                A flag requires a score of at least 3.5, at least 20 more records, and at least 50% more activity
                than the baseline median of 20 or more. Scores are descriptive. Source gaps, uneven reporting,
                and correlated stories can affect them.</p>
            </details>
          </div>
          {data.daily.some((day) => day.count !== null)
            ? <Suspense fallback={<p className="panel-empty">Loading historical chart…</p>}><HistoryChart daily={data.daily} /></Suspense>
            : <p className="panel-empty">Daily trend appears after the first complete UTC day of exports.</p>}
        </div>
      ) : <p className="panel-empty" role={query.error || unavailable ? 'alert' : 'status'}>{query.error || unavailable
        ? 'Reporting history is unavailable. Refresh to retry.' : 'Loading reporting history…'}</p>}
    </section>
  )
}

export function CountryOverview({ filters, analytics, name, refresh, unavailable }: {
  filters: Filters | null; analytics?: Analytics; name: string; refresh: number; unavailable: boolean
}) {
  const query = useResource<CountryProfile>(filters?.country ? `/country-profile?${queryParams(filters)}` : null, refresh)
  const coverage = analytics?.coverage.reduce((sum, row) =>
    ({ expected: sum.expected + row.expected_exports, completed: sum.completed + row.completed_exports }),
  { expected: 0, completed: 0 })
  const categoryCount = (category: string) => analytics?.category_counts.find((row) => row.primary_category === category)?.count ?? 0
  return (
    <section className="panel country-profile" id="country-profile" tabIndex={-1}>
      <PanelHeading title={name} subtitle="Country profile · GDELT event geography">
        <a href="#events" className="outline-button">Recent events</a>
      </PanelHeading>
      <div className="profile-content">
        {analytics && <>
          <div className="profile-summary">
            <span><strong>{count(analytics.summary.total_events)}</strong> reported events</span>
            {(!filters?.category || filters.category === 'politics')
              && <span><strong>{count(categoryCount('politics'))}</strong> politics</span>}
            {(!filters?.category || filters.category === 'conflict')
              && <span><strong>{count(categoryCount('conflict'))}</strong> conflict</span>}
          </div>
          <p className="profile-coverage">{count(analytics.summary.geolocated_events)} records with coordinates
            {' · '}{coverage?.completed ?? 0}/{coverage?.expected ?? 0} source exports in the selected window.
            {' '}Map precision ranges from country centroids to localities.</p>
        </>}
        {query.data ? (
          <div className="profile-lists">
            {([['Prominent locations', query.data.locations], ['Source domains', query.data.sources]] as const)
              .map(([label, rows]) => <div key={label}>
                <h3>{label}</h3>
                {rows.length ? <ol>{rows.map((row) => <li key={row.label}>
                  <span>{row.label}</span><strong>{count(row.count)}</strong>
                </li>)}</ol> : <p>No detail available in this window.</p>}
              </div>)}
          </div>
        ) : <p className="profile-coverage" role={query.error || unavailable ? 'alert' : 'status'}>{query.error || unavailable
          ? 'Country details are unavailable. Refresh to retry.' : 'Loading country details…'}</p>}
      </div>
    </section>
  )
}
