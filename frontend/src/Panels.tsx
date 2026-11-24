import type { ReactNode } from 'react'
import { ExternalLink } from 'lucide-react'
import type { Analytics, EventPage, Spike, Tension } from './api'
import { capitalize, categoryColors, count, sourceLink, utc } from './format'

export function PanelHeading({ title, subtitle, children }: { title: string; subtitle?: string; children?: ReactNode }) {
  return (
    <div className="panel-heading">
      <div><h2>{title}</h2>{subtitle && <p>{subtitle}</p>}</div>
      {children}
    </div>
  )
}

function Empty({ children }: { children: ReactNode }) {
  return <p className="panel-empty">{children}</p>
}

export function CountryChart({ analytics, country, onCountry, loading }: {
  analytics?: Analytics; country: string; onCountry: (code: string) => void; loading: boolean
}) {
  const ranks = analytics?.country_counts.slice(0, 5) ?? []
  const maximum = ranks[0]?.count || 1
  return (
    <section className="panel ranking-panel">
      <PanelHeading title="Top countries" subtitle="By event count" />
      {ranks.length ? (
        <ol className="country-chart">
          {ranks.map((row) => (
            <li key={row.country_code ?? 'unknown'}>
              <button type="button" className="country-bar" disabled={!row.country_code}
                onClick={() => row.country_code && onCountry(row.country_code)}
                aria-pressed={Boolean(country && country === row.country_code)}>
                <span className="country-code">{row.country_code ?? '—'}</span>
                <span className="country-name" title={row.country_name}>{row.country_name}</span>
                <span className="rank-track"><i style={{ width: `${100 * row.count / maximum}%` }} /></span>
                <span className="rank-count">{count(row.count)}</span>
              </button>
            </li>
          ))}
        </ol>
      ) : <Empty>{loading ? 'Loading country counts…' : analytics ? 'No country records in this selection.' : 'Country counts unavailable.'}</Empty>}
    </section>
  )
}

export function CategoryChart({ analytics, category, onCategory, loading }: {
  analytics?: Analytics; category: string; onCategory: (code: string) => void; loading: boolean
}) {
  const rows = analytics?.category_counts ?? []
  const total = analytics?.summary.total_events || 1
  return (
    <section className="panel category-panel">
      <PanelHeading title="Category distribution" subtitle="Share of events in the selection" />
      {rows.length ? (
        <>
          <div className="category-strip" aria-hidden="true">
            {rows.map(({ primary_category: label, count: value }) => (
              <span key={label} style={{ flexGrow: value, background: categoryColors[label] }} />
            ))}
          </div>
          <ul className="category-chart">
            {rows.map(({ primary_category: label, count: value }) => (
              <li key={label}>
                <button type="button" aria-pressed={category === label} onClick={() => onCategory(label)}>
                  <i style={{ background: categoryColors[label] }} />{capitalize(label)}
                  <span>{count(value)}</span><span>{(100 * value / total).toFixed(1)}%</span>
                </button>
              </li>
            ))}
          </ul>
        </>
      ) : <Empty>{loading ? 'Loading category counts…' : analytics ? 'No categories in this selection.' : 'Category counts unavailable.'}</Empty>}
    </section>
  )
}

export function EventFeed({ page, loading, loadingPage, pageError, loadMore }: {
  page?: EventPage; loading: boolean; loadingPage: boolean; pageError: boolean; loadMore: () => void
}) {
  return (
    <section className="panel events-panel" id="events">
      <PanelHeading title="Recent events" subtitle="GDELT records and their source references">
        {page && <span className="panel-meta">{count(page.rows.length)} loaded</span>}
      </PanelHeading>
      {page?.rows.length ? (
        <>
          <table className="event-table">
            <thead><tr><th scope="col">Event / CAMEO</th><th scope="col">Location</th><th scope="col">Actors</th>
              <th scope="col">Category</th><th scope="col">Record time (UTC)</th></tr></thead>
            <tbody>
              {page.rows.map((event) => {
                const source = sourceLink(event.source_url)
                const code = event.original_event_code || event.event_code
                return (
                  <tr key={event.event_id}>
                    <td data-label="Event">
                      <strong>{event.event_label}</strong>
                      <div className="event-reference">
                        {code && <span className="mono">{code}</span>}
                        {source && <a href={source} target="_blank" rel="noreferrer">Source <ExternalLink size={12} /></a>}
                      </div>
                    </td>
                    <td data-label="Location">
                      <strong>{event.country_name}</strong>
                      {event.location_name && <span className="row-detail">{event.location_name}</span>}
                    </td>
                    <td data-label="Actors">{[event.actor1_name, event.actor2_name].filter(Boolean).join(' / ') || '—'}</td>
                    <td data-label="Category"><span className="event-category">
                      <i style={{ background: categoryColors[event.primary_category] }} />{capitalize(event.primary_category)}
                    </span></td>
                    <td data-label="Record time"><time dateTime={event.event_time_utc}>{utc(event.event_time_utc)}</time>
                      {event.event_date && <span className="row-detail">Reported date {event.event_date}</span>}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          <div className="table-footer">
            {pageError && <span role="alert">Could not load the next page. Try again.</span>}
            {page.next_cursor
              ? <button type="button" className="outline-button" disabled={loading || loadingPage} onClick={loadMore}>
                {loadingPage ? 'Loading events…' : 'Load more events'}
              </button>
              : <span>All matching events loaded</span>}
          </div>
        </>
      ) : <Empty>{loading ? 'Loading events…' : page ? 'No events match these filters.' : 'Event feed unavailable.'}</Empty>}
    </section>
  )
}

export function Indicators({ spikes, tension, loading, hours }: {
  spikes?: { rows: Spike[]; unavailable: boolean }
  tension?: { rows: Tension[]; unavailable: boolean }
  loading: boolean; hours: number
}) {
  return (
    <div className="indicator-grid" id="indicators">
      <section className="panel">
        <PanelHeading title="Activity spikes" subtitle="Recent volume compared with recorded baseline days" />
        {spikes?.rows.length ? (
          <div className="indicator-scroll"><table>
            <thead><tr><th scope="col">Country / category</th><th scope="col">Events</th><th scope="col">Baseline</th>
              <th scope="col">Lift</th><th scope="col">Z-score</th></tr></thead>
            <tbody>{spikes.rows.map((row) => <tr key={`${row.country_name}:${row.category}`}>
              <td>{row.country_name}<span className="row-detail">{capitalize(row.category)}</span></td>
              <td>{count(row.recent_count)}</td><td>{row.baseline_avg.toFixed(1)}</td>
              <td>{row.lift_ratio.toFixed(2)}×</td><td>{row.z_score?.toFixed(2) ?? '—'}</td>
            </tr>)}</tbody>
          </table></div>
        ) : <Empty>{loading && !spikes ? 'Loading spike candidates…' : !spikes || spikes.unavailable
          ? 'Spike indicator unavailable.' : hours >= 360 ? 'This window leaves no days in the 15-day baseline.'
            : 'No candidates. Spikes require at least 10 events and a matching historical baseline.'}</Empty>}
        <p className="indicator-note">Sparse coverage can skew the baseline.
          {hours !== 24 && hours < 360 && ' Use a 24-hour window to compare equal durations.'}</p>
      </section>
      <section className="panel">
        <PanelHeading title="Tension indicator" subtitle="Actor groups by Goldstein scale · events scoring −5 or below" />
        {tension?.rows.length ? (
          <div className="indicator-scroll"><table>
            <thead><tr><th scope="col">Actors / category</th><th scope="col">Events</th>
              <th scope="col">Avg.</th><th scope="col">Min.</th></tr></thead>
            <tbody>{tension.rows.map((row) => <tr key={JSON.stringify([row.actor1, row.actor2, row.category])}>
              <td>{row.actor1} / {row.actor2}<span className="row-detail">{capitalize(row.category)}</span></td>
              <td>{count(row.event_count)}</td><td>{row.avg_tone.toFixed(2)}</td><td>{row.worst_tone.toFixed(2)}</td>
            </tr>)}</tbody>
          </table></div>
        ) : <Empty>{loading && !tension ? 'Loading actor groups…' : !tension || tension.unavailable
          ? 'Tension indicator unavailable.' : 'No actor groups with three qualifying events in this selection.'}</Empty>}
        <p className="indicator-note">A pattern in coded media coverage. Interpret it in the context of source coverage.</p>
      </section>
    </div>
  )
}
