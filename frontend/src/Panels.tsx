import type { ReactNode } from 'react'
import { ExternalLink } from 'lucide-react'
import type { Analytics, EventPage } from './api'
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
      <PanelHeading title="Most reported countries" subtitle="Select a country to filter the dashboard" />
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
      <PanelHeading title="What was reported" subtitle="Share of event records by category. Select one to explore." />
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
    <section className="panel events-panel" id="events" tabIndex={-1}>
      <PanelHeading title="Event records" subtitle="Individual GDELT entries, newest first. Several entries can link to the same article.">
        {page && <span className="panel-meta">{count(page.rows.length)} loaded</span>}
      </PanelHeading>
      {page?.rows.length ? (
        <>
          <table className="event-table">
            <thead><tr><th scope="col">Event classification</th><th scope="col">Reported location</th><th scope="col">People or groups</th>
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
                        {code && <span className="mono" title="GDELT event classification code">{code}</span>}
                        {source && <a href={source} target="_blank" rel="noopener noreferrer">{new URL(source).hostname}
                          <span className="sr-only"> (opens in a new tab)</span><ExternalLink size={12} aria-hidden="true" /></a>}
                      </div>
                    </td>
                    <td data-label="Location">
                      <strong>{event.country_name}</strong>
                      {event.location_name && <span className="row-detail">{event.location_name}</span>}
                      {event.geographic_precision && event.geographic_precision !== 'none' && event.geographic_precision !== 'unknown'
                        && <span className="row-detail">Location detail · {event.geographic_precision.replace('_', ' ')}</span>}
                      {event.country_status === 'unmapped' && <span className="row-detail">Unmapped code {event.original_country_code}</span>}
                      {event.country_status === 'missing' && <span className="row-detail">Country absent in GDELT</span>}
                    </td>
                    <td data-label="People or groups">{[event.actor1_name, event.actor2_name].filter(Boolean).join(' / ') || '—'}</td>
                    <td data-label="Category"><span className="event-category">
                      <i style={{ background: categoryColors[event.primary_category] }} />{capitalize(event.primary_category)}
                    </span></td>
                    <td data-label="Record time"><time dateTime={event.event_time_utc}>{utc(event.event_time_utc)}</time>
                      {event.event_date && <span className="row-detail">Event date {event.event_date}</span>}
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
