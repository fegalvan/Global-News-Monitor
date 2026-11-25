import { useEffect, useRef, useState } from 'react'
import { ExternalLink } from 'lucide-react'
import { queryParams, type Filters, type SourceCoverage } from './api'
import { categoryLabel, count, precisionLabels, sourceLink, utc } from './format'
import { PanelHeading } from './Panels'
import { useResource } from './useDashboard'

export function Coverage({ filters, placeName, scope, refresh, unavailable, requested }: {
  filters: Filters | null; placeName: string; scope: string; refresh: number; unavailable: boolean; requested: boolean
}) {
  const section = useRef<HTMLElement>(null)
  const [visible, setVisible] = useState(false)
  const [retry, setRetry] = useState(0)
  const [order, setOrder] = useState<'varied' | 'recent'>('varied')
  const [selection, setSelection] = useState({ scope: '', domain: '' })
  const domain = selection.scope === scope ? selection.domain : ''

  useEffect(() => {
    if (!section.current) return
    const observer = new IntersectionObserver(([entry]) => {
      if (entry.isIntersecting) {
        setVisible(true)
        observer.disconnect()
      }
    }, { rootMargin: '120px' })
    observer.observe(section.current)
    return () => observer.disconnect()
  }, [])

  const basePath = filters ? `/coverage?${queryParams(filters)}` : null
  const path = basePath ? `${basePath}&${new URLSearchParams(domain ? { domain, order: 'recent' } : { order })}` : null
  const query = useResource<SourceCoverage>(path, refresh + retry, visible || requested)
  const data = query.data
  const previous = query.snapshot
  const summary = data ?? (basePath && previous?.path.startsWith(`${basePath}&`)
    ? previous.data : undefined)

  return (
    <section ref={section} className="panel coverage-panel" id="coverage" tabIndex={-1}>
      <PanelHeading title="Original GDELT links"
        subtitle={`${placeName} · ${filters?.category ? categoryLabel(filters.category) : 'All categories'}`}>
        <a href="#events" className="outline-button">View event records</a>
      </PanelHeading>
      {summary ? (
        <div className="coverage-content">
          <p className="coverage-note">These links come from automated GDELT records and can include background mentions.</p>
          <p className="coverage-counts">
            <span><strong>{count(summary.unique_urls)}</strong> distinct article links</span>
            <span><strong>{count(summary.linked_records)}</strong> of {count(summary.inspected_records)} inspected records with links</span>
          </p>
          {summary.unique_urls > 0 && !domain && <label className="coverage-sort">Show links by
            <select aria-label="Order original links" value={order}
              onChange={(event) => setOrder(event.target.value === 'recent' ? 'recent' : 'varied')}>
              <option value="varied">Varied sources</option>
              <option value="recent">Most recent</option>
            </select>
          </label>}
          {summary.truncated && <p className="coverage-note">Showing coverage from the newest {count(summary.inspected_records)} matching records, back to {utc(summary.earliest_inspected)}. Narrow the filters to explore more of the window.</p>}
          {(summary.domains.length > 0 || domain) && (
            <>
              <div className="coverage-sources" role="group" aria-label="Filter recent links by source domain">
                <button type="button" className="outline-button" aria-pressed={!domain}
                  onClick={() => setSelection({ scope, domain: '' })}>All sources</button>
                {summary.domains.map((source) => (
                  <button type="button" className="outline-button" key={source.domain} aria-pressed={domain === source.domain}
                    onClick={() => setSelection({ scope, domain: source.domain })}>
                    {source.domain} <span>{count(source.articles)}</span>
                  </button>
                ))}
              </div>
              <label className="coverage-source-select">Source
                <select aria-label="Source domain" value={domain} onChange={(event) => setSelection({ scope, domain: event.target.value })}>
                  <option value="">All sources</option>
                  {domain && !summary.domains.some((source) => source.domain === domain) && <option value={domain}>{domain}</option>}
                  {summary.domains.map((source) => <option key={source.domain} value={source.domain}>
                    {source.domain} ({count(source.articles)} links)
                  </option>)}
                </select>
              </label>
            </>
          )}
          {data?.articles.length ? (
            <ul className="coverage-articles">
              {data.articles.map((article) => {
                const href = sourceLink(article.url, article.domain)
                return (
                  <li key={article.url}>
                    <div className="coverage-article-heading">
                      {href ? <a href={href} target="_blank" rel="noopener noreferrer">
                        {article.domain} <span className="sr-only">(opens in a new tab)</span><ExternalLink size={13} aria-hidden="true" />
                      </a> : <strong>{article.domain}</strong>}
                      <span className="event-category">{categoryLabel(article.category)}</span>
                    </div>
                    <span className="coverage-classification">GDELT coded {article.event_label}</span>
                    <small>{utc(article.latest_record_time)} · {count(article.record_count)} {article.record_count === 1 ? 'record' : 'records'} in this sample</small>
                    <details className="coverage-record-details">
                      <summary>Record details</summary>
                      <small>Location in record: {article.location_name || article.country_name} · {precisionLabels[article.geographic_precision]}</small>
                      <small>Named in record: {[article.actor1_name, article.actor2_name].filter(Boolean).join(' / ') || 'Unavailable'}</small>
                      {article.gdelt_id && <small>GDELT ID {article.gdelt_id}{article.event_code && ` · CAMEO ${article.event_code}`}</small>}
                    </details>
                  </li>
                )
              })}
            </ul>
          ) : data && <p className="coverage-note">{domain ? 'No links from this domain in the current sample.'
            : 'No usable article links here. Event details and links are available for up to 17 days.'}</p>}
          <details className="coverage-note">
            <summary>How links are counted</summary>
            <p>GNM groups identical stored URLs and shows the most recent matching record for each one. Varied sources brings more publishers into view and favors recent records with more detail. Most recent shows links in time order. Both views use the same records and counts.</p>
          </details>
        </div>
      ) : null}
      {query.error || unavailable ? (
        <div className="coverage-content"><p role="alert">News coverage is temporarily unavailable.</p>
          {query.error && <button type="button" className="outline-button"
            onClick={() => setRetry((previous) => previous + 1)}>Retry coverage</button>}</div>
      ) : !data && <p className="panel-empty" role="status">{domain ? `Loading links from ${domain}…` : 'Loading news coverage…'}</p>}
    </section>
  )
}
