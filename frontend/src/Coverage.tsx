import { useEffect, useRef, useState } from 'react'
import { ExternalLink } from 'lucide-react'
import { queryParams, type Filters, type SourceCoverage } from './api'
import { capitalize, count, sourceLink, utc } from './format'
import { PanelHeading } from './Panels'
import { useResource } from './useDashboard'

export function Coverage({ filters, scope, refresh, unavailable }: {
  filters: Filters | null; scope: string; refresh: number; unavailable: boolean
}) {
  const section = useRef<HTMLElement>(null)
  const [visible, setVisible] = useState(false)
  const [retry, setRetry] = useState(0)
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
  const path = basePath && domain ? `${basePath}&${new URLSearchParams({ domain })}` : basePath
  const query = useResource<SourceCoverage>(path, refresh + retry, visible)
  const data = query.data
  const previous = query.snapshot
  const summary = data ?? (basePath && (previous?.path === basePath || previous?.path.startsWith(`${basePath}&domain=`))
    ? previous.data : undefined)

  return (
    <section ref={section} className="panel coverage-panel" id="coverage" tabIndex={-1}>
      <PanelHeading title="News coverage" subtitle="Follow the original links GDELT associated with event records in this selection.">
        <a href="#events" className="outline-button">View event records</a>
      </PanelHeading>
      {summary ? (
        <div className="coverage-content">
          <p className="coverage-counts">
            <span><strong>{count(summary.unique_urls)}</strong> distinct article links</span>
            <span><strong>{count(summary.linked_records)}</strong> of {count(summary.inspected_records)} inspected records with links</span>
          </p>
          {summary.truncated && <p className="coverage-note">Showing coverage from the newest {count(summary.inspected_records)} matching records, back to {utc(summary.earliest_inspected)}. Narrow the filters to explore more of the window.</p>}
          {(summary.domains.length > 0 || domain) && (
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
          )}
          {data?.articles.length ? (
            <ul className="coverage-articles">
              {data.articles.map((article) => {
                const href = sourceLink(article.url, article.domain)
                return (
                  <li key={article.url}>
                    <div className="coverage-article-heading">
                      <strong>{article.event_label}</strong>
                      <span className="event-category">{capitalize(article.category)}</span>
                    </div>
                    {href ? <a href={href} target="_blank" rel="noopener noreferrer">
                      {article.domain} <span className="sr-only">(opens in a new tab)</span><ExternalLink size={13} aria-hidden="true" />
                    </a> : <span>{article.domain}</span>}
                    <small>{utc(article.latest_record_time)} · {count(article.record_count)} {article.record_count === 1 ? 'record' : 'records'} in this sample</small>
                  </li>
                )
              })}
            </ul>
          ) : data && <p className="coverage-note">{domain ? 'No links from this domain in the current sample.'
            : 'No usable article links here. Event details and links are available for up to 17 days.'}</p>}
          <details className="coverage-note">
            <summary>How links are counted</summary>
            <p>GNM groups identical stored URLs. Different versions of a URL can point to the same story. One link can appear in several event records. Each row shows the latest matching record’s event label and time. Source counts refer to distinct links in the inspected records.</p>
          </details>
        </div>
      ) : null}
      {query.error || unavailable ? (
        <div className="coverage-content"><p role="alert">News coverage is temporarily unavailable.</p>
          <button type="button" className="outline-button" onClick={() => setRetry((previous) => previous + 1)}>Retry coverage</button></div>
      ) : !data && <p className="panel-empty" role="status">{domain ? `Loading links from ${domain}…` : 'Loading news coverage…'}</p>}
    </section>
  )
}
