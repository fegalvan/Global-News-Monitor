import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { Analytics, Bucket, Window } from './api'
import { activitySeries, count, utc } from './format'
import { PanelHeading } from './Panels'

const hourLabel = new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', timeZone: 'UTC' })
const dateLabel = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' })

export default function VolumeChart({ analytics, window, bucket, loading }: {
  analytics?: Analytics; window: Window; bucket: Bucket; loading: boolean
}) {
  const coverage = analytics?.coverage ?? []
  const rows = activitySeries(analytics?.series ?? [], window, bucket, coverage)
  const completed = coverage.reduce((total, row) => total + row.completed_exports, 0)
  const expected = coverage.reduce((total, row) => total + row.expected_exports, 0)
  return (
    <section className="panel volume-panel">
      <PanelHeading title="Activity over time" subtitle={`Events per ${bucket} · UTC`} />
      {rows.some((row) => row.count !== null) ? (
        <div className="volume-chart" aria-label="Chart of events by UTC time bucket">
          <ResponsiveContainer width="100%" height="100%" minWidth={0}>
            <AreaChart data={rows} margin={{ left: 0, right: 14, top: 8, bottom: 0 }}>
              <CartesianGrid vertical={false} stroke="#283240" strokeDasharray="3 4" />
              <XAxis dataKey="time" tickFormatter={(time) => (bucket === 'hour' ? hourLabel : dateLabel).format(time)}
                minTickGap={24} tickLine={false} axisLine={false} tick={{ fill: '#9eabba', fontSize: 11 }} />
              <YAxis width={44} allowDecimals={false} tickCount={4} tickFormatter={count}
                axisLine={false} tickLine={false} tick={{ fill: '#9eabba', fontSize: 11 }} />
              <Tooltip labelFormatter={(time) => utc(new Date(Number(time)).toISOString())}
                formatter={(value) => [count(Number(value)), 'Events']}
                contentStyle={{ background: '#141f2b', color: '#eef3f9', border: '1px solid #405061', borderRadius: 4 }}
                itemStyle={{ color: '#7bd2c6' }} />
              <Area type="linear" dataKey="count" stroke="#73d3c3" strokeWidth={2}
                fill="#4ab9a1" fillOpacity={0.17} isAnimationActive={false}
                dot={{ r: 2.5, fill: '#73d3c3', strokeWidth: 0 }} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      ) : <p className="panel-empty">{loading ? 'Loading activity…' : analytics ? 'No activity observations in the selected window.' : 'Activity unavailable.'}</p>}
      {expected > 0 && <p className="coverage-note">{count(completed)} of {count(expected)} expected exports processed
        {completed < expected && ' · Gaps show incomplete coverage'}</p>}
    </section>
  )
}
