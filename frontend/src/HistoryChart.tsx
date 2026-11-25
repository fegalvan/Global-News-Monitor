import { CartesianGrid, ComposedChart, Line, Bar, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { History } from './api'
import { count } from './format'

export default function HistoryChart({ daily }: { daily: History['daily'] }) {
  const data = daily.map((day) => ({ ...day, date: day.start.slice(5, 10) }))
  return (
    <figure className="history-chart">
      <figcaption>Daily reporting · Bars show complete UTC days. The line shows the seven-day average.</figcaption>
      <div>
        <ResponsiveContainer width="100%" height={175}>
          <ComposedChart data={data} margin={{ top: 8, right: 10, bottom: 4, left: 4 }}>
            <CartesianGrid stroke="#273647" vertical={false} />
            <XAxis dataKey="date" minTickGap={22} tick={{ fill: '#aabdd0', fontSize: 10 }} />
            <YAxis width={49} tick={{ fill: '#aabdd0', fontSize: 10 }} allowDecimals={false} />
            <Tooltip contentStyle={{ backgroundColor: '#172433', borderColor: '#40556b' }}
              labelStyle={{ color: '#e3edf6' }} formatter={(value) => count(Number(value))} />
            <Bar dataKey="count" name="Event records" fill="#5fa9a9" minPointSize={2} isAnimationActive={false} />
            <Line dataKey="rolling_average" name="Seven-day average" stroke="#e8bc83"
              strokeWidth={2} dot={false} connectNulls={false} isAnimationActive={false} />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </figure>
  )
}
