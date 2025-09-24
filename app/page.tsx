'use client'
import { useEffect, useMemo, useState } from 'react'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'
import { METRICS } from '@/lib/metrics'
import { computeRelative } from '@/lib/relative'
import SourcesFooter from '@/components/SourcesFooter'

const DISPLAY_METRICS = METRICS.filter(m => m.id !== 'internet_use')

type Pt = { year: number; value: number }
const fetchVersion = process.env.NEXT_PUBLIC_COMMIT_SHA ?? Date.now().toString()
function unitFor(id: string): string {
  switch (id) {
    case 'internet_use':
      return '%'
    case 'co2_ppm':
      return 'ppm'
    case 'life_expectancy':
      return 'years'
    default:
      return ''
  }
}

function formatValue(id: string, v: number): string {
  const u = unitFor(id)
  if (id === 'internet_use') return `${Math.round(v)}%`
  if (u) return `${Number(v.toFixed(2))} ${u}`
  return String(Number(v.toFixed(2)))
}
async function load(id: string): Promise<Pt[]> {
  const url = `/data/${id}.json?v=${fetchVersion}`
  const res = await fetch(url, { cache: 'no-store' })
  if (!res.ok) return []
  return res.json()
}

type PtMaybe = { year: number; value: number | null | undefined }

function prepSeriesForPlot(series: PtMaybe[]): PtMaybe[] {
  return [...series]
    .sort((a, b) => a.year - b.year)
    .map(p => {
      const v = p?.value as number | null | undefined
      return Number.isFinite(v) && (v as number) > 0
        ? { year: p.year, value: v as number }
        : { year: p.year, value: null }
    })
}

function getLatestNonMissingPoint(series: PtMaybe[]): PtMaybe | null {
  for (let i = series.length - 1; i >= 0; i--) {
    const v = series[i]?.value
    if (Number.isFinite(v) && (v as number) > 0) return series[i]
  }
  return null
}

export default function Home() {
  const [data, setData] = useState<Record<string, Pt[]>>({})
  const [registry, setRegistry] = useState<Record<string, any>>({})
  useEffect(() => {
    DISPLAY_METRICS.forEach(async m => {
      const series = await load(m.id)
      setData(prev => ({ ...prev, [m.id]: series }))
    })
    ;(async () => {
      const res = await fetch('/data/metrics_registry.json', { cache: 'no-store' })
      if (res.ok) {
        const arr = await res.json()
        setRegistry(Object.fromEntries(arr.map((r: any) => [r.id, r])))
      }
    })()
  }, [])

  const aggregate = useMemo(() => {
    const keys = Object.keys(data)
    if (!keys.length) return []
    const years = Array.from(new Set(keys.flatMap(k => data[k].map(p => p.year)))).sort()
    const z = (arr:number[]) => {
      const mean = arr.reduce((a,b)=>a+b,0)/arr.length
      const sd = Math.sqrt(arr.reduce((a,b)=>a+(b-mean)**2,0)/arr.length) || 1
      return arr.map(v => (v-mean)/sd)
    }
    const seriesZ: Record<string, Map<number, number>> = {}
    keys.forEach(k => {
      const vals = data[k].map(p => p.value)
      const y = data[k].map(p => p.year)
      const zs = z(vals)
      seriesZ[k] = new Map(y.map((yy, i) => [yy, zs[i]]))
    })
    return years.map(yy => {
      const zs = keys.map(k => seriesZ[k].get(yy)).filter(v => v!==undefined) as number[]
      if (!zs.length) return null
      return { year: yy, value: Number((zs.reduce((a,b)=>a+b,0)/zs.length).toFixed(2)) }
    }).filter(Boolean) as Pt[]
  }, [data])

  return (
    <main className="mx-auto max-w-6xl p-6 space-y-8">
        <header className="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
          <div>
            <h1 className="text-3xl font-semibold tracking-tight">Global Alignment Index <span className="text-sm opacity-70">v0.1</span></h1>
            <p className="opacity-70">Factual signals; no opinions. A simple aggregate line + domain charts (mock data for now).</p>
          </div>
        </header>

      <section className="card p-4 shadow-sm">
        <h2 className="text-xl mb-2">Whole Alignment Trend (aggregate, mock)</h2>
        <div className="w-full h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={aggregate}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="year" />
              <YAxis label={{ value: 'z-score (std units)', angle: -90, position: 'insideLeft' }} />
              <Tooltip />
              <Line type="monotone" dataKey="value" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <p className="text-sm opacity-70 mt-2">Method: per-year z‑score average of available series (placeholder).</p>
      </section>

      <section className="grid md:grid-cols-2 gap-6">
        {DISPLAY_METRICS.map(m => {
          const k = m.id
          const raw = data[k] || []
          return (
            <div key={k} className="card p-4 shadow-sm">
              {(() => {
                const base = m.name ?? k
                const titled = k === 'u5_mortality' ? `${base} (per 1,000 live births)` : base
                return <h3 className="text-lg font-medium">{titled}</h3>
              })()}
              <div className="w-full h-56 mt-2">
                <ResponsiveContainer width="100%" height="100%">
                  {(() => {
                    const series = prepSeriesForPlot(raw).map(p => ({ year: p.year, [k]: p.value }))
                    const metricIds = Object.keys(series[0] ?? {}).filter(id => id !== 'year')
                    const mid = metricIds.length === 1 ? metricIds[0] : undefined
                    return (
                      <LineChart data={series}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="year" />
                        {mid === 'internet_use' ? (
                          <YAxis domain={[0,100]} tickFormatter={v => formatValue(mid, v)} />
                        ) : mid ? (
                          <YAxis tickFormatter={v => formatValue(mid, v)} />
                        ) : (
                          <YAxis />
                        )}
                        {mid ? (
                          <Tooltip formatter={(val:any) => formatValue(mid, Number(val))} />
                        ) : (
                          <Tooltip />
                        )}
                        {mid ? (
                          <Line type="monotone" dataKey={mid} dot={false} />
                        ) : (
                          metricIds.map(id => (
                            <Line key={id} type="monotone" dataKey={id} dot={false} />
                          ))
                        )}
                      </LineChart>
                    )
                  })()}
                </ResponsiveContainer>
              </div>
              <p className="text-sm opacity-70 mt-2">{m.domain}</p>
              {(() => {
                const reg = registry[k]
                const latest = getLatestNonMissingPoint(raw)
                const latestValue = latest?.value
                const latestYear = latest?.year
                const canShowLatest = Number.isFinite(latestValue)
                if (reg && canShowLatest && latestYear !== undefined) {
                  const rel = computeRelative(latestValue as number, {
                    direction: reg.direction,
                    reference_min: reg.reference_min,
                    reference_max: reg.reference_max,
                    target: reg.target,
                  })
                  return (
                    <p className="text-sm mt-1">
                      Latest ({latestYear}): {formatValue(k, latestValue as number)} · Relative: {Math.round(rel)}%
                    </p>
                  )
                }
                if (reg && !canShowLatest) {
                  return <p className="text-sm mt-1">Latest: No recent data</p>
                }
                return null
              })()}
            </div>
          )
        })}
      </section>

      <footer className="text-sm opacity-70 py-10">
        <p>Sources: NOAA, WHO, World Bank, ITU (real datasets to be wired). This is a v0.1 prototype.</p>
      </footer>
      <SourcesFooter />
    </main>
  )
}
