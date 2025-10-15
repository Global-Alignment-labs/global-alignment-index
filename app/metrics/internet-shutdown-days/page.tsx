'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { TooltipProps } from 'recharts'

const DATA_PATH = '/data/internet_shutdown_days.json'
const fetchVersion = process.env.NEXT_PUBLIC_COMMIT_SHA ?? Date.now().toString()

type Point = { year: number; value: number }

type State = {
  status: 'idle' | 'loading' | 'ready' | 'error'
  series: Point[]
  error?: string
}

const INITIAL_STATE: State = { status: 'idle', series: [] }

export default function InternetShutdownDaysPage() {
  const [state, setState] = useState<State>(INITIAL_STATE)

  useEffect(() => {
    let cancelled = false
    const run = async () => {
      setState(prev => ({ ...prev, status: 'loading', error: undefined }))
      try {
        const res = await fetch(`${DATA_PATH}?v=${fetchVersion}`, { cache: 'no-store' })
        if (!res.ok) {
          if (!cancelled) {
            setState({ status: 'error', series: [], error: 'Data not yet baked for this branch.' })
          }
          return
        }
        const raw = (await res.json()) as Point[]
        const cleaned = Array.isArray(raw)
          ? raw
              .map(p => ({ year: Number(p.year), value: Number(p.value) }))
              .filter(p => Number.isFinite(p.year) && Number.isFinite(p.value))
              .sort((a, b) => a.year - b.year)
          : []
        if (!cleaned.length) {
          if (!cancelled) {
            setState({ status: 'error', series: [], error: 'Data not yet baked for this branch.' })
          }
          return
        }
        if (!cancelled) {
          setState({ status: 'ready', series: cleaned })
        }
      } catch (error) {
        if (!cancelled) {
          setState({ status: 'error', series: [], error: 'Data not yet baked for this branch.' })
        }
      }
    }
    run()
    return () => {
      cancelled = true
    }
  }, [])

  const { status, series, error } = state
  const latest = series.at(-1)
  const earliest = series[0]

  return (
    <main className="mx-auto max-w-4xl p-6 space-y-6">
      <Link href="/" className="inline-flex items-center text-sm text-blue-600 hover:underline">
        <span aria-hidden className="mr-1">
          ←
        </span>
        Back to dashboard
      </Link>

      <header className="space-y-3">
        <div className="space-y-2">
          <p className="text-sm uppercase tracking-wide text-emerald-600">Truth &amp; Clarity</p>
          <h1 className="text-3xl font-semibold tracking-tight">Internet shutdown days</h1>
          <p className="text-base text-slate-600">
            Population-weighted average shutdown days per year. Series spans{' '}
            {earliest && latest ? `${earliest.year}–${latest.year}` : '2016 onward'}.
          </p>
        </div>
        <div className="flex flex-wrap gap-3 text-sm">
          <span className="rounded-full bg-rose-100 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-rose-700">
            Up = worse
          </span>
          <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold uppercase tracking-wide text-slate-700">
            Unit: days (1 decimal)
          </span>
        </div>
      </header>

      {status === 'loading' ? (
        <p className="rounded border border-dashed border-slate-300 bg-slate-50 p-4 text-sm text-slate-600">
          Loading internet shutdown days…
        </p>
      ) : null}

      {status === 'error' && error ? (
        <p className="rounded border border-dashed border-amber-400 bg-amber-50 p-4 text-sm text-amber-700">
          {error}
        </p>
      ) : null}

      {status === 'ready' && series.length ? (
        <section className="card p-4 shadow-sm space-y-4">
          <div className="w-full h-80">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={series}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="year" />
                <YAxis tickFormatter={v => `${Number(v).toFixed(1)}`} />
                <Tooltip content={renderTooltipContent} />
                <Line type="monotone" dataKey="value" stroke="#c026d3" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
          {latest ? (
            <p className="text-sm text-slate-600">
              Latest complete year ({latest.year}): {latest.value.toFixed(1)} days. Lower is better; includes all Access Now STOP
              shutdown scopes (national, regional, platform, mobile).
            </p>
          ) : null}
        </section>
      ) : null}
    </main>
  )
}

function renderTooltipContent({ active, payload, label }: TooltipProps<number, string>) {
  if (!active || !payload?.length) return null
  const value = payload[0]?.value
  if (typeof value !== 'number') return null
  return (
    <div className="rounded bg-white px-3 py-2 text-sm font-medium text-slate-900 shadow">
      {`${label} — ${value.toFixed(1)} days`}
    </div>
  )
}
