import { useEffect, useRef, useState } from 'react'
import {
  getEvents, queryParams, request,
  type Analytics, type Bucket, type Country, type EventPage, type Filters, type Freshness,
} from './api'

const POLL_INTERVAL = 60_000
const NO_COUNTRIES: Country[] = []

export function useFreshness(refresh: number) {
  const [state, setState] = useState({ data: null as Freshness | null, checked: false, error: false })

  useEffect(() => {
    const controller = new AbortController()
    let checking = false
    let lastCheck = 0

    async function check() {
      if (checking || document.hidden) return
      checking = true
      lastCheck = Date.now()
      try {
        const data = await request<Freshness>('/freshness', controller.signal)
        if (!controller.signal.aborted) setState({ data, checked: true, error: false })
      } catch {
        if (!controller.signal.aborted) setState((current) => ({ ...current, checked: true, error: true }))
      } finally {
        checking = false
      }
    }

    function onVisibility() {
      if (Date.now() - lastCheck >= POLL_INTERVAL) void check()
    }

    void check()
    const timer = window.setInterval(check, POLL_INTERVAL)
    document.addEventListener('visibilitychange', onVisibility)
    return () => {
      controller.abort()
      window.clearInterval(timer)
      document.removeEventListener('visibilitychange', onVisibility)
    }
  }, [refresh])

  return state
}

type Snapshot = {
  key: string; scope: string; filters: Filters; bucket: Bucket
  analytics: Analytics; events: EventPage; refreshedAt: string
}

export function useResource<T>(path: string | null, refresh = 0, enabled = true) {
  const [snapshot, setSnapshot] = useState<{ path: string; refresh: number; data?: T; error: boolean }>()
  useEffect(() => {
    if (!path || !enabled) return
    const controller = new AbortController()
    request<T>(path, controller.signal).then(
      (data) => { if (!controller.signal.aborted) setSnapshot({ path, refresh, data, error: false }) },
      () => { if (!controller.signal.aborted) setSnapshot({ path, refresh, error: true }) },
    )
    return () => controller.abort()
  }, [path, refresh, enabled])
  const active = snapshot && snapshot.path === path && snapshot.refresh === refresh ? snapshot : undefined
  return { data: active?.data, error: active?.error ?? false, loading: Boolean(path && !active), snapshot }
}

export function useDashboard(filters: Filters, bucket: Bucket, scope: string, enabled: boolean, refresh: number) {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null)
  const [failedKey, setFailedKey] = useState<string | null>(null)
  const [pageFailedKey, setPageFailedKey] = useState<string | null>(null)
  const [loadingPage, setLoadingPage] = useState(false)
  const pageRequest = useRef<AbortController | null>(null)
  const countries = useResource<{ rows: Country[] }>('/countries', refresh).data?.rows ?? NO_COUNTRIES
  const { start, end, country, category } = filters
  const key = JSON.stringify([start, end, country, category, bucket, refresh])

  useEffect(() => {
    if (!enabled) return
    const controller = new AbortController()
    const currentFilters = { start, end, country, category }
    const analyticsQuery = queryParams(currentFilters, { bucket, include_map: 'false' })

    const analytics = request<Analytics>(`/analytics?${analyticsQuery}`, controller.signal)
    Promise.all([analytics, getEvents(currentFilters, controller.signal)])
      .then(([summary, events]) => {
        if (controller.signal.aborted) return
        setFailedKey(null)
        setPageFailedKey(null)
        setSnapshot({
          key, scope, filters: currentFilters, bucket, analytics: summary, events,
          refreshedAt: new Date().toISOString(),
        })
      })
      .catch(() => {
        if (!controller.signal.aborted) setFailedKey(key)
        controller.abort()
      })

    return () => {
      controller.abort()
      pageRequest.current?.abort()
      pageRequest.current = null
    }
  }, [start, end, country, category, bucket, enabled, key, scope, refresh])

  const data = snapshot?.scope === scope ? snapshot : null
  const error = failedKey === key
  const loading = !error && data?.key !== key

  async function loadMore() {
    if (!data?.events.next_cursor || data.key !== key || pageRequest.current) return
    const controller = new AbortController()
    pageRequest.current = controller
    setLoadingPage(true)
    setPageFailedKey(null)
    try {
      const page = await getEvents(data.filters, controller.signal, data.events.next_cursor)
      if (controller.signal.aborted) return
      setSnapshot((current) => current?.key === key ? {
        ...current,
        events: { rows: [...current.events.rows, ...page.rows], next_cursor: page.next_cursor },
      } : current)
    } catch {
      if (!controller.signal.aborted) setPageFailedKey(key)
    } finally {
      if (pageRequest.current === controller) pageRequest.current = null
      setLoadingPage(false)
    }
  }

  return {
    data, loading, error, loadMore, loadingPage, pageError: pageFailedKey === key,
    countries,
  }
}
