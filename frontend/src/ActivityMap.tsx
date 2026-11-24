import { useEffect, useRef, useState } from 'react'
import L from 'leaflet'
import { feature } from 'topojson-client'
import type { GeometryCollection, Topology } from 'topojson-specification'
import type { FeatureCollection, MultiPolygon, Polygon } from 'geojson'
import world from 'world-atlas/countries-110m.json'
import { Maximize } from 'lucide-react'
import { queryParams, type Country, type Filters, type MapData } from './api'
import { useResource } from './useDashboard'
import { count } from './format'
import { cellOrigin, mapPolygon } from './mapGeometry'
import 'leaflet/dist/leaflet.css'

const topology = world as unknown as Topology<{ countries: GeometryCollection<{ name: string }> }>
const sourceBoundaries = feature(topology, topology.objects.countries) as FeatureCollection<Polygon | MultiPolygon, { name: string }>
const boundaries: FeatureCollection<MultiPolygon, { name: string }> = {
  type: 'FeatureCollection',
  features: sourceBoundaries.features.filter(({ properties }) => properties.name !== 'Antarctica').map((country) => {
    const geometry = country.geometry
    const polygons = geometry.type === 'Polygon' ? [geometry.coordinates] : geometry.coordinates
    return { ...country, geometry: { type: 'MultiPolygon', coordinates: polygons.flatMap(mapPolygon) } }
  }),
}
const worldBounds = L.latLngBounds([-90, -180], [90, 180])
const bands = [
  { max: 1, color: '#427899', opacity: 0.55, label: '1' },
  { max: 9, color: '#55aacc', opacity: 0.64, label: '2–9' },
  { max: 49, color: '#69d4d1', opacity: 0.73, label: '10–49' },
  { max: 199, color: '#adecc7', opacity: 0.83, label: '50–199' },
  { max: Infinity, color: '#ebf9b8', opacity: 0.94, label: '200+' },
]
const aliases: Record<string, string> = {
  'United States of America': 'United States',
  'Dem. Rep. Congo': 'Democratic Republic of the Congo',
  'Central African Rep.': 'Central African Republic',
  'Dominican Rep.': 'Dominican Republic',
  'S. Sudan': 'South Sudan',
  'Eq. Guinea': 'Equatorial Guinea',
  'Solomon Is.': 'Solomon Islands',
  'Bosnia and Herz.': 'Bosnia and Herzegovina',
  'Czechia': 'Czech Republic',
}

type Props = {
  filters: Filters | null
  countries: Country[]
  activeCountry: string
  onCountry: (code: string) => void
  pending: boolean
  geolocated?: number
  refresh: number
}

function tooltip(text: string) {
  const element = document.createElement('span')
  element.textContent = text
  return element
}

export default function ActivityMap({ filters, countries, activeCountry, onCountry, pending, geolocated, refresh }: Props) {
  const container = useRef<HTMLDivElement>(null)
  const map = useRef<L.Map | null>(null)
  const geography = useRef<L.GeoJSON | null>(null)
  const worldView = useRef(true)
  const [bounds, setBounds] = useState({ south: -90, west: -180, north: 90, east: 180 })
  const viewport = useResource<MapData>(filters ? `/map?${queryParams(filters, bounds)}` : null, refresh)
  const data = viewport.data

  useEffect(() => {
    if (!container.current) return
    const instance = L.map(container.current, {
      crs: L.CRS.EPSG4326,
      zoomSnap: 0.1, zoomDelta: 0.6, minZoom: -2, maxZoom: 7,
      scrollWheelZoom: false, doubleClickZoom: false,
      maxBounds: [[-90, -215], [90, 215]], maxBoundsViscosity: 0.75,
      zoomControl: false,
    })
    map.current = instance
    instance.fitBounds(worldBounds, { padding: [12, 10], animate: false })
    instance.on('dragstart boxzoomstart', () => { worldView.current = false })
    let moveTimeout: number | undefined
    instance.on('moveend', () => {
      window.clearTimeout(moveTimeout)
      moveTimeout = window.setTimeout(() => {
        const view = worldView.current ? worldBounds : instance.getBounds()
        const next = {
          south: Math.max(-90, Math.floor(view.getSouth() * 100) / 100),
          west: Math.max(-180, Math.floor(view.getWest() * 100) / 100),
          north: Math.min(90, Math.ceil(view.getNorth() * 100) / 100),
          east: Math.min(180, Math.ceil(view.getEast() * 100) / 100),
        }
        if (next.south < next.north && next.west < next.east) setBounds(next)
      }, 180)
    })
    const graticule = L.layerGroup().addTo(instance)
    const gridStyle = { color: '#263444', opacity: 0.6, weight: 1, dashArray: '2 7', interactive: false }
    for (const lat of [-60, -30, 0, 30, 60]) L.polyline([[lat, -180], [lat, 180]], gridStyle).addTo(graticule)
    for (const lon of [-120, -60, 0, 60, 120]) L.polyline([[-85, lon], [85, lon]], gridStyle).addTo(graticule)

    geography.current = L.geoJSON(boundaries).addTo(instance)
    const pane = instance.createPane('eventCells')
    pane.style.zIndex = '430'
    pane.style.pointerEvents = 'none'
    instance.attributionControl.setPrefix(false)
    instance.attributionControl.addAttribution('Natural Earth')
    const resize = new ResizeObserver(() => {
      instance.invalidateSize({ pan: false })
      if (worldView.current) instance.fitBounds(worldBounds, { padding: [12, 10], animate: false })
    })
    resize.observe(container.current)
    return () => {
      window.clearTimeout(moveTimeout)
      resize.disconnect()
      instance.remove()
      map.current = null
    }
  }, [])

  useEffect(() => {
    const layer = geography.current
    if (!layer) return
    layer.eachLayer((item) => {
      const shape = item as L.GeoJSON & { feature: (typeof boundaries.features)[number] }
      const name = shape.feature.properties.name
      const records = countries.filter((row) => row.country_name === (aliases[name] || name) && row.country_code)
      const selected = records.find((row) => row.country_code === activeCountry)
      const target = selected ?? records[0]
      shape.setStyle({
        color: selected ? '#6dbab3' : '#465b6c',
        fillColor: selected ? '#315d66' : '#233445',
        fillOpacity: 1, weight: selected ? 1.4 : 0.7,
      })
      shape.unbindTooltip()
      shape.off('click mouseover mouseout')
      shape.bindTooltip(tooltip(target ? `${target.country_name} (${target.country_code}) · Click for profile` : name))
      if (target?.country_code) {
        const code = target.country_code
        shape.on('click', () => onCountry(code))
        shape.on('mouseover', () => shape.setStyle({ fillColor: '#365663' }))
        shape.on('mouseout', () => shape.setStyle({ fillColor: selected ? '#315d66' : '#233445' }))
      }
    })
  }, [countries, activeCountry, onCountry])

  useEffect(() => {
    const instance = map.current
    if (!instance || !data?.cells.length) return
    const size = data.cell_degrees
    const pane = instance.getPane('eventCells')!
    const canvas = L.DomUtil.create('canvas', 'leaflet-layer', pane)
    canvas.setAttribute('aria-hidden', 'true')
    const context = canvas.getContext('2d')
    const renderedCells = data.cells
    const cellCounts = new Map(renderedCells.map((cell) => [`${cell.grid_south}:${cell.grid_west}`, cell.count]))
    const hover = L.tooltip({ direction: 'top', offset: [0, -10] })
    let frame = 0

    function draw() {
      if (!instance || !context) return
      const { x: width, y: height } = instance.getSize()
      const ratio = Math.min(devicePixelRatio || 1, 2)
      canvas.width = width * ratio
      canvas.height = height * ratio
      canvas.style.width = `${width}px`
      canvas.style.height = `${height}px`
      L.DomUtil.setPosition(canvas, instance.containerPointToLayerPoint([0, 0]))
      context.scale(ratio, ratio)
      for (const cell of renderedCells) {
        const topLeft = instance.latLngToContainerPoint([
          Math.min(cell.grid_south + size, bounds.north), Math.max(cell.grid_west, bounds.west),
        ])
        const bottomRight = instance.latLngToContainerPoint([
          Math.max(cell.grid_south, bounds.south), Math.min(cell.grid_west + size, bounds.east),
        ])
        if (bottomRight.x < 0 || bottomRight.y < 0 || topLeft.x > width || topLeft.y > height) continue
        const band = bands.find(({ max }) => cell.count <= max)!
        context.fillStyle = band.color
        context.globalAlpha = band.opacity
        const cellWidth = bottomRight.x - topLeft.x
        const cellHeight = bottomRight.y - topLeft.y
        context.fillRect(topLeft.x, topLeft.y, Math.max(1, cellWidth - 0.4), Math.max(1, cellHeight - 0.4))
      }
    }

    function scheduleDraw() {
      cancelAnimationFrame(frame)
      frame = requestAnimationFrame(draw)
    }
    function showCell({ latlng }: L.LeafletMouseEvent) {
      const { lat, lng } = latlng
      const inside = lat >= bounds.south && lat <= bounds.north && lng >= bounds.west && lng <= bounds.east
      const total = inside && cellCounts.get(`${cellOrigin(lat, -90, size)}:${cellOrigin(lng, -180, size)}`)
      if (total && instance) {
        hover.setLatLng(latlng).setContent(tooltip(`${count(total)} reported events · ${size}° grid`)).addTo(instance)
      } else {
        hover.remove()
      }
    }
    function hideCell() { hover.remove() }

    scheduleDraw()
    instance.on('move zoom resize viewreset', scheduleDraw)
    instance.on('mousemove', showCell)
    instance.on('mouseout movestart', hideCell)
    return () => {
      cancelAnimationFrame(frame)
      instance.off('move zoom resize viewreset', scheduleDraw)
      instance.off('mousemove', showCell)
      instance.off('mouseout movestart', hideCell)
      hideCell()
      canvas.remove()
    }
  }, [bounds, data])

  function returnToWorld() {
    worldView.current = true
    map.current?.fitBounds(worldBounds, { padding: [12, 10], animate: false })
  }

  function zoom(direction: 1 | -1) {
    worldView.current = false
    if (direction === 1) map.current?.zoomIn()
    else map.current?.zoomOut()
  }
  return (
    <>
      <div className="map-frame">
        <div className="leaflet-host" ref={container} role="region" aria-label="Interactive world map of GDELT event activity" />
        <div className="map-controls" aria-label="Map controls">
          <button type="button" aria-label="Zoom in" title="Zoom in" onClick={() => zoom(1)}>+</button>
          <button type="button" aria-label="Zoom out" title="Zoom out" onClick={() => zoom(-1)}>−</button>
          <button type="button" aria-label="Reset map view" title="Reset map view" onClick={returnToWorld}><Maximize size={15} /></button>
        </div>
        {!data?.cells.length && (
          <div className="map-message">
            {pending || viewport.loading ? 'Loading mapped activity…' : viewport.error || !filters
              ? 'Mapped activity unavailable' : 'No mapped events in this view'}
          </div>
        )}
      </div>
      <div className="map-footer">
        <div className="map-legend" aria-label="Event count legend">
          <span>Events per cell</span>
          {bands.map(({ color, label }) => <span key={label}><i style={{ background: color }} />{label}</span>)}
        </div>
        <span>{data && `${count(data.viewport_events)} in view${geolocated !== undefined
          ? ` of ${count(geolocated)} geolocated` : ''} · ${data.cell_degrees}° grid`}</span>
      </div>
    </>
  )
}
