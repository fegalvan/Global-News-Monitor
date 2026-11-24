import { useEffect, useRef } from 'react'
import L from 'leaflet'
import { feature } from 'topojson-client'
import type { GeometryCollection, Topology } from 'topojson-specification'
import type { FeatureCollection, MultiPolygon, Polygon } from 'geojson'
import world from 'world-atlas/countries-110m.json'
import { Maximize } from 'lucide-react'
import type { Analytics, Country } from './api'
import { count } from './format'
import { mapPolygon } from './mapGeometry'
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
const worldBounds = L.latLngBounds([-61, -180], [84, 180])
const bands = [
  { max: 1, color: '#447a9c', label: '1' },
  { max: 9, color: '#54a8c3', label: '2–9' },
  { max: 49, color: '#63cbc5', label: '10–49' },
  { max: Infinity, color: '#b3edc6', label: '50+' },
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
  data?: Analytics['map']
  countries: Country[]
  activeCountry: string
  onCountry: (code: string) => void
  pending: boolean
  unavailable: boolean
}

function tooltip(text: string) {
  const element = document.createElement('span')
  element.textContent = text
  return element
}

export default function ActivityMap({ data, countries, activeCountry, onCountry, pending, unavailable }: Props) {
  const container = useRef<HTMLDivElement>(null)
  const map = useRef<L.Map | null>(null)
  const geography = useRef<L.GeoJSON | null>(null)
  const activity = useRef<L.LayerGroup | null>(null)
  const worldView = useRef(true)

  useEffect(() => {
    if (!container.current) return
    const instance = L.map(container.current, {
      crs: L.CRS.EPSG4326,
      zoomSnap: 0.1, zoomDelta: 0.6, minZoom: -1, maxZoom: 7,
      scrollWheelZoom: false, doubleClickZoom: false,
      maxBounds: [[-90, -215], [90, 215]], maxBoundsViscosity: 0.75,
      zoomControl: false,
    })
    map.current = instance
    instance.fitBounds(worldBounds, { padding: [12, 10], animate: false })
    instance.on('dragstart boxzoomstart', () => { worldView.current = false })
    const graticule = L.layerGroup().addTo(instance)
    const gridStyle = { color: '#263444', opacity: 0.6, weight: 1, dashArray: '2 7', interactive: false }
    for (const lat of [-60, -30, 0, 30, 60]) L.polyline([[lat, -180], [lat, 180]], gridStyle).addTo(graticule)
    for (const lon of [-120, -60, 0, 60, 120]) L.polyline([[-85, lon], [85, lon]], gridStyle).addTo(graticule)

    geography.current = L.geoJSON(boundaries).addTo(instance)
    instance.createPane('eventCells').style.zIndex = '450'
    activity.current = L.layerGroup().addTo(instance)
    instance.attributionControl.setPrefix(false)
    instance.attributionControl.addAttribution('Natural Earth')
    const resize = new ResizeObserver(() => {
      instance.invalidateSize({ pan: false })
      if (worldView.current) instance.fitBounds(worldBounds, { padding: [12, 10], animate: false })
    })
    resize.observe(container.current)
    return () => {
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
      shape.bindTooltip(tooltip(target ? `${target.country_name} (${target.country_code}) · Click to filter` : name))
      if (target?.country_code) {
        const code = target.country_code
        shape.on('click', () => onCountry(code))
        shape.on('mouseover', () => shape.setStyle({ fillColor: '#365663' }))
        shape.on('mouseout', () => shape.setStyle({ fillColor: selected ? '#315d66' : '#233445' }))
      }
    })
  }, [countries, activeCountry, onCountry])

  useEffect(() => {
    const layer = activity.current
    if (!layer) return
    layer.clearLayers()
    const half = (data?.cell_degrees ?? 2) / 2
    for (const { latitude, longitude, count: total } of data?.cells ?? []) {
      const band = bands.find(({ max }) => total <= max)!
      const details = `${count(total)} events · ${data?.cell_degrees}° cell · Center ${latitude}°, ${longitude}°`
      L.rectangle([[latitude - half, longitude - half], [latitude + half, longitude + half]], {
        pane: 'eventCells', fillColor: band.color, color: band.color, fillOpacity: 0.8, opacity: 0.85, weight: 1,
      }).bindTooltip(tooltip(details), { sticky: true }).bindPopup(tooltip(details)).addTo(layer)
    }
  }, [data])

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
            {pending ? 'Loading mapped activity…' : unavailable ? 'Mapped activity unavailable' : 'No mapped events in this selection'}
          </div>
        )}
      </div>
      <div className="map-footer">
        <div className="map-legend" aria-label="Event count legend">
          <span>Events per cell</span>
          {bands.map(({ color, label }) => <span key={label}><i style={{ background: color }} />{label}</span>)}
        </div>
        <span>{data?.omitted_events ? `${count(data.omitted_events)} events outside the returned cells` : 'Drag to pan · Select a country to filter'}</span>
      </div>
    </>
  )
}
