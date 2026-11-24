import type { Position } from 'geojson'

function nearestLongitude(longitude: number, reference: number) {
  return longitude + 360 * Math.round((reference - longitude) / 360)
}

export function mapPolygon(rings: Position[][]): Position[][][] {
  const reference = rings[0][0][0]
  const unwrapped = rings.map((ring) => {
    let previous = reference
    return ring.map(([longitude, latitude]) => {
      previous = nearestLongitude(longitude, previous)
      return [previous, latitude]
    })
  })
  const longitudes = unwrapped[0].map(([longitude]) => longitude)
  const shift = Math.min(...longitudes) < -180 ? 360 : Math.max(...longitudes) > 180 ? -360 : 0
  // Draw a second copy at the opposite edge when a polygon crosses the date line.
  return shift ? [unwrapped, unwrapped.map((ring) => ring.map(([lon, lat]) => [lon + shift, lat]))] : [unwrapped]
}
