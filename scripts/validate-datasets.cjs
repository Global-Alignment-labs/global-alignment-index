const fs = require('fs')
const path = require('path')

const dataDir = path.join(__dirname, '..', 'public', 'data')
const schemaPath = path.join(__dirname, '..', 'schemas', 'timeseries.schema.json')
const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf8'))

const files = fs
  .readdirSync(dataDir)
  .filter(f => f.endsWith('.json') && !['sources.json', 'metrics_registry.json'].includes(f))

let ok = true

for (const f of files) {
  const full = path.join(dataDir, f)
  try {
    const data = JSON.parse(fs.readFileSync(full, 'utf8'))
    if (!Array.isArray(data)) throw new Error('not an array')

    if (f.endsWith('_coverage.json')) {
      let prevYear = -Infinity
      data.forEach((d, i) => {
        if (typeof d !== 'object' || d === null) throw new Error(`item ${i} not object`)
        const keys = Object.keys(d)
        if (
          keys.length !== 4 ||
          !('year' in d) ||
          !('coverage' in d) ||
          !('n_iso' in d) ||
          !('n_pop' in d)
        )
          throw new Error(`item ${i} invalid keys`)
        if (!Number.isInteger(d.year)) throw new Error(`item ${i} year not integer`)
        if (
          typeof d.coverage !== 'number' ||
          Number.isNaN(d.coverage) ||
          d.coverage < 0 ||
          d.coverage > 1
        )
          throw new Error(`item ${i} coverage out of range`)
        if (typeof d.n_iso !== 'number' || d.n_iso < 0)
          throw new Error(`item ${i} n_iso invalid`)
        if (typeof d.n_pop !== 'number' || d.n_pop < d.n_iso)
          throw new Error(`item ${i} n_pop invalid`)
        if (d.year <= prevYear) throw new Error(`item ${i} year not ascending`)
        prevYear = d.year
      })
    } else if (f.endsWith('_by_type.json')) {
      const ORDER = new Map([
        ['interstate', 0],
        ['intrastate', 1],
        ['internationalized_intrastate', 2],
        ['extrasystemic', 3],
      ])
      let prevYear = -Infinity
      let prevTypeIdx = -1
      data.forEach((d, i) => {
        if (typeof d !== 'object' || d === null) throw new Error(`item ${i} not object`)
        const keys = Object.keys(d)
        if (keys.length !== 3 || !('year' in d) || !('type' in d) || !('value' in d))
          throw new Error(`item ${i} invalid keys`)
        if (!Number.isInteger(d.year)) throw new Error(`item ${i} year not integer`)
        if (typeof d.type !== 'string' || !d.type)
          throw new Error(`item ${i} type invalid`)
        const idx = ORDER.has(d.type) ? ORDER.get(d.type) : null
        if (idx == null) throw new Error(`item ${i} type ${d.type} unexpected`)
        if (typeof d.value !== 'number' || Number.isNaN(d.value))
          throw new Error(`item ${i} value not number`)
        if (d.year < prevYear) throw new Error(`item ${i} year not ascending`)
        if (d.year !== prevYear) {
          prevTypeIdx = -1
        } else if (idx < prevTypeIdx) {
          throw new Error(`item ${i} type order invalid`)
        }
        prevYear = d.year
        prevTypeIdx = idx
      })
    } else if (f.endsWith('.by_country.json')) {
      data.forEach((d, i) => {
        if (typeof d !== 'object' || d === null) throw new Error(`item ${i} not object`)
        const keys = Object.keys(d)
        if (
          keys.length !== 4 ||
          !('iso3' in d) ||
          !('country' in d) ||
          !('year' in d) ||
          !('value' in d)
        )
          throw new Error(`item ${i} invalid keys`)
        if (typeof d.iso3 !== 'string' || !/^[A-Z]{3}$/.test(d.iso3))
          throw new Error(`item ${i} iso3 invalid`)
        if (typeof d.country !== 'string' || !d.country.trim())
          throw new Error(`item ${i} country invalid`)
        if (!Number.isInteger(d.year)) throw new Error(`item ${i} year not integer`)
        if (typeof d.value !== 'number' || Number.isNaN(d.value))
          throw new Error(`item ${i} value not number`)
      })
    } else {
      let prevYear = -Infinity
      data.forEach((d, i) => {
        if (typeof d !== 'object' || d === null) throw new Error(`item ${i} not object`)
        const keys = Object.keys(d)
        if (keys.length !== 2 || !('year' in d) || !('value' in d))
          throw new Error(`item ${i} invalid keys`)
        if (!Number.isInteger(d.year)) throw new Error(`item ${i} year not integer`)
        if (typeof d.value !== 'number' || Number.isNaN(d.value))
          throw new Error(`item ${i} value not number`)
        if (d.year <= prevYear) throw new Error(`item ${i} year not ascending`)
        prevYear = d.year
      })
    }
  } catch (err) {
    console.error(`Validation failed for ${f}: ${err.message}`)
    ok = false
  }
}

if (!ok) {
  process.exit(1)
}
