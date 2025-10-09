import { readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import url from 'node:url';

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..', '..');

// This file is the prebuilt JS body of the TS pipeline (committed).
// If you later change the TS, re-export an updated JS body here (same API).

async function run() {
  // keep this in sync with TS pipeline (minus types)
  const OUTPUT_JSON = path.join(root, 'public', 'data', 'firearm_stock_per_100.json');
  const GAISUM_LOG = path.join(root, 'scripts', 'logs', 'firearm_stock_per_100.gaisum.json');
  // Offline-first assumption: cached SAS + WDI CSV snapshots live in scripts/data.
  const DATA_DIR = path.join(root, 'scripts', 'data');

  const csv = async (name) => readFile(path.join(DATA_DIR, name), 'utf8');
  const parse = (txt) => {
    const [h, ...rows] = txt.trim().split(/\r?\n/);
    const cols = h.split(',').map(s => s.trim().replace(/^"|"$/g, ''));
    return rows.filter(Boolean).map(r => {
      const cells = r.split(',');
      const o = {};
      cols.forEach((c,i)=>o[c]=(cells[i]??'').trim().replace(/^"|"$/g,''));
      return o;
    });
  };
  const ensureContinuity = (series) => {
    const years = series.map(d => d.year);
    for (let i = 1; i < years.length; i += 1) {
      const gap = years[i] - years[i - 1];
      if (gap > 5) {
        throw new Error(`gap of ${gap} years detected between ${years[i - 1]} and ${years[i]}`);
      }
    }
  };
  const ensureRange = (series) => {
    for (const row of series) {
      if (row.value < 0 || row.value > 120) {
        throw new Error(`value out of expected range (0-120) for year ${row.year}: ${row.value}`);
      }
    }
  };

  const sasRows = parse(await csv('sas_civilian_firearms.csv'));
  const wdiRows = parse(await csv('wdi_population.csv'));

  // normalize
  const sas = sasRows.map(r => ({ iso3: (r.iso3||'').toUpperCase(), year:+r.year, civilian_firearms:+r.civilian_firearms }))
    .filter(r => r.iso3 && Number.isInteger(r.year) && r.civilian_firearms>0);
  const wdi = wdiRows.map(r => ({ iso3: (r.iso3||'').toUpperCase(), year:+r.year, population:+r.population }))
    .filter(r => r.iso3 && Number.isInteger(r.year) && r.population>0);

  console.log(`[firearm_stock_per_100.js] SAS rows: ${sas.length}`);
  console.log(`[firearm_stock_per_100.js] WDI rows: ${wdi.length}`);

  // join
  const pop = new Map(wdi.map(r => [`${r.iso3}:${r.year}`, r.population]));
  const joined = sas.map(r => {
    const P = pop.get(`${r.iso3}:${r.year}`);
    if (!P) return null;
    return { year:r.year, per100: (r.civilian_firearms*100)/P, population:P };
  }).filter(Boolean);

  console.log(`[firearm_stock_per_100.js] joined rows: ${joined.length}`);

  const totalPopulationByYear = new Map();
  for (const row of wdi) {
    const prev = totalPopulationByYear.get(row.year) || 0;
    totalPopulationByYear.set(row.year, prev + row.population);
  }

  // aggregate by year (weighted)
  const byY = new Map();
  for (const r of joined) {
    const a = byY.get(r.year) || { num:0, den:0 };
    a.num += r.per100 * r.population;
    a.den += r.population;
    byY.set(r.year, a);
  }
  const aggregated = [...byY.entries()].map(([y, a]) => {
    const denom = a.den;
    const value = denom > 0 ? a.num / denom : NaN;
    const totalPop = totalPopulationByYear.get(+y) || denom;
    const coverage = totalPop > 0 ? denom / totalPop : 0;
    return { year: +y, value, coverage };
  }).sort((a,b)=>a.year-b.year);

  const filtered = aggregated.filter(row => {
    if (!Number.isFinite(row.value) || row.value <= 0) {
      console.warn(`[firearm_stock_per_100.js] skip year ${row.year} due to invalid value`);
      return false;
    }
    if (!Number.isFinite(row.coverage) || row.coverage <= 0) {
      console.warn(`[firearm_stock_per_100.js] skip year ${row.year} due to zero coverage`);
      return false;
    }
    if (row.coverage < 0.3) {
      console.warn(`[firearm_stock_per_100.js] skip year ${row.year} (coverage ${row.coverage.toFixed(3)})`);
      return false;
    }
    return true;
  });

  const coverageValues = filtered.map(row => row.coverage);

  const series = filtered
    .map(row => ({ year: row.year, value: Math.round(row.value*1000)/1000 }))
    .sort((a,b)=>a.year-b.year);

  ensureContinuity(series);
  ensureRange(series);

  await mkdir(path.dirname(OUTPUT_JSON), { recursive:true });
  await writeFile(OUTPUT_JSON, JSON.stringify(series, null, 2)+'\n', 'utf8');

  const vals = series.map(d=>d.value);
  const yrs = series.map(d=>d.year);
  const roundCoverage = value => (value === null || value === undefined ? null : Math.round(value * 1000) / 1000);

  const gaisum = {
    metric: 'firearm_stock_per_100',
    rows: series.length,
    min_year: Math.min(...yrs),
    max_year: Math.max(...yrs),
    min_value: Math.min(...vals),
    max_value: Math.max(...vals),
    coverage_min: roundCoverage(coverageValues.length ? Math.min(...coverageValues) : null),
    coverage_max: roundCoverage(coverageValues.length ? Math.max(...coverageValues) : null),
    coverage_mean: roundCoverage(
      coverageValues.length
        ? coverageValues.reduce((acc, val) => acc + val, 0) / coverageValues.length
        : null,
    ),
  };
  await mkdir(path.dirname(GAISUM_LOG), { recursive:true });
  await writeFile(GAISUM_LOG, JSON.stringify(gaisum, null, 2)+'\n', 'utf8');

  const coverageSummary =
    gaisum.coverage_min !== null && gaisum.coverage_max !== null
      ? `, coverage [${gaisum.coverage_min.toFixed(3)} – ${gaisum.coverage_max.toFixed(3)}]`
      : '';

  console.log(`GAISUM firearm_stock_per_100 → ${series.length} rows (${gaisum.min_year}-${gaisum.max_year}), range [${gaisum.min_value.toFixed(3)} – ${gaisum.max_value.toFixed(3)}]${coverageSummary}`);
  return series;
}

const isEntry = import.meta.url === new URL(process.argv[1], 'file://').href;
if (isEntry) run().catch(e => { console.error('[firearm_stock_per_100.js] fatal:', e); process.exit(1); });
export { run };
