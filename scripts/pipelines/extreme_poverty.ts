import { writeJson } from '../lib/io.ts';
import { upsertSource } from '../lib/manifest.ts';

// Pipeline to compute population-weighted global extreme poverty (% at $2.15, 2017 PPP)

const EXCLUDE = new Set([
  'WLD','HIC','INX','LIC','LMC','MIC','UMC','OED','ARB','EAP','ECA','ECS','EUU','LCN','LAC','MEA','NAC','SAS','SSA','FCS'
]);

async function fetchIndicator(indicator: string) {
  const rows: any[] = [];
  let page = 1;
  while (true) {
    const url = `https://api.worldbank.org/v2/country/all/indicator/${indicator}?format=json&per_page=20000&page=${page}`;
    const res = await fetch(url, {
      headers: { 'User-Agent': 'GAI-fetch-bot' },
    });
    if (!res.ok) {
      const txt = await res.text();
      console.error(`[extreme_poverty] fetch ${indicator} page ${page} failed: ${res.status} ${txt.slice(0,100)}`);
      throw new Error('fetch failed');
    }
    const json = await res.json();
    const meta = Array.isArray(json) ? json[0] : null;
    const data = Array.isArray(json) ? json[1] : null;
    if (Array.isArray(data)) rows.push(...data);
    const pages = meta?.pages ?? 1;
    if (page >= pages) break;
    page++;
  }
  return rows;
}

export async function run() {
  const [povRows, popRows] = await Promise.all([
    fetchIndicator('SI.POV.DDAY'),
    fetchIndicator('SP.POP.TOTL')
  ]);

  const popMap = new Map<string, number>();
  for (const d of popRows) {
    const iso = (d?.countryiso3code || d?.country?.id || '').toUpperCase();
    const year = Number(d?.date);
    const val = Number(d?.value);
    if (iso && !EXCLUDE.has(iso) && /^[A-Z]{3}$/.test(iso) && !isNaN(year) && !isNaN(val)) {
      popMap.set(`${iso}:${year}`, val);
    }
  }
  if (popMap.size === 0) throw new Error('[extreme_poverty] empty population map');

  const totals: Record<number, { sum: number; pop: number }> = {};
  const isoSet = new Set<string>();
  for (const d of povRows) {
    const iso = (d?.countryiso3code || d?.country?.id || '').toUpperCase();
    const year = Number(d?.date);
    const pov = Number(d?.value);
    if (iso && !EXCLUDE.has(iso) && /^[A-Z]{3}$/.test(iso) && !isNaN(year) && !isNaN(pov)) {
      const pop = popMap.get(`${iso}:${year}`);
      if (pop != null && !isNaN(pop)) {
        if (!totals[year]) totals[year] = { sum: 0, pop: 0 };
        totals[year].sum += pov * pop;
        totals[year].pop += pop;
        isoSet.add(iso);
      }
    }
  }

  const data = Object.entries(totals)
    .map(([year, { sum, pop }]) => ({ year: Number(year), value: Number((sum / pop).toFixed(2)) }))
    .sort((a, b) => a.year - b.year);
  if (data.length === 0) {
    throw new Error('[extreme_poverty] computed points = 0; nothing written.');
  }

  console.log(`[extreme_poverty] raw rows: ${povRows.length} / population rows: ${popRows.length} / ISO3 count: ${isoSet.size} / years count: ${Object.keys(totals).length} / computed points: ${data.length}`);

  await writeJson('public/data/extreme_poverty.json', data);
  await upsertSource('extreme_poverty', {
    name: 'Extreme poverty ($2.15)',
    domain: 'Economics & Poverty',
    unit: '% of population',
    source_org: 'World Bank WDI',
    source_url: 'https://data.worldbank.org/indicator/SI.POV.DDAY',
    license: 'CC BY 4.0',
    cadence: 'annual',
    method: 'Pop-weighted global mean from national SI.POV.DDAY using SP.POP.TOTL; exclude aggregates; round 2 decimals; WDI may include modeled/nowcasted values.',
    updated_at: new Date().toISOString(),
    data_start_year: data.length ? data[0].year : undefined,
  });
  return data;
}

if ((import.meta as any).main) {
  run().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
