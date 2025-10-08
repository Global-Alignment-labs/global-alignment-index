import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, '..', '..');

// Offline-first assumption: cached SAS + WDI CSV snapshots live in scripts/data.
const DATA_DIR = path.join(projectRoot, 'scripts', 'data');
const OUTPUT_JSON = path.join(projectRoot, 'public', 'data', 'firearm_stock_per_100.json');
const GAISUM_LOG = path.join(projectRoot, 'scripts', 'logs', 'firearm_stock_per_100.gaisum.json');

const DEFAULT_SAS_URL = 'https://raw.githubusercontent.com/smallarms-survey/firearms-holdings/main/civilian_holdings.csv';
const DEFAULT_WDI_URL = 'https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv';

type SasRow = {
  iso3: string;
  year: number;
  civilian_firearms: number;
};

type WdiRow = {
  iso3: string;
  year: number;
  population: number;
};

type JoinedRow = SasRow & { population: number; per100: number };

type YearAggregate = {
  year: number;
  numerator: number;
  denominator: number;
  populationCovered: number;
};

function toNumber(value: string, label: string): number {
  if (value === undefined || value === null || value === '') {
    throw new Error(`missing numeric value for ${label}`);
  }
  const num = Number(value);
  if (!Number.isFinite(num)) {
    throw new Error(`invalid numeric value for ${label}: ${value}`);
  }
  return num;
}

async function readLocalCsv(relPath: string): Promise<string> {
  const fullPath = path.join(DATA_DIR, relPath);
  return fs.readFile(fullPath, 'utf8');
}

async function loadCsv(label: string, fallbackRelPath: string, url?: string): Promise<string> {
  const offline = process.env.OFFLINE === '1';
  if (offline) {
    console.log(`[firearm_stock_per_100] OFFLINE=1 → using cached ${label}`);
    return readLocalCsv(fallbackRelPath);
  }

  const candidateUrl = url ?? (label === 'SAS' ? DEFAULT_SAS_URL : DEFAULT_WDI_URL);
  if (!candidateUrl) {
    console.warn(`[firearm_stock_per_100] no URL for ${label}, falling back to cache`);
    return readLocalCsv(fallbackRelPath);
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30_000);
    console.log(`[firearm_stock_per_100] fetching ${label} from ${candidateUrl}`);
    const res = await fetch(candidateUrl, { signal: controller.signal });
    clearTimeout(timeout);
    if (!res.ok) {
      throw new Error(`fetch failed: ${res.status} ${res.statusText}`);
    }
    const text = await res.text();
    if (!text.trim()) {
      throw new Error('remote response empty');
    }
    return text;
  } catch (err) {
    console.warn(`[firearm_stock_per_100] remote ${label} fetch failed (${(err as Error).message}); using cached copy`);
    try {
      return await readLocalCsv(fallbackRelPath);
    } catch (fallbackErr) {
      throw new Error(`failed to load ${label}: ${(fallbackErr as Error).message}`);
    }
  }
}

function parseCsv(text: string): Record<string, string>[] {
  const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length === 0) {
    return [];
  }
  const header = lines[0].split(',').map((cell) => cell.trim().replace(/^"|"$/g, ''));
  return lines.slice(1).map((line) => {
    const cells = line.split(',');
    const record: Record<string, string> = {};
    header.forEach((col, idx) => {
      record[col] = (cells[idx] ?? '').trim().replace(/^"|"$/g, '');
    });
    return record;
  });
}

function normalizeSas(rows: Record<string, string>[]): SasRow[] {
  const output: SasRow[] = [];
  for (const row of rows) {
    const iso3 = row.iso3?.toUpperCase();
    const yearStr = row.year ?? row.Year ?? row.YEAR;
    const firearmsStr = row.civilian_firearms ?? row.firearms ?? row.total ?? row.value;
    if (!iso3 || !yearStr || !firearmsStr) {
      continue;
    }
    const year = Number(yearStr);
    const civilian_firearms = toNumber(firearmsStr, `civilian_firearms (${iso3})`);
    if (!Number.isInteger(year) || year < 1800) {
      continue;
    }
    output.push({ iso3, year, civilian_firearms });
  }
  return output;
}

function normalizeWdi(rows: Record<string, string>[]): WdiRow[] {
  const output: WdiRow[] = [];
  for (const row of rows) {
    const iso3 = row.iso3?.toUpperCase() ?? row.CountryCode?.toUpperCase();
    const yearStr = row.year ?? row.Year;
    const popStr = row.population ?? row.Value;
    if (!iso3 || !yearStr || !popStr) {
      continue;
    }
    const year = Number(yearStr);
    if (!Number.isInteger(year) || year < 1800) {
      continue;
    }
    const population = toNumber(popStr, `population (${iso3})`);
    if (population <= 0) {
      console.warn(`[firearm_stock_per_100] drop row with non-positive population for ${iso3} ${year}`);
      continue;
    }
    output.push({ iso3, year, population });
  }
  return output;
}

function roundN(value: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round((value + Number.EPSILON) * factor) / factor;
}

function joinDatasets(sas: SasRow[], wdi: WdiRow[]): JoinedRow[] {
  const populationIndex = new Map<string, number>();
  for (const row of wdi) {
    populationIndex.set(`${row.iso3}:${row.year}`, row.population);
  }

  const joined: JoinedRow[] = [];
  for (const row of sas) {
    const key = `${row.iso3}:${row.year}`;
    const population = populationIndex.get(key);
    if (!population) {
      continue;
    }
    if (row.civilian_firearms <= 0) {
      console.warn(`[firearm_stock_per_100] drop row with non-positive firearms for ${row.iso3} ${row.year}`);
      continue;
    }
    const per100 = (row.civilian_firearms * 100) / population;
    joined.push({ ...row, population, per100 });
  }
  return joined;
}

function aggregateByYear(rows: JoinedRow[], wdi: WdiRow[]): { year: number; value: number; coverage: number }[] {
  const byYear = new Map<number, YearAggregate>();
  for (const row of rows) {
    const current = byYear.get(row.year) ?? {
      year: row.year,
      numerator: 0,
      denominator: 0,
      populationCovered: 0,
    };
    current.numerator += row.per100 * row.population;
    current.denominator += row.population;
    current.populationCovered += row.population;
    byYear.set(row.year, current);
  }

  const totalPopulationByYear = new Map<number, number>();
  for (const row of wdi) {
    const total = totalPopulationByYear.get(row.year) ?? 0;
    totalPopulationByYear.set(row.year, total + row.population);
  }

  const totals = Array.from(byYear.values())
    .map((agg) => {
      if (agg.denominator <= 0) {
        throw new Error(`zero denominator for year ${agg.year}`);
      }
      const value = agg.numerator / agg.denominator;
      const totalPop = totalPopulationByYear.get(agg.year) ?? agg.denominator;
      const coverage = totalPop > 0 ? agg.populationCovered / totalPop : 0;
      return {
        year: agg.year,
        value,
        coverage,
      };
    })
    .sort((a, b) => a.year - b.year);

  return totals;
}

function ensureContinuity(series: { year: number; value: number }[]): void {
  const years = series.map((row) => row.year);
  for (let i = 1; i < years.length; i += 1) {
    const gap = years[i] - years[i - 1];
    if (gap > 5) {
      throw new Error(`gap of ${gap} years detected between ${years[i - 1]} and ${years[i]}`);
    }
  }
}

function ensureRange(series: { year: number; value: number }[]): void {
  for (const row of series) {
    if (row.value < 0 || row.value > 120) {
      throw new Error(`value out of expected range (0-120) for year ${row.year}: ${row.value}`);
    }
  }
}

export async function run() {
  const sasRaw = await loadCsv('SAS', 'sas_civilian_firearms.csv', process.env.SAS_URL);
  const wdiRaw = await loadCsv('WDI', 'wdi_population.csv', process.env.WDI_URL);

  const sasRows = normalizeSas(parseCsv(sasRaw));
  const wdiRows = normalizeWdi(parseCsv(wdiRaw));

  console.log(`[firearm_stock_per_100] SAS rows: ${sasRows.length}`);
  console.log(`[firearm_stock_per_100] WDI rows: ${wdiRows.length}`);

  const joined = joinDatasets(sasRows, wdiRows);
  console.log(`[firearm_stock_per_100] joined rows: ${joined.length}`);

  const aggregated = aggregateByYear(joined, wdiRows);

  const filtered = aggregated.filter((row) => {
    const coverage = row.coverage;
    if (!Number.isFinite(coverage) || coverage <= 0) {
      console.warn(`[firearm_stock_per_100] skip year ${row.year} due to zero coverage`);
      return false;
    }
    if (coverage < 0.3) {
      console.warn(`[firearm_stock_per_100] skip year ${row.year} (coverage ${coverage.toFixed(3)})`);
      return false;
    }
    return true;
  });

  const coverageValues = filtered.map((row) => row.coverage);

  const series = filtered.map((row) => ({
    year: row.year,
    value: roundN(row.value, 3),
  }));

  ensureContinuity(series);
  ensureRange(series);

  if (series.length === 0) {
    throw new Error('no data produced for firearm_stock_per_100');
  }

  const values = series.map((row) => row.value);
  const years = series.map((row) => row.year);

  await fs.mkdir(path.dirname(OUTPUT_JSON), { recursive: true });
  await fs.writeFile(OUTPUT_JSON, `${JSON.stringify(series, null, 2)}\n`, 'utf8');

  const gaisum = {
    metric: 'firearm_stock_per_100',
    rows: series.length,
    min_year: Math.min(...years),
    max_year: Math.max(...years),
    min_value: Math.min(...values),
    max_value: Math.max(...values),
    coverage_min: coverageValues.length ? Math.min(...coverageValues) : null,
    coverage_max: coverageValues.length ? Math.max(...coverageValues) : null,
    coverage_mean:
      coverageValues.length
        ? coverageValues.reduce((acc, val) => acc + val, 0) / coverageValues.length
        : null,
  };
  await fs.mkdir(path.dirname(GAISUM_LOG), { recursive: true });
  await fs.writeFile(GAISUM_LOG, `${JSON.stringify(gaisum, null, 2)}\n`, 'utf8');

  const coverageSummary =
    gaisum.coverage_min !== null && gaisum.coverage_max !== null
      ? `, coverage [${gaisum.coverage_min.toFixed(3)} – ${gaisum.coverage_max.toFixed(3)}]`
      : '';

  console.log(
    `GAISUM firearm_stock_per_100 → ${series.length} rows (${gaisum.min_year}-${gaisum.max_year}), range [` +
      `${gaisum.min_value.toFixed(3)} – ${gaisum.max_value.toFixed(3)}]${coverageSummary}`,
  );

  return series;
}

const isEntry = import.meta.url === new URL(process.argv[1], 'file://').href;
if (isEntry) {
  run().catch((err) => {
    console.error('[firearm_stock_per_100] fatal:', err);
    process.exit(1);
  });
}
