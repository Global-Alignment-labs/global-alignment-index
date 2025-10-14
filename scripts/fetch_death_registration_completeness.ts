import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { writeJson } from "./lib/io.ts";
import { upsertSource } from "./lib/manifest.ts";

type CsvRow = Record<string, string>;

type SeriesPoint = { year: number; value: number };

type CountryPoint = { iso3: string; country: string; year: number; value: number };

const RAW_PATH = resolve(process.cwd(), "data/raw/wdi_death_registration.csv");
const POP_PATH = resolve(process.cwd(), "data/raw/pop_by_country.csv");
const OUTPUT_GLOBAL = resolve(process.cwd(), "public/data/death_registration_completeness.json");
const OUTPUT_BY_COUNTRY = resolve(
  process.cwd(),
  "public/data/by_country/death_registration_completeness.json",
);

const METRIC_ID = "death_registration_completeness";
const SOURCE_ID = "wdi_sp_reg_dths_zs";
const INDICATOR_CODE = "SP.REG.DTHS.ZS";
const START_YEAR = 1990;
const END_YEAR = 2023;

const ISO_WLD = "WLD";

const AGGREGATE_ISO3 = new Set([
  "AFE",
  "AFW",
  "ARB",
  "CEB",
  "CSS",
  "EAP",
  "EAR",
  "EAS",
  "ECA",
  "ECS",
  "EMU",
  "EUU",
  "FCS",
  "HIC",
  "HPC",
  "IBD",
  "IBT",
  "IDA",
  "IDB",
  "IDX",
  "LAC",
  "LCN",
  "LDC",
  "LIC",
  "LMC",
  "LMY",
  "LTE",
  "MEA",
  "MIC",
  "MNA",
  "NAC",
  "OED",
  "OSS",
  "PRE",
  "PSS",
  "PST",
  "SAS",
  "SSA",
  "SSF",
  "SST",
  "TEA",
  "TEC",
  "TLA",
  "TMN",
  "TSA",
  "TSS",
  "UMC",
]);

function parseCsvLine(line: string): string[] {
  const cells: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (inQuotes) {
      if (char === "\"") {
        if (line[i + 1] === "\"") {
          current += "\"";
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        current += char;
      }
    } else if (char === "\"") {
      inQuotes = true;
    } else if (char === ",") {
      cells.push(current.trim());
      current = "";
    } else {
      current += char;
    }
  }
  cells.push(current.trim());
  return cells;
}

function parseCsv(text: string): { headers: string[]; rows: CsvRow[] } {
  const normalized = text.replace(/\r\n/g, "\n");
  const lines = normalized
    .split("\n")
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0);
  if (!lines.length) {
    return { headers: [], rows: [] };
  }
  if (lines[0][0] === "\ufeff") {
    lines[0] = lines[0].slice(1);
  }
  const headers = parseCsvLine(lines[0]).map((header) => header.trim());
  const rows: CsvRow[] = [];
  for (let i = 1; i < lines.length; i++) {
    const raw = parseCsvLine(lines[i]);
    const row: CsvRow = {};
    headers.forEach((header, idx) => {
      row[header] = raw[idx] ?? "";
    });
    rows.push(row);
  }
  return { headers, rows };
}

function isCountryIso(code: string): boolean {
  return /^[A-Z]{3}$/.test(code) && code !== ISO_WLD && !AGGREGATE_ISO3.has(code);
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function roundTo(value: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

async function loadDeathRegistration(): Promise<CountryPoint[]> {
  const text = await readFile(RAW_PATH, "utf8");
  const { headers, rows } = parseCsv(text);
  if (!headers.length) {
    throw new Error("[death-registration] raw CSV missing headers");
  }
  const isoKey = headers.find((header) => header.toLowerCase() === "iso3") ?? "iso3";
  const countryKey = headers.find((header) => header.toLowerCase().includes("country")) ?? "country";
  const yearKey = headers.find((header) => header.toLowerCase() === "year") ?? "year";
  const valueKey = headers.find((header) => header.toLowerCase() === "value") ?? "value";

  const results: CountryPoint[] = [];
  for (const row of rows) {
    const iso = (row[isoKey] ?? "").trim().toUpperCase();
    const country = (row[countryKey] ?? "").trim();
    if (!isCountryIso(iso)) continue;
    const year = Number((row[yearKey] ?? "").trim());
    if (!Number.isInteger(year) || year < START_YEAR || year > END_YEAR) continue;
    const valueText = (row[valueKey] ?? "").trim();
    if (!valueText || valueText.toLowerCase() === "na" || valueText.toLowerCase() === "null") continue;
    const rawValue = Number(valueText);
    if (!Number.isFinite(rawValue)) continue;
    const value = clamp(rawValue, 0, 100);
    results.push({ iso3: iso, country, year, value });
  }
  return results;
}

async function loadPopulation(): Promise<{
  populationByIsoYear: Map<string, number>;
  worldPopulation: Map<number, number>;
  countryNames: Map<string, string>;
}> {
  const text = await readFile(POP_PATH, "utf8");
  const { headers, rows } = parseCsv(text);
  if (!headers.length) {
    throw new Error("[death-registration] population CSV missing headers");
  }
  const isoKey = headers.find((header) => header.toLowerCase() === "iso3") ?? "iso3";
  const yearKey = headers.find((header) => header.toLowerCase() === "year") ?? "year";
  const popKey = headers.find((header) => header.toLowerCase().includes("pop")) ?? "population";
  const countryKey = headers.find((header) => header.toLowerCase().includes("country")) ?? "country";

  const populationByIsoYear = new Map<string, number>();
  const worldPopulation = new Map<number, number>();
  const countryNames = new Map<string, string>();

  for (const row of rows) {
    const iso = (row[isoKey] ?? "").trim().toUpperCase();
    const year = Number((row[yearKey] ?? "").trim());
    const pop = Number((row[popKey] ?? "").trim());
    if (!Number.isInteger(year)) continue;
    if (!Number.isFinite(pop) || pop <= 0) continue;
    if (iso === ISO_WLD) {
      worldPopulation.set(year, pop);
      continue;
    }
    if (!isCountryIso(iso)) continue;
    populationByIsoYear.set(`${iso}:${year}`, pop);
    if (!countryNames.has(iso)) {
      countryNames.set(iso, (row[countryKey] ?? "").trim());
    }
  }

  return { populationByIsoYear, worldPopulation, countryNames };
}

async function run(): Promise<void> {
  const [deathRows, popData] = await Promise.all([
    loadDeathRegistration(),
    loadPopulation(),
  ]);

  const { populationByIsoYear, worldPopulation, countryNames } = popData;

  const byCountry: CountryPoint[] = [];
  const statsByYear = new Map<
    number,
    { weighted: number; pop: number; isoSet: Set<string>; coveragePop: number }
  >();

  for (const row of deathRows) {
    const pop = populationByIsoYear.get(`${row.iso3}:${row.year}`);
    if (!pop) continue;
    const stats = statsByYear.get(row.year) ?? {
      weighted: 0,
      pop: 0,
      isoSet: new Set<string>(),
      coveragePop: 0,
    };
    stats.weighted += row.value * pop;
    stats.pop += pop;
    stats.coveragePop += pop;
    stats.isoSet.add(row.iso3);
    statsByYear.set(row.year, stats);

    byCountry.push({
      iso3: row.iso3,
      country: countryNames.get(row.iso3) ?? row.country ?? row.iso3,
      year: row.year,
      value: roundTo(row.value, 2),
    });
  }

  const years = Array.from(statsByYear.keys()).sort((a, b) => a - b);
  const series: SeriesPoint[] = [];
  const coverageByYear = new Map<number, number>();
  const meanAccumulator: number[] = [];

  for (const year of years) {
    const stats = statsByYear.get(year);
    if (!stats || stats.pop === 0) continue;
    const mean = stats.weighted / stats.pop;
    series.push({ year, value: roundTo(mean, 1) });
    meanAccumulator.push(mean);

    const worldPop = worldPopulation.get(year);
    if (worldPop && worldPop > 0) {
      coverageByYear.set(year, clamp(stats.coveragePop / worldPop, 0, 1));
    }
  }

  await writeJson(OUTPUT_GLOBAL, series);
  await writeJson(OUTPUT_BY_COUNTRY, byCountry.sort((a, b) => {
    return a.iso3 === b.iso3 ? a.year - b.year : a.iso3.localeCompare(b.iso3);
  }));

  const minYear = series.length ? series[0].year : null;
  const maxYear = series.length ? series[series.length - 1].year : null;
  const meanValue =
    meanAccumulator.length > 0
      ? roundTo(meanAccumulator.reduce((acc, value) => acc + value, 0) / meanAccumulator.length, 2)
      : NaN;
  const coverage2023 = coverageByYear.get(2023) ?? 0;

  const first3 = JSON.stringify(series.slice(0, 3), null, 2);
  const last3 = JSON.stringify(series.slice(-3), null, 2);

  const countries1990 = statsByYear.get(1990)?.isoSet.size ?? 0;
  const countries2023 = statsByYear.get(2023)?.isoSet.size ?? 0;

  console.log(`[${METRIC_ID}] first3 ${first3}`);
  console.log(`[${METRIC_ID}] last3 ${last3}`);
  console.log(
    `[${METRIC_ID}] countries_used 1990=${countries1990} 2023=${countries2023}`,
  );
  console.log(
    `GAISUM ${METRIC_ID} rows=${series.length} min_year=${minYear ?? "n/a"} max_year=${
      maxYear ?? "n/a"
    } mean=${Number.isFinite(meanValue) ? meanValue.toFixed(2) : "nan"} coverage_2023=${coverage2023.toFixed(4)}`,
  );

  await upsertSource(SOURCE_ID, {
    id: SOURCE_ID,
    name: "World Bank WDI — Completeness of death registration (%)",
    code: INDICATOR_CODE,
    domain: "truth_and_clarity",
    description:
      "Share of deaths registered with civil registration systems. Population-weighted global mean across countries.",
    url: "https://data.worldbank.org/indicator/SP.REG.DTHS.ZS",
    publisher: "World Bank (WHO/UN DESA CRVS underlying)",
    years: {
      min: minYear,
      max: maxYear,
    },
    license: "CC BY 4.0",
    method:
      "Clamp national SP.REG.DTHS.ZS values to [0,100], join WDI SP.POP.TOTL populations, compute population-weighted annual global mean, round to 1 decimal.",
    last_fetched: new Date().toISOString().slice(0, 10),
  });
}

const isEntry = import.meta.url === new URL(process.argv[1], "file://").href;

if (isEntry) {
  run().catch((error) => {
    console.error(`[${METRIC_ID}] fatal`, error);
    process.exit(1);
  });
}

export { run };
