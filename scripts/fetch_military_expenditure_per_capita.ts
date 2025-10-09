import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { writeJson } from "./lib/io.ts";
import { upsertSource } from "./lib/manifest.ts";

const MILEX_PATH = resolve(process.cwd(), "data/raw/sipri_milex.csv");
const POP_PATH = resolve(process.cwd(), "data/raw/pop_by_country.csv");
const PERCENT_GDP_PATH = resolve(
  process.cwd(),
  "data/raw/sipri_milex_percent_gdp.csv",
);

const OUTPUT_GLOBAL = resolve(
  process.cwd(),
  "public/data/military_expenditure_per_capita.json",
);
const OUTPUT_BY_COUNTRY = resolve(
  process.cwd(),
  "public/data/military_expenditure_per_capita.by_country.json",
);
const OUTPUT_PERCENT_GLOBAL = resolve(
  process.cwd(),
  "public/data/military_expenditure_percent_gdp.json",
);
const OUTPUT_PERCENT_BY_COUNTRY = resolve(
  process.cwd(),
  "public/data/military_expenditure_percent_gdp.by_country.json",
);

const START_YEAR = 1990;
const WARN_MIN = 50;
const WARN_MAX = 3000;
const DECIMALS = 1;
const PERCENT_DECIMALS = 2;
const RETRIEVED_ON = "2025-10-08";

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
  "WLD",
]);

type CsvRow = Record<string, string>;

type CountryYear = {
  iso3: string;
  country: string;
  year: number;
  expenditureUsd: number;
};

type PopulationYear = {
  iso3: string;
  year: number;
  population: number;
};

type PercentGdpYear = {
  iso3: string;
  country: string;
  year: number;
  percentOfGdp: number;
  gdpUsd: number;
};

type SeriesPoint = {
  year: number;
  value: number;
};

function parseCsvLine(line: string): string[] {
  const result: string[] = [];
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
      result.push(current.trim());
      current = "";
    } else {
      current += char;
    }
  }
  result.push(current.trim());
  return result;
}

function parseCsv(text: string): { headers: string[]; rows: CsvRow[] } {
  const normalized = text.replace(/\r\n/g, "\n");
  const lines = normalized
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (lines.length === 0) {
    return { headers: [], rows: [] };
  }
  if (lines[0][0] === "\ufeff") {
    lines[0] = lines[0].slice(1);
  }
  const headers = parseCsvLine(lines[0]);
  const rows: CsvRow[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = parseCsvLine(lines[i]);
    const row: CsvRow = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = cells[j] ?? "";
    }
    rows.push(row);
  }
  return { headers, rows };
}

function roundN(value: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function isCountryIso(code: string): boolean {
  return /^[A-Z]{3}$/.test(code) && !AGGREGATE_ISO3.has(code);
}

async function loadMilex(): Promise<CountryYear[]> {
  const text = await readFile(MILEX_PATH, "utf8");
  const { headers, rows } = parseCsv(text);
  if (!headers.length) {
    throw new Error("[milex] CSV missing headers");
  }
  const idxIso = headers.indexOf("iso3");
  const idxCountry = headers.indexOf("country");
  const idxYear = headers.indexOf("year");
  const idxValue = headers.indexOf("military_expenditure_usd");
  if (idxIso === -1 || idxCountry === -1 || idxYear === -1 || idxValue === -1) {
    throw new Error("[milex] CSV missing required columns");
  }
  const result: CountryYear[] = [];
  for (const row of rows) {
    const iso3 = (row[headers[idxIso]] ?? "").trim().toUpperCase();
    if (!isCountryIso(iso3)) continue;
    const country = (row[headers[idxCountry]] ?? "").trim();
    const year = Number(row[headers[idxYear]]);
    const expenditure = Number(row[headers[idxValue]]);
    if (!Number.isInteger(year) || year < START_YEAR) continue;
    if (!Number.isFinite(expenditure) || expenditure <= 0) continue;
    result.push({ iso3, country, year, expenditureUsd: expenditure });
  }
  return result;
}

async function loadPopulation(): Promise<PopulationYear[]> {
  const text = await readFile(POP_PATH, "utf8");
  const { headers, rows } = parseCsv(text);
  if (!headers.length) {
    throw new Error("[milex] population CSV missing headers");
  }
  const idxIso = headers.indexOf("iso3");
  const idxYear = headers.indexOf("year");
  const idxPop = headers.indexOf("population");
  if (idxIso === -1 || idxYear === -1 || idxPop === -1) {
    throw new Error("[milex] population CSV missing required columns");
  }
  const result: PopulationYear[] = [];
  for (const row of rows) {
    const iso3 = (row[headers[idxIso]] ?? "").trim().toUpperCase();
    if (!isCountryIso(iso3)) continue;
    const year = Number(row[headers[idxYear]]);
    const population = Number(row[headers[idxPop]]);
    if (!Number.isInteger(year) || year < START_YEAR) continue;
    if (!Number.isFinite(population) || population <= 0) continue;
    result.push({ iso3, year, population });
  }
  return result;
}

async function loadPercentOfGdp(): Promise<PercentGdpYear[]> {
  const text = await readFile(PERCENT_GDP_PATH, "utf8");
  const { headers, rows } = parseCsv(text);
  if (!headers.length) {
    throw new Error("[milex] percent-of-GDP CSV missing headers");
  }
  const idxIso = headers.indexOf("iso3");
  const idxCountry = headers.indexOf("country");
  const idxYear = headers.indexOf("year");
  const idxPercent = headers.indexOf("military_expenditure_percent_gdp");
  const idxGdp = headers.indexOf("gdp_current_usd");
  if (
    idxIso === -1 ||
    idxCountry === -1 ||
    idxYear === -1 ||
    idxPercent === -1 ||
    idxGdp === -1
  ) {
    throw new Error("[milex] percent-of-GDP CSV missing required columns");
  }
  const result: PercentGdpYear[] = [];
  for (const row of rows) {
    const iso3 = (row[headers[idxIso]] ?? "").trim().toUpperCase();
    if (!isCountryIso(iso3)) continue;
    const country = (row[headers[idxCountry]] ?? "").trim();
    const year = Number(row[headers[idxYear]]);
    const percent = Number(row[headers[idxPercent]]);
    const gdp = Number(row[headers[idxGdp]]);
    if (!Number.isInteger(year) || year < START_YEAR) continue;
    if (!Number.isFinite(percent)) continue;
    if (!Number.isFinite(gdp) || gdp <= 0) continue;
    result.push({
      iso3,
      country,
      year,
      percentOfGdp: percent,
      gdpUsd: gdp,
    });
  }
  return result;
}

function computeStd(values: number[], mean: number): number {
  if (values.length === 0) return 0;
  const variance =
    values.reduce((acc, value) => acc + (value - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

export async function run(): Promise<SeriesPoint[]> {
  console.log("[milex] load SIPRI military expenditure (current USD)");
  const milex = await loadMilex();
  console.log(`[milex] loaded ${milex.length} country-year expenditure rows`);

  console.log("[milex] load World Bank population by country");
  const population = await loadPopulation();
  console.log(`[milex] loaded ${population.length} country-year population rows`);

  console.log("[milex] load SIPRI military expenditure (% of GDP)");
  const percentOfGdp = await loadPercentOfGdp();
  console.log(
    `[milex] loaded ${percentOfGdp.length} country-year percent-of-GDP rows`,
  );

  const popMap = new Map<string, number>();
  for (const entry of population) {
    popMap.set(`${entry.iso3}:${entry.year}`, entry.population);
  }

  const countryNames = new Map<string, string>();
  const countrySeries = new Map<string, SeriesPoint[]>();
  const globalTotals = new Map<
    number,
    { expenditure: number; population: number }
  >();
  const percentCountrySeries = new Map<string, SeriesPoint[]>();
  const percentCountryNames = new Map<string, string>();
  const percentTotals = new Map<
    number,
    { weightedPercentTimesGdp: number; gdp: number }
  >();

  for (const entry of milex) {
    const pop = popMap.get(`${entry.iso3}:${entry.year}`);
    if (pop === undefined) continue;
    const value = entry.expenditureUsd / pop;
    if (!Number.isFinite(value) || value <= 0) continue;

    countryNames.set(entry.iso3, entry.country);
    const rounded = roundN(value, DECIMALS);
    const point: SeriesPoint = { year: entry.year, value: rounded };
    let list = countrySeries.get(entry.iso3);
    if (!list) {
      list = [];
      countrySeries.set(entry.iso3, list);
    }
    list.push(point);

    const agg = globalTotals.get(entry.year) ?? { expenditure: 0, population: 0 };
    agg.expenditure += entry.expenditureUsd;
    agg.population += pop;
    globalTotals.set(entry.year, agg);
  }

  for (const entry of percentOfGdp) {
    const percentValue = entry.percentOfGdp;
    const rounded = roundN(percentValue, PERCENT_DECIMALS);
    percentCountryNames.set(entry.iso3, entry.country);
    let list = percentCountrySeries.get(entry.iso3);
    if (!list) {
      list = [];
      percentCountrySeries.set(entry.iso3, list);
    }
    list.push({ year: entry.year, value: rounded });

    const agg = percentTotals.get(entry.year) ?? {
      weightedPercentTimesGdp: 0,
      gdp: 0,
    };
    agg.weightedPercentTimesGdp += (percentValue / 100) * entry.gdpUsd;
    agg.gdp += entry.gdpUsd;
    percentTotals.set(entry.year, agg);
  }

  const perCountryOutput: Array<{
    iso3: string;
    country: string;
    year: number;
    value: number;
  }> = [];

  for (const [iso3, points] of countrySeries) {
    points.sort((a, b) => a.year - b.year);
    for (const point of points) {
      if (point.value < WARN_MIN || point.value > WARN_MAX) {
        console.warn(
          `[milex] WARN per-capita value out of expected range ${iso3} ${point.year}=${point.value.toFixed(DECIMALS)}`,
        );
      }
      perCountryOutput.push({
        iso3,
        country: countryNames.get(iso3) ?? iso3,
        year: point.year,
        value: point.value,
      });
    }
  }

  perCountryOutput.sort((a, b) => {
    if (a.iso3 === b.iso3) return a.year - b.year;
    return a.iso3.localeCompare(b.iso3);
  });

  const globalSeries: SeriesPoint[] = [];
  const globalYears = Array.from(globalTotals.keys()).sort((a, b) => a - b);
  for (const year of globalYears) {
    const totals = globalTotals.get(year)!;
    if (totals.population <= 0 || totals.expenditure <= 0) continue;
    const value = roundN(totals.expenditure / totals.population, DECIMALS);
    globalSeries.push({ year, value });
  }

  if (globalSeries.length === 0) {
    throw new Error("[milex] global series empty");
  }

  const percentGlobalSeries: SeriesPoint[] = [];
  const percentYears = Array.from(percentTotals.keys()).sort((a, b) => a - b);
  for (const year of percentYears) {
    const totals = percentTotals.get(year)!;
    if (totals.gdp <= 0 || totals.weightedPercentTimesGdp < 0) continue;
    const value = roundN(
      (totals.weightedPercentTimesGdp / totals.gdp) * 100,
      PERCENT_DECIMALS,
    );
    percentGlobalSeries.push({ year, value });
  }

  if (percentGlobalSeries.length === 0) {
    throw new Error("[milex] percent-of-GDP global series empty");
  }

  const values = globalSeries.map((point) => point.value);
  const mean = values.reduce((acc, value) => acc + value, 0) / values.length;
  const std = computeStd(values, mean);

  const firstYear = globalSeries[0].year;
  const lastYear = globalSeries[globalSeries.length - 1].year;

  console.log(
    `GAISUM military_expenditure_per_capita_global {rows:${globalSeries.length}, min_year:${firstYear}, max_year:${lastYear}, mean:${mean.toFixed(DECIMALS)}, std:${std.toFixed(DECIMALS)}}`,
  );
  if (perCountryOutput.length > 0) {
    const countryYears = perCountryOutput.map((point) => point.year);
    const countryFirstYear = Math.min(...countryYears);
    const countryLastYear = Math.max(...countryYears);
    console.log(
      `GAISUM military_expenditure_per_capita_by_country {rows:${perCountryOutput.length}, min_year:${countryFirstYear}, max_year:${countryLastYear}}`,
    );
  }

  const percentValues = percentGlobalSeries.map((point) => point.value);
  const percentMean =
    percentValues.reduce((acc, value) => acc + value, 0) / percentValues.length;
  const percentStd = computeStd(percentValues, percentMean);
  const percentFirstYear = percentGlobalSeries[0].year;
  const percentLastYear =
    percentGlobalSeries[percentGlobalSeries.length - 1].year;
  console.log(
    `GAISUM military_expenditure_percent_gdp_global {rows:${percentGlobalSeries.length}, min_year:${percentFirstYear}, max_year:${percentLastYear}, mean:${percentMean.toFixed(PERCENT_DECIMALS)}, std:${percentStd.toFixed(PERCENT_DECIMALS)}}`,
  );

  const percentCountryOutput: Array<{
    iso3: string;
    country: string;
    year: number;
    value: number;
  }> = [];
  for (const [iso3, points] of percentCountrySeries) {
    points.sort((a, b) => a.year - b.year);
    for (const point of points) {
      percentCountryOutput.push({
        iso3,
        country: percentCountryNames.get(iso3) ?? iso3,
        year: point.year,
        value: point.value,
      });
    }
  }
  percentCountryOutput.sort((a, b) =>
    a.iso3 === b.iso3 ? a.year - b.year : a.iso3.localeCompare(b.iso3),
  );
  if (percentCountryOutput.length > 0) {
    const years = percentCountryOutput.map((entry) => entry.year);
    const minYear = Math.min(...years);
    const maxYear = Math.max(...years);
    console.log(
      `GAISUM military_expenditure_percent_gdp_by_country {rows:${percentCountryOutput.length}, min_year:${minYear}, max_year:${maxYear}}`,
    );
  }

  await writeJson(OUTPUT_GLOBAL, globalSeries);
  await writeJson(OUTPUT_BY_COUNTRY, perCountryOutput);
  await writeJson(OUTPUT_PERCENT_GLOBAL, percentGlobalSeries);
  await writeJson(OUTPUT_PERCENT_BY_COUNTRY, percentCountryOutput);

  await upsertSource("sipri_milex", {
    name: "SIPRI Military Expenditure Database",
    publisher: "SIPRI",
    license: "CC-BY",
    url: "https://www.sipri.org/databases/milex",
    retrieved: RETRIEVED_ON,
  });

  return globalSeries;
}

if (import.meta.url === new URL(process.argv[1], "file://").href) {
  run().catch((err) => {
    console.error("[milex] fatal", err);
    process.exit(1);
  });
}
