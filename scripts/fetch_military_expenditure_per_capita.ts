import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { writeJson } from "./lib/io.ts";

interface YearValue {
  year: number;
  value: number;
}

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
      cells.push(current);
      current = "";
    } else {
      current += char;
    }
  }
  cells.push(current);
  return cells;
}

function parseCsv(text: string): { headers: string[]; rows: string[][] } {
  const normalized = text.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n");
  const filtered: string[] = [];
  for (const raw of lines) {
    const line = raw.trimEnd();
    if (!line) continue;
    filtered.push(line);
  }
  if (!filtered.length) {
    return { headers: [], rows: [] };
  }
  if (filtered[0][0] === "\ufeff") {
    filtered[0] = filtered[0].slice(1);
  }
  const headers = parseCsvLine(filtered[0]).map((header) => header.trim());
  const rows = filtered.slice(1).map((line) => parseCsvLine(line));
  return { headers, rows };
}

function toNumber(value: string | undefined): number | null {
  if (!value) return null;
  const clean = value.trim();
  if (!clean) return null;
  const num = Number(clean.replace(/[, ]/g, ""));
  return Number.isFinite(num) ? num : null;
}

function round1(value: number): number {
  return Math.round(value * 10) / 10;
}

const PROJECT_ROOT = process.cwd();
const SIPRI_PATH = resolve(PROJECT_ROOT, "data/raw/sipri_milex.csv");
const POP_PATH = resolve(PROJECT_ROOT, "data/raw/wb_population_total.csv");
const DEFLATOR_PATH = resolve(PROJECT_ROOT, "data/raw/wld_gdp_deflator.csv");
const OUTPUT_GLOBAL = resolve(
  PROJECT_ROOT,
  "public/data/military_expenditure_per_capita_constant_usd.json",
);
const OUTPUT_LOG = resolve(
  PROJECT_ROOT,
  "scripts/logs/military_expenditure_per_capita_constant_usd.gaisum.json",
);

async function loadWorldMilitaryExpenditure(): Promise<Map<number, number>> {
  const text = await readFile(SIPRI_PATH, "utf8");
  const normalized = text.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n");
  let headers: string[] | null = null;
  const values = new Map<number, number>();
  for (const rawLine of lines) {
    if (!rawLine) continue;
    const cells = parseCsvLine(rawLine);
    if (!cells.length) continue;
    if (!headers) {
      if (cells[0] === "Country Name") {
        headers = cells;
      }
      continue;
    }
    const iso3 = (cells[1] ?? "").trim();
    if (iso3 !== "WLD") {
      continue;
    }
    for (let i = 4; i < headers.length; i++) {
      const yearStr = headers[i];
      if (!yearStr) continue;
      const year = Number(yearStr);
      if (!Number.isInteger(year)) continue;
      const exp = toNumber(cells[i]);
      if (exp && exp > 0) {
        values.set(year, exp);
      }
    }
    break;
  }
  if (!values.size) {
    throw new Error("[milex] world military expenditure row missing");
  }
  return values;
}

async function loadWorldPopulation(): Promise<Map<number, number>> {
  const text = await readFile(POP_PATH, "utf8");
  const normalized = text.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n");
  let headers: string[] | null = null;
  const map = new Map<number, number>();
  for (const rawLine of lines) {
    if (!rawLine) continue;
    const cells = parseCsvLine(rawLine);
    if (!cells.length) continue;
    if (!headers) {
      if (cells[0] === "Country Name") {
        headers = cells;
      }
      continue;
    }
    const countryName = (cells[0] ?? "").trim();
    if (!countryName) continue;
    if (countryName !== "World") {
      continue;
    }
    for (let i = 4; i < headers.length; i++) {
      const yearStr = headers[i];
      if (!yearStr) continue;
      const year = Number(yearStr);
      if (!Number.isInteger(year)) continue;
      const pop = toNumber(cells[i]);
      if (pop && pop > 0) {
        map.set(year, pop);
      }
    }
    break;
  }
  if (!map.size) {
    throw new Error("[milex] world population row missing");
  }
  return map;
}

async function loadDeflator(): Promise<{ baseIndex: number; indexByYear: Map<number, number> }> {
  const text = await readFile(DEFLATOR_PATH, "utf8");
  const { headers, rows } = parseCsv(text);
  if (!headers.length) {
    throw new Error("[milex] deflator CSV missing headers");
  }
  const idxYear = headers.indexOf("year");
  const idxIndex = headers.indexOf("index");
  if (idxYear === -1 || idxIndex === -1) {
    throw new Error("[milex] deflator CSV missing year/index columns");
  }
  const indexByYear = new Map<number, number>();
  for (const row of rows) {
    const year = Number(row[idxYear]);
    const index = toNumber(row[idxIndex]);
    if (Number.isInteger(year) && index && index > 0) {
      indexByYear.set(year, index);
    }
  }
  const baseIndex = indexByYear.get(2020);
  if (!baseIndex) {
    throw new Error("[milex] missing GDP deflator value for 2020");
  }
  return { baseIndex, indexByYear };
}

function assertAscending(series: YearValue[]): void {
  for (let i = 1; i < series.length; i++) {
    if (series[i].year <= series[i - 1].year) {
      throw new Error(
        `[milex] years not strictly ascending at index ${i}: ${series[i - 1].year} -> ${series[i].year}`,
      );
    }
  }
}

async function run(): Promise<void> {
  console.log("[milex] loading SIPRI/WDI dataset");
  const worldTotals = await loadWorldMilitaryExpenditure();
  console.log(`[milex] world total years: ${worldTotals.size}`);

  console.log("[milex] loading world population");
  const population = await loadWorldPopulation();
  console.log(`[milex] population years: ${population.size}`);

  console.log("[milex] loading GDP deflator");
  const { baseIndex, indexByYear } = await loadDeflator();
  console.log(`[milex] deflator base (2020): ${baseIndex.toFixed(3)}`);

  const series: YearValue[] = [];
  let missingPopulationYears = 0;
  let missingDeflatorYears = 0;
  let minValue = Number.POSITIVE_INFINITY;
  let maxValue = Number.NEGATIVE_INFINITY;

  const sortedYears = Array.from(worldTotals.keys()).sort((a, b) => a - b);

  for (const year of sortedYears) {
    const total = worldTotals.get(year);
    if (typeof total !== "number") {
      continue;
    }
    const pop = population.get(year);
    if (!pop) {
      missingPopulationYears++;
      continue;
    }
    const deflator = indexByYear.get(year);
    if (!deflator) {
      missingDeflatorYears++;
      continue;
    }
    const perCapitaCurrent = total / pop;
    const perCapita2020 = perCapitaCurrent * (baseIndex / deflator);
    const rounded = round1(perCapita2020);
    series.push({ year, value: rounded });
    if (rounded < minValue) minValue = rounded;
    if (rounded > maxValue) maxValue = rounded;
  }

  series.sort((a, b) => a.year - b.year);
  assertAscending(series);

  if (!series.length) {
    throw new Error("[milex] no output rows computed");
  }

  const firstYear = series[0].year;
  const lastYear = series[series.length - 1].year;
  const coverage = ((series.length / worldTotals.size) * 100).toFixed(2);

  if (missingPopulationYears > 0) {
    console.warn(
      `[milex] WARN missing population for ${missingPopulationYears} world rows; coverage ${coverage}%`,
    );
  }
  if (missingDeflatorYears > 0) {
    console.warn(`[milex] WARN missing deflator for ${missingDeflatorYears} world rows`);
  }

  const gaisum = {
    id: "military_expenditure_per_capita_constant_usd",
    rows: series.length,
    min_year: firstYear,
    max_year: lastYear,
    min_value: minValue,
    max_value: maxValue,
    rebased_from: 2019,
    rebased_to: 2020,
  };

  console.log(
    `GAISUM military_expenditure_per_capita_constant_usd → rows=${series.length}; min_year=${firstYear}; max_year=${lastYear}; min=${minValue.toFixed(1)}; max=${maxValue.toFixed(1)}`,
  );
  console.log(`[milex] coverage (world rows with population): ${coverage}%`);
  console.log(`[milex] sample first rows`, series.slice(0, 3));
  console.log(`[milex] sample last rows`, series.slice(-5));

  await writeJson(OUTPUT_GLOBAL, series);
  console.log(`[milex] wrote ${OUTPUT_GLOBAL}`);

  await writeJson(OUTPUT_LOG, gaisum);
  console.log(`[milex] wrote GAISUM log ${OUTPUT_LOG}`);
}

const isMain = import.meta.url === new URL(process.argv[1], "file://").href;
if (isMain) {
  run().catch((err) => {
    console.error("[milex] fatal", err);
    process.exit(1);
  });
}

export { run };
