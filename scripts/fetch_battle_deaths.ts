import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { writeJson } from "./lib/io.ts";
import { upsertSource } from "./lib/manifest.ts";

const UCDP_URL =
  "https://ucdp.uu.se/downloads/battle-related-deaths/ucdp-brd-conflict-251.csv";
// Source pinned: UCDP Battle-Related Deaths Dataset v25.1 (released 2025-06-24)
const LOCAL_UCDP_PATH = resolve(process.cwd(), "data/raw/ucdp-brd-conflict-241.csv");
const POPULATION_URL =
  "https://api.worldbank.org/v2/country/WLD/indicator/SP.POP.TOTL?format=json&per_page=600";
// Population: World Bank WDI SP.POP.TOTL (global population, CC BY 4.0)

const START_YEAR = 1990;
const YEAR_MIN = 1900;
const YEAR_MAX = 2100;
const TOLERANCE = 0.02;

const TYPE_MAP = {
  "1": "interstate",
  "2": "intrastate",
  "3": "internationalized_intrastate",
  "4": "extrasystemic",
} as const;

const TYPE_ORDER = [
  "interstate",
  "intrastate",
  "internationalized_intrastate",
  "extrasystemic",
] as const;

type ConflictType = (typeof TYPE_ORDER)[number];

type CsvRow = Record<string, string>;

type SeriesPoint = { year: number; value: number };
type SeriesByTypePoint = { year: number; type: ConflictType; value: number };

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
    } else {
      if (char === "\"") {
        inQuotes = true;
      } else if (char === ",") {
        result.push(current.trim());
        current = "";
      } else {
        current += char;
      }
    }
  }
  result.push(current.trim());
  return result;
}

function parseCsv(text: string): { headers: string[]; rows: CsvRow[] } {
  const normalized = text.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n").filter((line) => line.trim().length > 0);
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
    headers.forEach((header, idx) => {
      row[header] = cells[idx] ?? "";
    });
    rows.push(row);
  }
  return { headers, rows };
}

function roundN(value: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

async function fetchText(
  url: string,
  label: string,
  expected: "csv" | "json" = "csv",
): Promise<string> {
  const accept = expected === "json" ? "application/json" : "text/csv";
  const response = await fetch(url, {
    headers: {
      "User-Agent": "GAI-battle-deaths-pipeline/1.0",
      Accept: accept,
    },
  });
  if (!response.ok) {
    const snippet = await response.text().catch(() => "");
    throw new Error(
      `[battle-deaths] ${label} fetch failed ${response.status} ${response.statusText}: ${snippet.slice(0, 200)}`,
    );
  }
  const ct = response.headers.get("content-type") ?? "";
  const text = await response.text();
  const lowered = ct.toLowerCase();
  if (expected === "csv") {
    if (!lowered.includes("text/csv")) {
      const snippet = text.slice(0, 200);
      throw new Error(
        `[battle-deaths] expected CSV, got ${ct || "unknown"}: ${snippet}`,
      );
    }
  } else {
    if (!lowered.includes("application/json")) {
      const snippet = text.slice(0, 200);
      throw new Error(
        `[battle-deaths] expected JSON, got ${ct || "unknown"}: ${snippet}`,
      );
    }
  }
  return text;
}

async function loadPopulation(): Promise<Map<number, number>> {
  const text = await fetchText(POPULATION_URL, "population", "json");
  const json = JSON.parse(text);
  if (!Array.isArray(json) || json.length < 2 || !Array.isArray(json[1])) {
    throw new Error("[battle-deaths] population payload missing rows");
  }
  const rows = json[1] as any[];
  const pop = new Map<number, number>();
  for (const row of rows) {
    const year = Number(row?.date);
    const value = Number(row?.value);
    if (!Number.isInteger(year) || year < YEAR_MIN || year > YEAR_MAX) continue;
    if (!Number.isFinite(value) || value <= 0) continue;
    pop.set(year, value);
  }
  if (pop.size === 0) {
    throw new Error("[battle-deaths] population series empty");
  }
  return pop;
}

function selectDeathColumn(headers: string[]): string {
  const lowerHeaders = headers.map((header) => header.toLowerCase());
  const findHeader = (key: string): string | undefined => {
    const idx = lowerHeaders.indexOf(key);
    return idx === -1 ? undefined : headers[idx];
  };

  const candidates = [
    "bd_best",
    "best",
    "best_estimate",
    "deaths_b",
    "deaths_best",
    "fatality_best",
  ];
  for (const key of candidates) {
    const header = findHeader(key);
    if (header) return header;
  }

  const triplet = ["deaths_a", "deaths_b", "deaths_c"]; // fallback set
  if (triplet.every((key) => findHeader(key))) {
    const header = findHeader("deaths_b");
    if (header) return header;
  }

  console.error("[battle-deaths] headers", headers);
  throw new Error(
    `[battle-deaths] missing death column; expected one of ${candidates.join(", ")}`,
  );
}

async function loadUcdpCsv(): Promise<string> {
  if (existsSync(LOCAL_UCDP_PATH)) {
    console.log(`[battle-deaths] using local CSV ${LOCAL_UCDP_PATH}`);
    return readFile(LOCAL_UCDP_PATH, "utf8");
  }
  console.log("[battle-deaths] fetch UCDP CSV", UCDP_URL);
  return fetchText(UCDP_URL, "ucdp", "csv");
}

async function run(): Promise<void> {
  const csvText = await loadUcdpCsv();
  const { headers, rows } = parseCsv(csvText);
  console.log("[battle-deaths] rows fetched", rows.length);
  if (!rows.length) {
    throw new Error("[battle-deaths] no rows parsed from UCDP");
  }

  const deathColumn = selectDeathColumn(headers);
  console.log("[battle-deaths] using death column", deathColumn);

  const deathsByYearType = new Map<number, Map<ConflictType, number>>();

  for (const row of rows) {
    const yearRaw = row["year"]?.trim();
    if (!yearRaw) continue;
    const year = Number(yearRaw);
    if (!Number.isInteger(year) || year < YEAR_MIN || year > YEAR_MAX) {
      throw new Error(`[battle-deaths] invalid year: ${yearRaw}`);
    }

    const typeCode = row["type_of_conflict"]?.trim();
    const type = (TYPE_MAP as Record<string, ConflictType | undefined>)[typeCode ?? ""];
    if (!type) {
      throw new Error(`[battle-deaths] unknown conflict type code: ${typeCode}`);
    }

    const raw = row[deathColumn]?.trim();
    const deaths = raw === "" ? 0 : Number(raw);
    if (!Number.isFinite(deaths)) {
      throw new Error(`[battle-deaths] non-numeric death value: ${raw}`);
    }
    if (deaths < 0) {
      throw new Error(`[battle-deaths] negative deaths for year ${year}`);
    }

    const typeMap = deathsByYearType.get(year) ?? new Map<ConflictType, number>();
    typeMap.set(type, (typeMap.get(type) ?? 0) + deaths);
    deathsByYearType.set(year, typeMap);
  }

  const popSeries = await loadPopulation();

  const availableYears = Array.from(deathsByYearType.keys())
    .filter((y) => y >= START_YEAR)
    .sort((a, b) => a - b);
  if (!availableYears.length) {
    throw new Error(`[battle-deaths] no UCDP years >= ${START_YEAR}`);
  }

  const populationYears = Array.from(popSeries.keys())
    .filter((y) => y >= START_YEAR)
    .sort((a, b) => a - b);
  if (!populationYears.length) {
    throw new Error(`[battle-deaths] no population years >= ${START_YEAR}`);
  }

  const maxCommonYear = Math.min(
    availableYears[availableYears.length - 1],
    populationYears[populationYears.length - 1],
  );
  if (maxCommonYear < START_YEAR) {
    throw new Error("[battle-deaths] no overlapping years between UCDP and population series");
  }

  const droppedBattleYears = availableYears.filter((year) => year > maxCommonYear);
  if (droppedBattleYears.length) {
    console.warn(
      `[battle-deaths] WARN dropping battle-death years beyond population coverage: ${droppedBattleYears.join(", ")}`,
    );
  }
  const droppedPopYears = populationYears.filter((year) => year > maxCommonYear);
  if (droppedPopYears.length) {
    console.warn(
      `[battle-deaths] WARN dropping population years beyond UCDP coverage: ${droppedPopYears.join(", ")}`,
    );
  }

  const years: number[] = [];
  const missingBattle: number[] = [];
  const missingPop: number[] = [];
  for (let year = START_YEAR; year <= maxCommonYear; year++) {
    years.push(year);
    if (!deathsByYearType.has(year)) missingBattle.push(year);
    if (!popSeries.has(year)) missingPop.push(year);
  }

  if (missingBattle.length) {
    throw new Error(
      `[battle-deaths] missing UCDP years in range ${START_YEAR}-${maxCommonYear}: ${missingBattle.join(", ")}`,
    );
  }
  if (missingPop.length) {
    throw new Error(
      `[battle-deaths] missing population years in range ${START_YEAR}-${maxCommonYear}: ${missingPop.join(", ")}`,
    );
  }

  const byTypeSeries: SeriesByTypePoint[] = [];
  const totalSeries: SeriesPoint[] = [];
  const interstateSeries: SeriesPoint[] = [];

  for (const year of years) {
    const pop = popSeries.get(year);
    if (!pop || !Number.isFinite(pop) || pop <= 0) {
      throw new Error(`[battle-deaths] invalid population for year ${year}`);
    }

    const typeMap = deathsByYearType.get(year) ?? new Map<ConflictType, number>();
    for (const type of TYPE_ORDER) {
      if (!typeMap.has(type)) typeMap.set(type, 0);
    }

    let totalDeaths = 0;
    let sumRounded = 0;
    for (const type of TYPE_ORDER) {
      const deaths = typeMap.get(type) ?? 0;
      if (deaths < 0) {
        throw new Error(`[battle-deaths] negative deaths encountered for ${year} ${type}`);
      }
      totalDeaths += deaths;
      const value = roundN((deaths / pop) * 100_000, 3);
      if (value < 0) {
        throw new Error(`[battle-deaths] negative per-capita value for ${year} ${type}`);
      }
      sumRounded = roundN(sumRounded + value, 3);
      const point = { year, type, value } as const;
      byTypeSeries.push(point);
      if (type === "interstate") {
        interstateSeries.push({ year, value });
      }
    }

    const totalValue = roundN((totalDeaths / pop) * 100_000, 3);
    if (totalValue < 0) {
      throw new Error(`[battle-deaths] negative total per-capita value for ${year}`);
    }
    totalSeries.push({ year, value: totalValue });

    const diff = Math.abs(sumRounded - totalValue);
    if (diff > TOLERANCE + 1e-9) {
      throw new Error(
        `[battle-deaths] tolerance check failed for ${year}: |${sumRounded.toFixed(3)} - ${totalValue.toFixed(3)}| = ${diff.toFixed(
          3,
        )}`,
      );
    }
  }

  const valueCheck = (point: SeriesPoint | SeriesByTypePoint) => {
    if (point.value < 0) {
      throw new Error(`[battle-deaths] negative value after rounding for year ${point.year}`);
    }
  };
  totalSeries.forEach(valueCheck);
  interstateSeries.forEach(valueCheck);
  byTypeSeries.forEach(valueCheck);

  await writeJson("public/data/battle_deaths_total.json", totalSeries);
  await writeJson("public/data/battle_deaths_by_type.json", byTypeSeries);
  await writeJson("public/data/battle_deaths_interstate.json", interstateSeries);
  await writeJson("public/data/battle_deaths.json", interstateSeries);

  const gaSummary = (series: SeriesPoint[]) => {
    const values = series.map((d) => d.value);
    const min = Math.min(...values);
    const max = Math.max(...values);
    return { min, max };
  };

  const totalSummary = gaSummary(totalSeries);
  const interstateSummary = gaSummary(interstateSeries);

  if (
    process.env.ALLOW_PLACEHOLDERS !== "1" &&
    (totalSummary.max === 0 || interstateSummary.max === 0)
  ) {
    throw new Error("placeholder dataset (all zeros)");
  }

  console.log(
    `GAISUM battle_deaths_total {rows:${totalSeries.length}, min_year:${years[0]}, max_year:${years[years.length - 1]}, min:${totalSummary.min.toFixed(
      3,
    )}, max:${totalSummary.max.toFixed(3)}}`,
  );
  console.log(
    `GAISUM battle_deaths_by_type {rows:${byTypeSeries.length}, min_year:${years[0]}, max_year:${years[years.length - 1]}, types:[${TYPE_ORDER.join(
      ",",
    )}]}`,
  );
  console.log(
    `GAISUM battle_deaths_interstate {rows:${interstateSeries.length}, min_year:${years[0]}, max_year:${years[years.length - 1]}, min:${interstateSummary.min.toFixed(
      3,
    )}, max:${interstateSummary.max.toFixed(3)}}`,
  );

  const first3 = (arr: any[]) => JSON.stringify(arr.slice(0, 3), null, 2);
  const last3 = (arr: any[]) => JSON.stringify(arr.slice(-3), null, 2);

  console.log(`[battle-deaths] total first3 ${first3(totalSeries)}`);
  console.log(`[battle-deaths] total last3 ${last3(totalSeries)}`);
  console.log(`[battle-deaths] by-type first3 ${first3(byTypeSeries)}`);
  console.log(`[battle-deaths] by-type last3 ${last3(byTypeSeries)}`);
  console.log(`[battle-deaths] interstate first3 ${first3(interstateSeries)}`);
  console.log(`[battle-deaths] interstate last3 ${last3(interstateSeries)}`);
  console.log(
    `[battle-deaths] coverage ${START_YEAR}-${years[years.length - 1]} (${years.length} years, continuous)`,
  );

  await upsertSource("ucdp_battle_deaths", {
    name: "Battle-related deaths (global)",
    domain: "Safety & Conflict",
    unit: "deaths per 100k people",
    source_org: "Uppsala Conflict Data Program (UCDP)",
    source_url: UCDP_URL,
    license: "© UCDP, academic & non-commercial use permitted", // dataset terms summary
    cadence: "annual",
    method:
      "Aggregate UCDP battle-related deaths by type_of_conflict using bd_best/best/best_estimate; join World Bank SP.POP.TOTL population; compute per 100k; enforce 1990-latest continuous coverage.",
    updated_at: new Date().toISOString().slice(0, 10),
    data_start_year: years[0],
    notes:
      "Conflict-type mapping: 1→interstate, 2→intrastate, 3→internationalized_intrastate, 4→extrasystemic. Population denominator: World Bank WDI SP.POP.TOTL.",
  });
}

const isEntry = import.meta.url === new URL(process.argv[1], "file://").href;
if (isEntry) {
  run().catch((err) => {
    console.error("[battle-deaths] fatal", err);
    process.exit(1);
  });
}

export { run };
