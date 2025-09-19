import { writeJson } from "../lib/io.ts";
import { upsertSource } from "../lib/manifest.ts";

type WdiRow = {
  countryiso3code?: string;
  country?: { id?: string };
  date?: string;
  value?: number | string | null;
};

const POVERTY_INDICATOR = "SI.POV.DDAY";
const POPULATION_INDICATOR = "SP.POP.TOTL";
const EXCLUDE = new Set([
  "WLD",
  "HIC",
  "INX",
  "LIC",
  "LMC",
  "MIC",
  "UMC",
  "OED",
  "ARB",
  "EAP",
  "ECA",
  "ECS",
  "EUU",
  "LCN",
  "LAC",
  "MEA",
  "NAC",
  "SAS",
  "SSA",
  "FCS",
]);
const COVERAGE_MIN = 0.8;

function isoFrom(row: WdiRow): string | null {
  const raw = (row.countryiso3code || row.country?.id || "").toUpperCase();
  if (!raw || !/^[A-Z]{3}$/.test(raw) || EXCLUDE.has(raw)) return null;
  return raw;
}

async function fetchWdiAll(indicator: string): Promise<WdiRow[]> {
  const base =
    `https://api.worldbank.org/v2/country/all/indicator/${indicator}` +
    "?format=json&per_page=20000";
  const firstRes = await fetch(`${base}&page=1`, {
    headers: { "User-Agent": "GAI-fetch-bot" },
  });
  if (!firstRes.ok) {
    const body = await firstRes.text().catch(() => "");
    console.error(
      `[extreme_poverty] ${indicator} page 1 HTTP ${firstRes.status} ${firstRes.statusText} :: ${body.slice(0, 200)}`,
    );
    throw new Error(`extreme_poverty: failed to fetch ${indicator} page 1`);
  }
  const first = await firstRes.json();
  if (!Array.isArray(first) || !Array.isArray(first[1])) {
    console.error("[extreme_poverty] unexpected payload", JSON.stringify(first).slice(0, 400));
    return [];
  }
  const pages = Number(first[0]?.pages ?? 1);
  const rows: WdiRow[] = [...first[1]];
  for (let page = 2; page <= pages; page++) {
    const res = await fetch(`${base}&page=${page}`, {
      headers: { "User-Agent": "GAI-fetch-bot" },
    });
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      console.error(
        `[extreme_poverty] ${indicator} page ${page} HTTP ${res.status} ${res.statusText} :: ${body.slice(0, 200)}`,
      );
      break;
    }
    const payload = await res.json();
    if (Array.isArray(payload) && Array.isArray(payload[1])) {
      rows.push(...payload[1]);
    }
  }
  return rows;
}

export async function run() {
  const [povertyRows, populationRows] = await Promise.all([
    fetchWdiAll(POVERTY_INDICATOR),
    fetchWdiAll(POPULATION_INDICATOR),
  ]);

  const poverty = new Map<string, Map<number, number>>();
  for (const row of povertyRows) {
    const iso = isoFrom(row);
    const year = Number(row.date);
    const value = row.value == null ? NaN : Number(row.value);
    if (!iso || !Number.isInteger(year) || Number.isNaN(value)) continue;
    if (!poverty.has(iso)) poverty.set(iso, new Map());
    poverty.get(iso)!.set(year, value);
  }

  const population = new Map<string, Map<number, number>>();
  for (const row of populationRows) {
    const iso = isoFrom(row);
    const year = Number(row.date);
    const value = row.value == null ? NaN : Number(row.value);
    if (!iso || !Number.isInteger(year) || Number.isNaN(value)) continue;
    if (!population.has(iso)) population.set(iso, new Map());
    population.get(iso)!.set(year, value);
  }

  if (population.size === 0) {
    throw new Error("extreme_poverty: population map empty");
  }

  const isoWithBoth: string[] = [];
  for (const iso of poverty.keys()) {
    const popYears = population.get(iso);
    if (!popYears) continue;
    const hasOverlap = Array.from(poverty.get(iso)!.keys()).some((year) => popYears.has(year));
    if (hasOverlap) isoWithBoth.push(iso);
  }

  const yearSet = new Set<number>();
  for (const iso of isoWithBoth) {
    const povYears = poverty.get(iso)!;
    const popYears = population.get(iso)!;
    for (const year of povYears.keys()) {
      if (popYears.has(year)) yearSet.add(year);
    }
  }
  const years = Array.from(yearSet).sort((a, b) => a - b);

  const populationIsos = Array.from(population.keys());
  const data: { year: number; value: number }[] = [];
  for (const year of years) {
    if (year < 1981) continue;
    let totalPopulation = 0;
    let usedPopulation = 0;
    let weighted = 0;
    for (const iso of populationIsos) {
      const popYears = population.get(iso);
      const popVal = popYears?.get(year);
      if (popVal == null || Number.isNaN(popVal)) continue;
      totalPopulation += popVal;
      const povVal = poverty.get(iso)?.get(year);
      if (povVal == null || Number.isNaN(povVal)) continue;
      usedPopulation += popVal;
      weighted += popVal * povVal;
    }
    if (totalPopulation <= 0) continue;
    const coverage = usedPopulation / totalPopulation;
    if (coverage < COVERAGE_MIN || usedPopulation <= 0) continue;
    const mean = weighted / usedPopulation;
    data.push({ year, value: Math.round(mean * 100) / 100 });
  }

  if (data.length === 0) {
    throw new Error("extreme_poverty: computed points empty");
  }

  const firstYear = data[0].year;
  const lastYear = data[data.length - 1].year;
  const candidateFirst = years[0] ?? "n/a";
  const candidateLast = years.length ? years[years.length - 1] : "n/a";
  console.log(
    `[extreme_poverty] raw rows: ${povertyRows.length} / population rows: ${populationRows.length} / ISO3 count: ${isoWithBoth.length} / years count: ${years.length} / computed points: ${data.length} range ${candidateFirst}-${candidateLast} / coverage_min: ${COVERAGE_MIN} / kept: ${firstYear}-${lastYear}`,
  );

  await writeJson("public/data/extreme_poverty.json", data);
  await upsertSource("extreme_poverty", {
    name: "Extreme poverty ($2.15)",
    domain: "Economics & Poverty",
    unit: "% of population",
    source_org: "World Bank (PovcalNet via WDI)",
    source_url: "https://data.worldbank.org/indicator/SI.POV.DDAY",
    license: "CC BY 4.0",
    cadence: "annual",
    method:
      "Pop-weighted global mean from national SI.POV.DDAY using SP.POP.TOTL; exclude aggregates; round 2 decimals; WDI may include modeled/nowcasted values.",
    updated_at: new Date().toISOString().slice(0, 10),
    data_start_year: firstYear,
  });

  return data;
}

const isEntry = import.meta.url === new URL(process.argv[1], "file://").href;
if (isEntry) {
  run().catch((err) => {
    console.error("[extreme_poverty] fatal:", err?.message || err);
    process.exit(1);
  });
}
