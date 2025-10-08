/**
 * Pipeline: Global population-weighted intentional homicide rate (per 100,000)
 * Sources:
 *   - data/raw/homicide_unodc_who.csv — World Bank WDI VC.IHR.PSRC.P5 (UNODC/WHO)
 *       SHA256 ec4b33a74b85f0da1eb5211075b9ac5e5a244bd5185e43ba182e69f47edc8f60
 *   - data/raw/pop_by_country.csv — World Bank WDI SP.POP.TOTL
 *       SHA256 247af97455f864f515432d1e3e36956294520547f00d1d7d3f23ffbc33d29b40
 * Method:
 *   - Normalize ISO3, exclude World Bank aggregate pseudo-codes
 *   - Resolve duplicate country-year rates preferring UNODC-coded values
 *   - Compute annual population-weighted mean where population coverage >= 0
 *   - Warn if population coverage <95% or values outside [0,40]
 *   - Emit rounded values (3 decimals) to public/data/homicide_rate.json
 */

import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { writeJson } from "./lib/io.ts";

const HOMICIDE_PATH = resolve(process.cwd(), "data/raw/homicide_unodc_who.csv");
const POP_PATH = resolve(process.cwd(), "data/raw/pop_by_country.csv");
const OUTPUT_PATH = resolve(process.cwd(), "public/data/homicide_rate.json");
const START_YEAR = 1990;
const WARN_MAX = 40;

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
  const lines = normalized.split("\n").filter((line) => line.trim().length > 0);
  if (!lines.length) {
    return { headers: [], rows: [] };
  }
  if (lines[0][0] === "\ufeff") {
    lines[0] = lines[0].slice(1);
  }
  const headers = parseCsvLine(lines[0]).map((header) => header.trim());
  const rows = lines.slice(1).map((line) => parseCsvLine(line));
  return { headers, rows };
}

function roundN(value: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function isCountryIso(code: string): boolean {
  return /^[A-Z]{3}$/.test(code) && code !== ISO_WLD && !AGGREGATE_ISO3.has(code);
}

async function loadHomicideRates(): Promise<{
  countryRates: Map<string, number>;
  wldByYear: Map<number, number>;
}> {
  const text = await readFile(HOMICIDE_PATH, "utf8");
  const { headers, rows } = parseCsv(text);
  if (!headers.length) {
    throw new Error("[homicide] homicide CSV missing headers");
  }
  const idxIso = headers.indexOf("iso3");
  const idxYear = headers.indexOf("year");
  const idxRate = headers.indexOf("rate_per_100k");
  const idxSource = headers.indexOf("source");
  if (idxIso === -1 || idxYear === -1 || idxRate === -1) {
    throw new Error("[homicide] homicide CSV missing required columns");
  }
  const data = new Map<string, { rate: number; source: string }>();
  const wldByYear = new Map<number, number>();
  for (const row of rows) {
    const iso = (row[idxIso] ?? "").trim().toUpperCase();
    const year = Number(row[idxYear]);
    const rate = Number(row[idxRate]);
    const source = (idxSource === -1 ? "" : row[idxSource] ?? "").trim();
    if (iso === ISO_WLD) {
      if (Number.isInteger(year) && year >= START_YEAR && Number.isFinite(rate) && rate >= 0) {
        wldByYear.set(year, rate);
      }
      continue;
    }
    if (!isCountryIso(iso)) continue;
    if (!Number.isFinite(year) || !Number.isFinite(rate)) continue;
    if (!Number.isInteger(year) || year < START_YEAR) continue;
    if (rate < 0) {
      console.warn(`[homicide] WARN negative rate skipped ${iso} ${year}=${rate}`);
      continue;
    }
    const key = `${iso}:${year}`;
    const existing = data.get(key);
    if (existing) {
      const preferNew = source.toLowerCase().includes("unodc") && !existing.source.toLowerCase().includes("unodc");
      if (preferNew) {
        data.set(key, { rate, source });
      } else if (Math.abs(existing.rate - rate) > 1e-6) {
        const avg = (existing.rate + rate) / 2;
        console.warn(
          `[homicide] duplicate ${iso} ${year} resolved by mean ${(avg).toFixed(4)} from ${existing.rate} and ${rate}`,
        );
        data.set(key, { rate: avg, source: existing.source });
      }
    } else {
      data.set(key, { rate, source });
    }
  }
  const result = new Map<string, number>();
  for (const [key, value] of data) {
    result.set(key, value.rate);
  }
  return { countryRates: result, wldByYear };
}

async function loadPopulations(): Promise<Map<string, number>> {
  const text = await readFile(POP_PATH, "utf8");
  const { headers, rows } = parseCsv(text);
  if (!headers.length) {
    throw new Error("[homicide] population CSV missing headers");
  }
  const idxIso = headers.indexOf("iso3");
  const idxYear = headers.indexOf("year");
  const idxPop = headers.indexOf("population");
  if (idxIso === -1 || idxYear === -1 || idxPop === -1) {
    throw new Error("[homicide] population CSV missing required columns");
  }
  const data = new Map<string, number>();
  for (const row of rows) {
    const iso = (row[idxIso] ?? "").trim().toUpperCase();
    const year = Number(row[idxYear]);
    const population = Number(row[idxPop]);
    if (iso === ISO_WLD) continue;
    if (!isCountryIso(iso)) continue;
    if (!Number.isInteger(year) || year < START_YEAR) continue;
    if (!Number.isFinite(population) || population <= 0) continue;
    const key = `${iso}:${year}`;
    if (data.has(key)) {
      const existing = data.get(key)!;
      if (existing !== population) {
        console.warn(
          `[homicide] duplicate population ${iso} ${year}, keeping larger ${Math.max(existing, population)}`,
        );
        data.set(key, Math.max(existing, population));
      }
    } else {
      data.set(key, population);
    }
  }
  return data;
}

async function run() {
  console.log("[homicide] loading raw datasets");
  const [{ countryRates, wldByYear }, populations] = await Promise.all([
    loadHomicideRates(),
    loadPopulations(),
  ]);
  console.log(`[homicide] homicide points: ${countryRates.size}`);
  console.log(`[homicide] population points: ${populations.size}`);

  const years = new Set<number>();
  for (const key of populations.keys()) {
    const year = Number(key.split(":")[1]);
    if (Number.isInteger(year)) years.add(year);
  }
  const sortedYears = Array.from(years).sort((a, b) => a - b);

  const computedByYear = new Map<number, { value: number; coverage: number }>();
  const coverageWarnings: { year: number; coverage: number }[] = [];

  for (const year of sortedYears) {
    let totalPop = 0;
    let coveredPop = 0;
    let weightedSum = 0;
    for (const [key, pop] of populations) {
      const [, yStr] = key.split(":");
      const y = Number(yStr);
      if (y !== year) continue;
      totalPop += pop;
      const rate = countryRates.get(key);
      if (rate == null || !Number.isFinite(rate)) continue;
      coveredPop += pop;
      weightedSum += rate * pop;
    }
    if (totalPop <= 0) {
      continue;
    }
    if (coveredPop <= 0) {
      coverageWarnings.push({ year, coverage: 0 });
      continue;
    }
    const coverage = coveredPop / totalPop;
    if (coverage < 0.95) {
      coverageWarnings.push({ year, coverage });
      console.warn(
        `[homicide] WARN population coverage ${(coverage * 100).toFixed(2)}% in ${year} (${coveredPop.toLocaleString()} / ${totalPop.toLocaleString()})`,
      );
    }
    const globalRate = weightedSum / coveredPop;
    if (globalRate < 0 || globalRate > WARN_MAX) {
      console.warn(`[homicide] WARN extreme value ${globalRate.toFixed(3)} in ${year}`);
    }
    computedByYear.set(year, { value: globalRate, coverage });
  }

  const yearSet = new Set<number>([
    ...computedByYear.keys(),
    ...wldByYear.keys(),
  ]);
  const hybridYears = Array.from(yearSet).sort((a, b) => a - b);

  if (!hybridYears.length) {
    throw new Error("[homicide] no output years available");
  }

  const output: { year: number; value: number }[] = [];
  let minValue = Number.POSITIVE_INFINITY;
  let maxValue = Number.NEGATIVE_INFINITY;

  for (const year of hybridYears) {
    const computed = computedByYear.get(year);
    const wld = wldByYear.get(year);
    if (computed && computed.coverage >= 0.95) {
      const rounded = roundN(computed.value, 3);
      output.push({ year, value: rounded });
      if (rounded < minValue) minValue = rounded;
      if (rounded > maxValue) maxValue = rounded;
      if (typeof wld === "number") {
        const diff = Math.abs(computed.value - wld);
        if (diff > 1.5) {
          console.warn(`DIFF_GT_1p5 year=${year} diff=${diff.toFixed(3)}`);
        }
      }
      continue;
    }
    if (typeof wld === "number") {
      const computedStr = computed ? computed.value.toFixed(3) : "NA";
      console.log(
        `FALLBACK_WLD year=${year} computed=${computedStr} wdi=${wld.toFixed(3)}`,
      );
      const rounded = roundN(wld, 3);
      output.push({ year, value: rounded });
      if (rounded < minValue) minValue = rounded;
      if (rounded > maxValue) maxValue = rounded;
      continue;
    }
    if (computed) {
      const rounded = roundN(computed.value, 3);
      console.warn(
        `[homicide] WARN fallback missing WLD for ${year}, using computed value with coverage ${(computed.coverage * 100).toFixed(2)}%`,
      );
      output.push({ year, value: rounded });
      if (rounded < minValue) minValue = rounded;
      if (rounded > maxValue) maxValue = rounded;
      continue;
    }
    throw new Error(`No bottom-up or WLD value available for year ${year}`);
  }

  output.sort((a, b) => a.year - b.year);

  const firstYear = output[0].year;
  const lastYear = output[output.length - 1].year;

  console.log(
    `GAISUM homicide_rate — rows=${output.length}; min_year=${firstYear}; max_year=${lastYear}; min=${minValue.toFixed(3)}; max=${maxValue.toFixed(3)}`,
  );
  if (coverageWarnings.length === 0) {
    console.log("[homicide] coverage >=95% for all years");
  } else {
    console.log(
      `[homicide] coverage shortfall years: ${coverageWarnings
        .map((w) => `${w.year}:${(w.coverage * 100).toFixed(2)}%`)
        .join(", ")}`,
    );
  }

  await writeJson(OUTPUT_PATH, output);
  console.log(`[homicide] wrote ${OUTPUT_PATH}`);
  console.log("[homicide] first 3 points", output.slice(0, 3));
  console.log("[homicide] last 3 points", output.slice(-3));
}

const isMain = import.meta.url === new URL(process.argv[1], "file://").href;
if (isMain) {
  run().catch((err) => {
    console.error("[homicide] fatal", err);
    process.exit(1);
  });
}

export { run };
