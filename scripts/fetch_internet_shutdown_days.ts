import {
  access,
  mkdtemp,
  mkdir,
  readdir,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { execFile } from "node:child_process";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { promisify } from "node:util";
import { constants as fsConstants } from "node:fs";

import { unzipSync } from "fflate";

import { writeJson } from "./lib/io.ts";
import { upsertSource } from "./lib/manifest.ts";

const execFileAsync = promisify(execFile);

const PROJECT_ROOT = process.cwd();
const STOP_CACHE_DIR = resolve(PROJECT_ROOT, "data/raw/keepiton");
const POPULATION_PATH = resolve(PROJECT_ROOT, "data/raw/pop_by_country.csv");
const OUTPUT_SERIES_PATH = resolve(PROJECT_ROOT, "public/data/internet_shutdown_days.json");
const GAISUM_LOG_PATH = resolve(PROJECT_ROOT, "scripts/logs/internet_shutdown_days.gaisum.json");

const METRIC_ID = "internet_shutdown_days";
const STOP_SOURCE_ID = "accessnow_keepiton_stop";
const POP_SOURCE_ID = "wdi_sp_pop_totl";
const START_YEAR = 2016;
const ISO_WORLD = "WLD";
const MS_PER_DAY = 86_400_000;

const STOP_DEFAULT_URL =
  process.env.STOP_BUNDLE_URL ??
  "https://stop.accessnow.org/wp-content/uploads/keepiton/STOP_2016-2024.csv";
const STOP_YEAR_URL_TEMPLATE = process.env.STOP_YEAR_URL_TEMPLATE ?? "";
const STOP_FIXTURE_PATH = process.env.STOP_FIXTURE_PATH ?? "";

const STOP_ALLOWED_EXTENSIONS = new Set([".csv", ".tsv", ".txt"]);

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

const STOP_ALIAS_TO_ISO: Record<string, string> = {
  AFGHANISTAN: "AFG",
  "BOLIVIA(BOLIVARIANREPUBLICOF)": "BOL",
  "BOLIVIA(BOLIVARIANREPUBLIC)": "BOL",
  "BOSNIAANDHERZEGOVINA": "BIH",
  "BRUNEIDARUSSALAM": "BRN",
  "CABOVERDE": "CPV",
  "CAPEVERDE": "CPV",
  "CONGO(BRAZZAVILLE)": "COG",
  "CONGO(KINSHASA)": "COD",
  "DEMOCRATICREPUBLICOFTHECONGO": "COD",
  "DEMOCRATICREPUBLICOFCONGO": "COD",
  "REPUBLICOFTHECONGO": "COG",
  "REPUBLICOFCONGO": "COG",
  "COTEDIVOIRE": "CIV",
  "IVORYCOAST": "CIV",
  "COTED'IVOIRE": "CIV",
  "COTEDIVOIRE(REPUBLICOF)": "CIV",
  "COTEIVOIRE": "CIV",
  "LAOPEOPLE'SDEMOCRATICREPUBLIC": "LAO",
  "LAOPEOPLEDEMOCRATICREPUBLIC": "LAO",
  "LAOS": "LAO",
  "MOLDOVA(REPUBLICOF)": "MDA",
  "MOLDOVA": "MDA",
  "MYANMAR(BURMA)": "MMR",
  "BURMA": "MMR",
  "RUSSIANFEDERATION": "RUS",
  "RUSSIA": "RUS",
  "SOUTHKOREA": "KOR",
  "NORTHKOREA": "PRK",
  "REPUBLICOFKOREA": "KOR",
  "DEMOCRATICPEOPLE'SREPUBLICOFKOREA": "PRK",
  "SYRIANARABREPUBLIC": "SYR",
  "SYRIA": "SYR",
  "UNITEDSTATES": "USA",
  "UNITEDSTATESOFAMERICA": "USA",
  "UNITEDKINGDOM": "GBR",
  "UNITEDKINGDOMOFGREATBRITAINANDNORTHERNIRELAND": "GBR",
  "GREATBRITAIN": "GBR",
  "UAE": "ARE",
  "UNITEDARABEMIRATES": "ARE",
  "TANZANIA,UNITEDREPUBLICOF": "TZA",
  "TANZANIA": "TZA",
  "TIMOR-LESTE": "TLS",
  "EASTTIMOR": "TLS",
  "VIETNAM": "VNM",
  "VENEZUELA(BOLIVARIANREPUBLICOF)": "VEN",
  "VENEZUELA": "VEN",
  "PALESTINE": "PSE",
  "STATEOFPALISTINE": "PSE",
  "STATEOFPLESTINE": "PSE",
  "WESTBANKANDGAZA": "PSE",
  "KOSOVO": "XKX",
  "SWAZILAND": "SWZ",
  "ESWATINI": "SWZ",
  "GAMBIA": "GMB",
  "THEGAMBIA": "GMB",
  "BAHAMAS": "BHS",
  "BAHAMAS,THE": "BHS",
  "GAMBIA,THE": "GMB",
  "IRAN(ISLAMICREPUBLICOF)": "IRN",
  "IRAN": "IRN",
  "BOLIVARIANREPUBLICOFVENEZUELA": "VEN",
  "KOREA,REPUBLICOF": "KOR",
  "KOREA,DEMPEOPLE'SREPUBLICOF": "PRK",
  "BOSNIA&HERZEGOVINA": "BIH",
  "SAOTOMEANDPRINCIPE": "STP",
  "SãOTOMEANDPRíNCIPE": "STP",
  "SãOTOMEANDPRINCIPE": "STP",
  "SAOTOME&PRINCIPE": "STP",
  "TRINIDADANDTOBAGO": "TTO",
  "ANTIGUAANDBARBUDA": "ATG",
  "ST.KITTSANDNEVIS": "KNA",
  "STKITTSANDNEVIS": "KNA",
  "ST.VINCENTANDTHEGRENADINES": "VCT",
  "STVINCENTANDTHEGRENADINES": "VCT",
  "BOSNIA-HERZEGOVINA": "BIH",
  "DOMINICANREPUBLIC": "DOM",
  "CENTRALAFRICANREPUBLIC": "CAF",
  "REPUBLICOFTHEPHILIPPINES": "PHL",
  "PHILIPPINES": "PHL",
  "SUDAN": "SDN",
  "SOUTHSUDAN": "SSD",
  "UNITEDREPUBLICOFTANZANIA": "TZA",
  "HONGKONG": "HKG",
  "HONGKONGSAR": "HKG",
  "MACAO": "MAC",
  "MACAOSAR": "MAC",
  "MACAU": "MAC",
  "MACAUSAR": "MAC",
  "REPUBLICOFMOLDOVA": "MDA",
  "CZECHREPUBLIC": "CZE",
  "SLOVAKREPUBLIC": "SVK",
  "SAINTLUCIA": "LCA",
};

type CsvRow = Record<string, string>;

type RawEvent = {
  country: string;
  iso3?: string;
  startDate: string;
  endDate: string;
  hasExplicitEndDate: boolean;
  startTime?: string;
  endTime?: string;
  scope?: string;
};

type NormalizedEvent = {
  iso3: string;
  country: string;
  scope?: string;
  startMs: number;
  endMs: number;
  approx: boolean;
};

type YearInterval = {
  iso3: string;
  country: string;
  year: number;
  startMs: number;
  endMs: number;
};

type PopulationData = {
  populationByIsoYear: Map<string, number>;
  totalPopulationByYear: Map<number, number>;
  canonicalNameToIso: Map<string, string>;
  isoToCountryName: Map<string, string>;
};

function canonicalizeHeader(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/_{2,}/g, "_").replace(/^_|_$/g, "");
}

function canonicalizeCountry(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/gi, "")
    .toUpperCase();
}

function parseCsv(text: string): { headers: string[]; rows: CsvRow[] } {
  const normalized = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const lines: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < normalized.length; i++) {
    const char = normalized[i];
    if (char === "\"") {
      if (inQuotes && normalized[i + 1] === "\"") {
        current += "\"";
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === "\n" && !inQuotes) {
      lines.push(current);
      current = "";
    } else {
      current += char;
    }
  }
  if (current.length > 0) {
    lines.push(current);
  }
  const filtered = lines.map((line) => line.trimEnd()).filter((line) => line.length > 0);
  if (!filtered.length) {
    return { headers: [], rows: [] };
  }
  if (filtered[0][0] === "\ufeff") {
    filtered[0] = filtered[0].slice(1);
  }
  const headers = parseCsvLine(filtered[0]).map((header) => header.trim());
  const rows: CsvRow[] = [];
  for (let i = 1; i < filtered.length; i++) {
    const cells = parseCsvLine(filtered[i]);
    const row: CsvRow = {};
    headers.forEach((header, idx) => {
      row[header] = cells[idx] ?? "";
    });
    rows.push(row);
  }
  return { headers, rows };
}

function parseCsvLine(line: string): string[] {
  const cells: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === "\"") {
      if (inQuotes && line[i + 1] === "\"") {
        current += "\"";
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      cells.push(current.trim());
      current = "";
    } else {
      current += char;
    }
  }
  cells.push(current.trim());
  return cells;
}

function findHeader(headers: string[], variants: string[], fuzzyIncludes?: string[]): string | undefined {
  const normalized = headers.map((header) => ({ raw: header, key: canonicalizeHeader(header) }));
  for (const variant of variants) {
    const match = normalized.find((header) => header.key === variant);
    if (match) return match.raw;
  }
  if (fuzzyIncludes && fuzzyIncludes.length) {
    for (const pattern of fuzzyIncludes) {
      const match = normalized.find((header) => header.key.includes(pattern));
      if (match) return match.raw;
    }
  }
  return undefined;
}

function parseStopRows(text: string): RawEvent[] {
  const { headers, rows } = parseCsv(text);
  if (!headers.length) {
    return [];
  }
  const countryHeader =
    findHeader(headers, ["country", "country_name", "affected_country", "territory"], ["country"]) ?? headers[0];
  const isoHeader = findHeader(headers, ["iso", "iso3", "country_iso", "iso_code"]);
  const startDateHeader =
    findHeader(headers, ["start_date", "start", "startdate", "date_start", "event_start_date"], ["start", "date"]);
  const endDateHeader =
    findHeader(headers, ["end_date", "end", "enddate", "date_end", "event_end_date"], ["end", "date"]);
  const startTimeHeader = findHeader(headers, ["start_time", "starttime", "time_start", "event_start_time"], ["start", "time"]);
  const endTimeHeader = findHeader(headers, ["end_time", "endtime", "time_end", "event_end_time"], ["end", "time"]);
  const scopeHeader = findHeader(headers, ["scope", "network_scope", "shutdown_scope", "type"], ["scope"]);

  if (!startDateHeader) {
    throw new Error("[internet-shutdown] STOP CSV missing start date column");
  }
  if (!endDateHeader) {
    console.warn("[internet-shutdown] WARN STOP CSV missing end date column; using start date as end date fallback");
  }

  const events: RawEvent[] = [];
  for (const row of rows) {
    const country = (row[countryHeader] ?? "").trim();
    if (!country) continue;
    const startDate = (row[startDateHeader] ?? "").trim();
    if (!startDate) continue;
    const rawEndDate = (endDateHeader ? row[endDateHeader] : "") ?? "";
    const endDate = rawEndDate.trim();
    const hasExplicitEndDate = endDate.length > 0;
    const startTime = startTimeHeader ? (row[startTimeHeader] ?? "").trim() : undefined;
    const endTime = endTimeHeader ? (row[endTimeHeader] ?? "").trim() : undefined;
    const scope = scopeHeader ? (row[scopeHeader] ?? "").trim() : undefined;
    events.push({
      country,
      iso3: isoHeader ? (row[isoHeader] ?? "").trim() : undefined,
      startDate,
      endDate: hasExplicitEndDate ? endDate : startDate,
      hasExplicitEndDate,
      startTime,
      endTime,
      scope,
    });
  }
  return events;
}

function parseTime(text: string | undefined | null, fallbackEnd: boolean): { time: string; approx: boolean } {
  if (!text) {
    return { time: fallbackEnd ? "23:59:59" : "00:00:00", approx: true };
  }
  const clean = text
    .replace(/(?<=\d)(st|nd|rd|th)/gi, "")
    .replace(/UTC/gi, "")
    .replace(/GMT/gi, "")
    .trim();
  if (!clean) {
    return { time: fallbackEnd ? "23:59:59" : "00:00:00", approx: true };
  }
  const ampmMatch = clean.match(/(\d{1,2})(?::(\d{2}))?(?::(\d{2}))?\s*(am|pm)/i);
  if (ampmMatch) {
    let hour = Number(ampmMatch[1]);
    const minute = ampmMatch[2] ? Number(ampmMatch[2]) : 0;
    const second = ampmMatch[3] ? Number(ampmMatch[3]) : 0;
    const suffix = ampmMatch[4].toLowerCase();
    if (suffix === "pm" && hour < 12) hour += 12;
    if (suffix === "am" && hour === 12) hour = 0;
    return {
      time: `${hour.toString().padStart(2, "0")}:${minute.toString().padStart(2, "0")}:${second
        .toString()
        .padStart(2, "0")}`,
      approx: false,
    };
  }
  const match = clean.match(/(\d{1,2})(?::(\d{2}))?(?::(\d{2}))?/);
  if (!match) {
    return { time: fallbackEnd ? "23:59:59" : "00:00:00", approx: true };
  }
  const hour = Math.max(0, Math.min(23, Number(match[1])));
  const minute = match[2] ? Math.max(0, Math.min(59, Number(match[2]))) : 0;
  const second = match[3] ? Math.max(0, Math.min(59, Number(match[3]))) : 0;
  return {
    time: `${hour.toString().padStart(2, "0")}:${minute.toString().padStart(2, "0")}:${second
      .toString()
      .padStart(2, "0")}`,
    approx: clean.indexOf(":") === -1,
  };
}

function parseDateParts(dateText: string): { year: number; month: number; day: number } | null {
  const trimmed = dateText.trim();
  if (!trimmed) return null;
  const isoMatch = trimmed.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
  if (isoMatch) {
    return {
      year: Number(isoMatch[1]),
      month: Number(isoMatch[2]),
      day: Number(isoMatch[3]),
    };
  }
  const altMatch = trimmed.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$/);
  if (altMatch) {
    return {
      year: Number(altMatch[3]),
      month: Number(altMatch[1]),
      day: Number(altMatch[2]),
    };
  }
  const parsed = new Date(trimmed);
  if (!Number.isNaN(parsed.getTime())) {
    return {
      year: parsed.getUTCFullYear(),
      month: parsed.getUTCMonth() + 1,
      day: parsed.getUTCDate(),
    };
  }
  return null;
}

function toUtcTimestamp(dateParts: { year: number; month: number; day: number }, time: string): number {
  const [hour, minute, second] = time.split(":").map((piece) => Number(piece));
  return Date.UTC(dateParts.year, dateParts.month - 1, dateParts.day, hour, minute, second);
}

function normalizeStopEvents(raw: RawEvent[], cutoffMs: number): {
  events: NormalizedEvent[];
  approxCount: number;
} {
  const events: NormalizedEvent[] = [];
  let approxCount = 0;
  for (const entry of raw) {
    const startParts = parseDateParts(entry.startDate);
    const endParts = entry.hasExplicitEndDate ? parseDateParts(entry.endDate) : null;
    if (!startParts) {
      console.warn(`[internet-shutdown] WARN invalid start date ${entry.startDate}`);
      continue;
    }
    const finalEndParts = endParts ?? startParts;
    let approx = false;
    const startTimeInfo = parseTime(entry.startTime, false);
    const endTimeInfo = parseTime(entry.endTime, true);
    approx = approx || startTimeInfo.approx || (entry.hasExplicitEndDate && endTimeInfo.approx);
    let startMs = toUtcTimestamp(startParts, startTimeInfo.time);
    let endMs: number;
    if (!entry.hasExplicitEndDate) {
      endMs = cutoffMs;
      approx = true;
    } else {
      if (!endParts) {
        approx = true;
      }
      endMs = toUtcTimestamp(finalEndParts, endTimeInfo.time);
      if (!entry.endTime && (!entry.startTime || entry.startTime.trim().length === 0)) {
        // STOP treats date-only spans as inclusive of both start and end days; extend to the next midnight to mirror report totals.
        endMs = Date.UTC(finalEndParts.year, finalEndParts.month - 1, finalEndParts.day + 1, 0, 0, 0);
        approx = true;
      }
    }
    if (endMs <= startMs) {
      // treat as inclusive day span when times missing or equal
      endMs = startMs + MS_PER_DAY;
      approx = true;
    }
    if (startMs >= cutoffMs) {
      continue;
    }
    if (endMs > cutoffMs) {
      endMs = cutoffMs;
      approx = true;
    }
    events.push({
      iso3: (entry.iso3 ?? "").trim().toUpperCase(),
      country: entry.country,
      scope: entry.scope,
      startMs,
      endMs,
      approx,
    });
    if (approx) approxCount++;
  }
  return { events, approxCount };
}

function isCountryIso(code: string | undefined | null): boolean {
  if (!code) return false;
  const iso = code.toUpperCase();
  return /^[A-Z]{3}$/.test(iso) && iso !== ISO_WORLD && !AGGREGATE_ISO3.has(iso);
}

async function collectLocalStopCsvs(): Promise<{ name: string; content: string }[]> {
  try {
    const entries = await readdir(STOP_CACHE_DIR, { withFileTypes: true });
    const files: { name: string; content: string }[] = [];
    for (const entry of entries) {
      const fullPath = join(STOP_CACHE_DIR, entry.name);
      if (entry.isDirectory()) {
        const nested = await collectStopCsvsRecursive(fullPath, entry.name);
        files.push(...nested);
        continue;
      }
      if (isAllowedStopFile(entry.name)) {
        const content = await readFile(fullPath, "utf8");
        files.push({ name: entry.name, content });
      }
    }
    return files;
  } catch (err: any) {
    if (err?.code === "ENOENT") {
      return [];
    }
    throw err;
  }
}

async function collectStopCsvsRecursive(dirPath: string, prefix: string): Promise<{ name: string; content: string }[]> {
  const entries = await readdir(dirPath, { withFileTypes: true });
  const results: { name: string; content: string }[] = [];
  for (const entry of entries) {
    const fullPath = join(dirPath, entry.name);
    if (entry.isDirectory()) {
      const nested = await collectStopCsvsRecursive(fullPath, join(prefix, entry.name));
      results.push(...nested);
    } else if (isAllowedStopFile(entry.name)) {
      const content = await readFile(fullPath, "utf8");
      results.push({ name: join(prefix, entry.name), content });
    }
  }
  return results;
}

function isAllowedStopFile(filename: string): boolean {
  const lower = filename.toLowerCase();
  return Array.from(STOP_ALLOWED_EXTENSIONS).some((ext) => lower.endsWith(ext));
}

async function fetchStopRemote(): Promise<{ name: string; content: string }[]> {
  const urls: string[] = [];
  if (STOP_DEFAULT_URL) urls.push(STOP_DEFAULT_URL);
  if (STOP_YEAR_URL_TEMPLATE) {
    const currentYear = new Date().getUTCFullYear();
    for (let year = START_YEAR; year <= currentYear; year++) {
      urls.push(STOP_YEAR_URL_TEMPLATE.replace(/\{year\}/gi, String(year)));
    }
  }
  const seen = new Set<string>();
  const outputs: { name: string; content: string }[] = [];
  await mkdir(STOP_CACHE_DIR, { recursive: true });
  for (const url of urls) {
    if (!url || seen.has(url)) continue;
    seen.add(url);
    try {
      console.log(`[internet-shutdown] fetching STOP dataset ${url}`);
      const res = await fetch(url, {
        headers: {
          Accept: "text/csv,application/zip,application/octet-stream",
          "User-Agent": "GAI-internet-shutdowns/1.0",
        },
      });
      if (!res.ok) {
        const snippet = await res.text().catch(() => "");
        console.warn(
          `[internet-shutdown] WARN fetch failed ${url} ${res.status} ${res.statusText} ${snippet.slice(0, 120)}`,
        );
        continue;
      }
      const contentType = (res.headers.get("content-type") ?? "").toLowerCase();
      const buffer = Buffer.from(await res.arrayBuffer());
      if (contentType.includes("zip")) {
        const extracted = await unzipToBuffers(buffer);
        for (const [name, data] of extracted) {
          if (!isAllowedStopFile(name)) continue;
          const cachedPath = join(STOP_CACHE_DIR, basename(name));
          await writeFile(cachedPath, data);
          outputs.push({ name: basename(name), content: data.toString("utf8") });
        }
      } else {
        const name = deriveFilenameFromUrl(url);
        const cachedPath = join(STOP_CACHE_DIR, name);
        await writeFile(cachedPath, buffer);
        outputs.push({ name, content: buffer.toString("utf8") });
      }
    } catch (err) {
      console.warn(`[internet-shutdown] WARN failed to fetch ${url}: ${(err as Error).message}`);
    }
  }
  return outputs;
}

function deriveFilenameFromUrl(url: string): string {
  const cleaned = url.split("?")[0];
  const tail = cleaned.split("/").filter(Boolean).pop();
  if (tail && isAllowedStopFile(tail)) {
    return tail;
  }
  return `stop_${Date.now()}.csv`;
}

async function unzipToBuffers(buffer: Buffer): Promise<Map<string, Buffer>> {
  try {
    return await unzipWithCommand(buffer);
  } catch (commandError) {
    const warnMessage =
      `[internet-shutdown] WARN unzip command unavailable or failed (${(commandError as Error).message}); ` +
      "falling back to JS unzip";
    console.warn(warnMessage);
    return unzipWithFflate(buffer);
  }
}

async function unzipWithCommand(buffer: Buffer): Promise<Map<string, Buffer>> {
  const dir = await mkdtemp(join(tmpdir(), "stop_bundle_"));
  const zipPath = join(dir, "bundle.zip");
  await writeFile(zipPath, buffer);
  try {
    await execFileAsync("unzip", ["-qo", zipPath, "-d", dir]);
  } catch (error: any) {
    await rm(dir, { recursive: true, force: true }).catch(() => {
      /* ignore */
    });
    throw new Error(error?.stderr || error?.message || String(error));
  }
  const results = new Map<string, Buffer>();
  async function walk(current: string, relative: string): Promise<void> {
    const entries = await readdir(current, { withFileTypes: true });
    for (const entry of entries) {
      const full = join(current, entry.name);
      const rel = relative ? join(relative, entry.name) : entry.name;
      if (entry.isDirectory()) {
        await walk(full, rel);
      } else {
        const data = await readFile(full);
        results.set(rel, data);
      }
    }
  }
  await walk(dir, "");
  await rm(dir, { recursive: true, force: true });
  return results;
}

function unzipWithFflate(buffer: Buffer): Map<string, Buffer> {
  const archive = unzipSync(new Uint8Array(buffer)) as Record<string, Uint8Array>;
  const results = new Map<string, Buffer>();
  for (const [name, data] of Object.entries(archive)) {
    results.set(name, Buffer.from(data));
  }
  return results;
}

async function loadPopulation(): Promise<PopulationData> {
  try {
    await access(POPULATION_PATH, fsConstants.F_OK);
  } catch (error) {
    throw new Error(
      `[internet-shutdown] required population snapshot missing at ${POPULATION_PATH}. ` +
        "Run 'npm run fetch:all' or refresh the World Bank SP.POP.TOTL cache as described in data/raw/README.md."
    );
  }
  const text = await readFile(POPULATION_PATH, "utf8");
  const { headers, rows } = parseCsv(text);
  if (!headers.length) {
    throw new Error("[internet-shutdown] population CSV missing headers");
  }
  const isoHeader = findHeader(headers, ["iso3", "iso", "country_iso"], ["iso"]);
  const yearHeader = findHeader(headers, ["year"], ["year"]);
  const popHeader = findHeader(headers, ["population", "pop"], ["pop"]);
  const nameHeader = findHeader(headers, ["country", "country_name"], ["country"]);
  if (!isoHeader || !yearHeader || !popHeader) {
    throw new Error("[internet-shutdown] population CSV missing iso/year/pop columns");
  }
  const populationByIsoYear = new Map<string, number>();
  const totalPopulationByYear = new Map<number, number>();
  const canonicalNameToIso = new Map<string, string>();
  const isoToCountryName = new Map<string, string>();
  for (const row of rows) {
    const iso = (row[isoHeader] ?? "").trim().toUpperCase();
    const year = Number((row[yearHeader] ?? "").trim());
    const popValue = (row[popHeader] ?? "").trim().replace(/[, ]+/g, "");
    const population = Number(popValue);
    if (!Number.isInteger(year)) continue;
    if (!Number.isFinite(population) || population <= 0) continue;
    if (iso === ISO_WORLD) continue;
    if (!isCountryIso(iso)) continue;
    populationByIsoYear.set(`${iso}:${year}`, population);
    totalPopulationByYear.set(year, (totalPopulationByYear.get(year) ?? 0) + population);
    if (nameHeader) {
      const name = (row[nameHeader] ?? "").trim();
      if (name) {
        const canonical = canonicalizeCountry(name);
        if (!canonicalNameToIso.has(canonical)) {
          canonicalNameToIso.set(canonical, iso);
        }
        if (!isoToCountryName.has(iso)) {
          isoToCountryName.set(iso, name);
        }
      }
    }
  }
  return { populationByIsoYear, totalPopulationByYear, canonicalNameToIso, isoToCountryName };
}

function resolveIsoForEvent(
  event: NormalizedEvent,
  population: PopulationData,
): { iso3: string | null; countryName: string } {
  const isoHint = event.iso3;
  const trimmedCountry = event.country.trim();
  if (isCountryIso(isoHint)) {
    const iso = isoHint.toUpperCase();
    const name = population.isoToCountryName.get(iso) ?? trimmedCountry;
    return { iso3: iso, countryName: name };
  }
  const candidate = trimmedCountry.toUpperCase();
  if (isCountryIso(candidate)) {
    const iso = candidate;
    const name = population.isoToCountryName.get(iso) ?? trimmedCountry;
    return { iso3: iso, countryName: name };
  }
  const canonical = canonicalizeCountry(trimmedCountry);
  if (STOP_ALIAS_TO_ISO[canonical]) {
    const iso = STOP_ALIAS_TO_ISO[canonical];
    const name = population.isoToCountryName.get(iso) ?? trimmedCountry;
    return { iso3: iso, countryName: name };
  }
  const iso = population.canonicalNameToIso.get(canonical);
  if (iso) {
    const name = population.isoToCountryName.get(iso) ?? trimmedCountry;
    return { iso3: iso, countryName: name };
  }
  return { iso3: null, countryName: trimmedCountry };
}

function splitByYear(event: NormalizedEvent): YearInterval[] {
  const intervals: YearInterval[] = [];
  const startYear = new Date(event.startMs).getUTCFullYear();
  const endYear = new Date(event.endMs - 1).getUTCFullYear();
  for (let year = startYear; year <= endYear; year++) {
    const yearStart = Date.UTC(year, 0, 1);
    const yearEnd = Date.UTC(year + 1, 0, 1);
    const start = Math.max(event.startMs, yearStart);
    const end = Math.min(event.endMs, yearEnd);
    if (end <= start) continue;
    intervals.push({
      iso3: event.iso3,
      country: event.country,
      year,
      startMs: start,
      endMs: end,
    });
  }
  return intervals;
}

function mergeIntervals(intervals: YearInterval[]): number {
  if (!intervals.length) return 0;
  const sorted = intervals
    .slice()
    .sort((a, b) => (a.startMs === b.startMs ? a.endMs - b.endMs : a.startMs - b.startMs));
  let total = 0;
  let currentStart = sorted[0].startMs;
  let currentEnd = sorted[0].endMs;
  for (let i = 1; i < sorted.length; i++) {
    const interval = sorted[i];
    if (interval.startMs <= currentEnd) {
      currentEnd = Math.max(currentEnd, interval.endMs);
    } else {
      total += currentEnd - currentStart;
      currentStart = interval.startMs;
      currentEnd = interval.endMs;
    }
  }
  total += currentEnd - currentStart;
  return total / MS_PER_DAY;
}

function round1(value: number): number {
  return Math.round(value * 10) / 10;
}

async function ensureLogsDir(): Promise<void> {
  await mkdir(dirname(GAISUM_LOG_PATH), { recursive: true });
}

async function loadStopData(): Promise<{ events: NormalizedEvent[]; approxCount: number }> {
  const cutoffYear = new Date().getUTCFullYear() - 1;
  const cutoffMs = Date.UTC(cutoffYear + 1, 0, 1);
  const rawEvents: RawEvent[] = [];
  const fixturePath = STOP_FIXTURE_PATH.trim();
  if (fixturePath) {
    const absolutePath = resolve(PROJECT_ROOT, fixturePath);
    let content: string;
    try {
      content = await readFile(absolutePath, "utf8");
    } catch (error) {
      throw new Error(
        `[internet-shutdown] STOP fixture not found at ${absolutePath}; ensure STOP_FIXTURE_PATH is correct.`,
      );
    }
    console.log(`[internet-shutdown] using STOP fixture ${absolutePath}`);
    try {
      rawEvents.push(...parseStopRows(content));
    } catch (err) {
      throw new Error(`[internet-shutdown] failed to parse STOP fixture: ${(err as Error).message}`);
    }
  } else {
    let sources = await collectLocalStopCsvs();
    if (!sources.length) {
      sources = await fetchStopRemote();
    }
    if (!sources.length) {
      throw new Error(
        "[internet-shutdown] missing STOP dataset; set STOP_FIXTURE_PATH or provide data/raw/keepiton/*.csv",
      );
    }
    for (const file of sources) {
      try {
        const parsed = parseStopRows(file.content);
        rawEvents.push(...parsed);
      } catch (err) {
        console.warn(`[internet-shutdown] WARN failed to parse ${file.name}: ${(err as Error).message}`);
      }
    }
  }
  if (!rawEvents.length) {
    throw new Error("[internet-shutdown] STOP dataset produced zero events");
  }
  const { events, approxCount } = normalizeStopEvents(rawEvents, cutoffMs);
  return { events, approxCount };
}

async function run(): Promise<void> {
  console.log("[internet-shutdown] loading STOP events and WDI population");
  const population = await loadPopulation();
  const { events, approxCount } = await loadStopData();
  const dedupe = new Set<string>();
  const countryCoverage = new Set<string>();
  const perCountryYear = new Map<string, YearInterval[]>();
  const resolvedEvents: NormalizedEvent[] = [];
  let droppedNoPopulation = 0;
  let droppedNoIso = 0;
  for (const event of events) {
    const resolution = resolveIsoForEvent(event, population);
    if (!resolution.iso3) {
      droppedNoIso++;
      continue;
    }
    const iso = resolution.iso3;
    const key = `${iso}:${event.startMs}:${event.endMs}`;
    if (dedupe.has(key)) continue;
    dedupe.add(key);
    const name = resolution.countryName;
    const normalized: NormalizedEvent = {
      iso3: iso,
      country: name,
      scope: event.scope,
      startMs: event.startMs,
      endMs: event.endMs,
      approx: event.approx,
    };
    const intervals = splitByYear(normalized);
    for (const interval of intervals) {
      const popKey = `${iso}:${interval.year}`;
      if (!population.populationByIsoYear.has(popKey)) {
        droppedNoPopulation++;
        continue;
      }
      const bucketKey = `${iso}:${interval.year}`;
      if (!perCountryYear.has(bucketKey)) {
        perCountryYear.set(bucketKey, []);
      }
      perCountryYear.get(bucketKey)!.push(interval);
      countryCoverage.add(iso);
    }
    resolvedEvents.push(normalized);
  }
  if (!perCountryYear.size) {
    throw new Error("[internet-shutdown] merged intervals empty after population filtering");
  }
  const worldSeriesMap = new Map<number, number>();
  const popShareByYear = new Map<number, number>();
  for (const [key, intervals] of perCountryYear) {
    const [iso, yearText] = key.split(":");
    const year = Number(yearText);
    const durationDays = mergeIntervals(intervals);
    const populationValue = population.populationByIsoYear.get(`${iso}:${year}`);
    const totalPopulation = population.totalPopulationByYear.get(year);
    if (!populationValue || !totalPopulation || totalPopulation <= 0) {
      continue;
    }
    const weight = populationValue / totalPopulation;
    const contribution = durationDays * weight;
    worldSeriesMap.set(year, (worldSeriesMap.get(year) ?? 0) + contribution);
    if (durationDays > 0) {
      popShareByYear.set(year, (popShareByYear.get(year) ?? 0) + weight);
    }
  }
  const years = Array.from(worldSeriesMap.keys()).sort((a, b) => a - b);
  if (!years.length) {
    throw new Error("[internet-shutdown] world series empty");
  }
  const series = years
    .filter((year) => year >= START_YEAR)
    .map((year) => ({ year, value: round1(worldSeriesMap.get(year) ?? 0) }))
    .sort((a, b) => a.year - b.year);

  await writeJson(OUTPUT_SERIES_PATH, series);

  const meanDays = series.reduce((sum, point) => sum + point.value, 0) / series.length;
  const popShareAnyShutdown = Object.fromEntries(
    Array.from(popShareByYear.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([year, share]) => [String(year), Number(share.toFixed(4))]),
  );
  if (Object.keys(popShareAnyShutdown).length) {
    const sampleYear = popShareByYear.has(2019)
      ? 2019
      : Math.max(...Array.from(popShareByYear.keys()));
    const share = popShareByYear.get(sampleYear) ?? 0;
    console.log(`[internet-shutdown] year ${sampleYear} pop_affected≈${share.toFixed(2)}`);
  }
  await ensureLogsDir();
  const gaisum = {
    id: METRIC_ID,
    rows: resolvedEvents.length,
    min_year: series[0].year,
    max_year: series[series.length - 1].year,
    coverage_countries: countryCoverage.size,
    mean_days_world: Number(meanDays.toFixed(3)),
    notes: {
      approx_duration_events: approxCount,
      dropped_no_iso: droppedNoIso,
      dropped_no_population: droppedNoPopulation,
      per_year_pop_share_affected: popShareAnyShutdown,
    },
  };
  await writeFile(GAISUM_LOG_PATH, `${JSON.stringify(gaisum, null, 2)}\n`, "utf8");
  console.log(`GAISUM ${METRIC_ID} ${JSON.stringify(gaisum)}`);
  console.log(
    `[internet-shutdown] coverage ${series[0].year}-${series[series.length - 1].year} (${series.length} years) countries=${countryCoverage.size}`,
  );
  console.log(
    `[internet-shutdown] dropped events: iso=${droppedNoIso} population=${droppedNoPopulation} approx=${approxCount}`,
  );

  await upsertSource(STOP_SOURCE_ID, {
    name: "#KeepItOn Shutdown Tracker (STOP)",
    domain: "Truth & Clarity",
    unit: "shutdown days",
    source_org: "Access Now",
    source_url: STOP_DEFAULT_URL,
    license: "© Access Now, used under fair-use for research", // dataset license summary placeholder
    cadence: "event",
    method:
      "Ingest Access Now STOP shutdown events (2016→latest), normalize times to UTC, split by calendar year, merge overlapping country-year intervals, and compute population-weighted shutdown days.",
    updated_at: new Date().toISOString().slice(0, 10),
    data_start_year: START_YEAR,
    notes: "Scope includes all STOP-verified shutdown types (mobile, regional, national).",
  });

  await upsertSource(POP_SOURCE_ID, {
    name: "World Bank Population (SP.POP.TOTL)",
    domain: "Demographics",
    unit: "people",
    source_org: "World Bank",
    source_url: "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL",
    license: "CC BY 4.0",
    cadence: "annual",
    method: "Use ISO3 country populations to construct annual population weights for the shutdown aggregation.",
    updated_at: new Date().toISOString().slice(0, 10),
    data_start_year: 1960,
  });

  await upsertSource(METRIC_ID, {
    name: "Internet shutdown days (population-weighted, annual)",
    domain: "Truth & Clarity",
    unit: "days",
    source_org: "Global Alignment Index",
    source_url: "/public/data/internet_shutdown_days.json",
    license: "CC BY 4.0",
    cadence: "annual",
    method:
      "Derived metric: Access Now STOP events normalized to UTC, split by year, merged per country-year, and weighted by World Bank population (SP.POP.TOTL). Values rounded to 1 decimal.",
    updated_at: new Date().toISOString().slice(0, 10),
    data_start_year: 2016,
    inputs: [STOP_SOURCE_ID, POP_SOURCE_ID],
    produces: ["public/data/internet_shutdown_days.json"],
    type: "derived",
  });

  console.log(`[internet-shutdown] wrote ${OUTPUT_SERIES_PATH}`);
  console.log(`[internet-shutdown] wrote GAISUM log ${GAISUM_LOG_PATH}`);
}

const isEntry = import.meta.url === new URL(process.argv[1], "file://").href;
if (isEntry) {
  run().catch((err) => {
    console.error("[internet-shutdown] fatal", err);
    process.exit(1);
  });
}

export { run };
