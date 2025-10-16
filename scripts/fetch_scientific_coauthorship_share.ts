import { execFile } from "node:child_process";
import { setDefaultResultOrder } from "node:dns";
import { mkdir, rename, writeFile } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { promisify } from "node:util";

const METRIC_ID = "scientific_coauthorship_share";
const BASE_URL = "https://api.openalex.org/works";
const START_YEAR = 1990;
const END_YEAR = 2050;
const USER_AGENT = "GAI-scientific-coauthorship/1.0";
const MAILTO = "contact@global-alignment-index.com";

try {
  setDefaultResultOrder("ipv4first");
} catch (err) {
  console.warn(`[${METRIC_ID}] WARN unable to set DNS result order: ${String(err)}`);
}

const execFileAsync = promisify(execFile);

const PROJECT_ROOT = process.cwd();
const OUTPUT_PATH = resolve(PROJECT_ROOT, "public/data/scientific_coauthorship_share.json");
const GAISUM_LOG_PATH = resolve(
  PROJECT_ROOT,
  "scripts/logs/scientific_coauthorship_share.gaisum.json",
);

const round1 = (value: number): number => Math.round(value * 10) / 10;
const round2 = (value: number): number => Math.round(value * 100) / 100;

type YearCount = { year: number; count: number };
type JoinedPoint = {
  year: number;
  total: number;
  international: number;
  sharePrecise: number;
  value: number;
};
type SeriesPoint = { year: number; value: number };
type YearDelta = { year: number; delta: number };

type GroupResponse = {
  group_by: Array<{ key: string | null; count: number }>;
  meta?: { next_cursor?: string | null };
};

type FetchError = Error & { status?: number };

async function sleep(ms: number): Promise<void> {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

function buildUrl(params: Record<string, string>): string {
  const url = new URL(BASE_URL);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }
  return url.toString();
}

async function fetchJson(url: string, attempt = 1, maxAttempts = 5): Promise<GroupResponse> {
  try {
    console.log(`[${METRIC_ID}] GET ${url}`);
    const { stdout } = await execFileAsync("curl", [
      "--silent",
      "--show-error",
      "--location",
      "--max-time",
      "30",
      "--compressed",
      "--header",
      `User-Agent: ${USER_AGENT}`,
      "--header",
      "Accept: application/json",
      "--write-out",
      "\n%{http_code}\n",
      url,
    ]);
    const trimmed = stdout.trimEnd();
    const lastNewline = trimmed.lastIndexOf("\n");
    if (lastNewline === -1) {
      throw new Error(`[${METRIC_ID}] unexpected curl output (missing status line)`);
    }
    const statusText = trimmed.slice(lastNewline + 1);
    const body = trimmed.slice(0, lastNewline);
    const status = Number.parseInt(statusText, 10);
    console.log(`[${METRIC_ID}] ← ${status}`);
    if (Number.isNaN(status) || status < 200 || status >= 300) {
      const error = new Error(`[${METRIC_ID}] HTTP ${status}`) as FetchError;
      error.status = status;
      throw error;
    }
    return JSON.parse(body) as GroupResponse;
  } catch (err) {
    if (err && typeof err === "object" && err !== null && "stdout" in err) {
      const stdout = String((err as { stdout?: string }).stdout ?? "").trimEnd();
      if (stdout) {
        const lastNewline = stdout.lastIndexOf("\n");
        if (lastNewline !== -1) {
          const statusText = stdout.slice(lastNewline + 1);
          const body = stdout.slice(0, lastNewline);
          const status = Number.parseInt(statusText, 10);
          if (!Number.isNaN(status)) {
            console.warn(`[${METRIC_ID}] WARN curl error status=${status}`);
            if (status >= 200 && status < 300) {
              try {
                return JSON.parse(body) as GroupResponse;
              } catch (parseErr) {
                console.warn(`[${METRIC_ID}] WARN failed to parse curl body after error: ${String(parseErr)}`);
              }
            }
            if ((status === 429 || status >= 500) && attempt < maxAttempts) {
              const backoff = 500 * 2 ** (attempt - 1);
              console.warn(
                `[${METRIC_ID}] WARN HTTP ${status}; retrying in ${backoff}ms (attempt ${attempt})`,
              );
              await sleep(backoff);
              return fetchJson(url, attempt + 1, maxAttempts);
            }
          }
        }
      }
    }
    if (attempt >= maxAttempts) {
      throw err;
    }
    const delay = 500 * 2 ** (attempt - 1);
    console.warn(`[${METRIC_ID}] WARN fetch failed (attempt ${attempt}): ${String(err)}; retrying in ${delay}ms`);
    await sleep(delay);
    return fetchJson(url, attempt + 1, maxAttempts);
  }
}

async function fetchGrouped(filter: string): Promise<YearCount[]> {
  const params = {
    filter,
    "group_by": "publication_year",
    "per-page": "200",
    mailto: MAILTO,
  } as const;

  let cursor: string | undefined;
  const rows: YearCount[] = [];

  do {
    const queryParams = cursor ? { ...params, cursor } : params;
    const url = buildUrl(queryParams as Record<string, string>);
    const json = await fetchJson(url);
    const groups = json.group_by ?? [];
    for (const entry of groups) {
      const year = entry?.key != null ? Number(entry.key) : NaN;
      if (!Number.isFinite(year) || year < START_YEAR || year > END_YEAR) {
        continue;
      }
      rows.push({ year, count: entry.count });
    }
    cursor = json.meta?.next_cursor ?? undefined;
    if (cursor) {
      console.log(`[${METRIC_ID}] paging with cursor ${cursor}`);
    }
  } while (cursor);

  const merged = new Map<number, number>();
  for (const { year, count } of rows) {
    merged.set(year, (merged.get(year) ?? 0) + count);
  }

  const result = Array.from(merged.entries())
    .map(([year, count]) => ({ year, count }))
    .sort((a, b) => a.year - b.year);

  console.log(
    `[${METRIC_ID}] fetched ${result.length} grouped rows for filter=${filter} (${result[0]?.year ?? "?"}-${
      result[result.length - 1]?.year ?? "?"
    })`,
  );

  return result;
}

function joinSeries(total: YearCount[], international: YearCount[]): JoinedPoint[] {
  const totals = new Map(total.map((item) => [item.year, item.count] as const));
  const intl = new Map(international.map((item) => [item.year, item.count] as const));
  const years = Array.from(totals.keys())
    .filter((year) => intl.has(year))
    .sort((a, b) => a - b);

  const joined: JoinedPoint[] = [];
  for (const year of years) {
    const totalCount = totals.get(year) ?? 0;
    const intlCount = intl.get(year) ?? 0;
    if (totalCount <= 0) {
      console.warn(`[${METRIC_ID}] WARN skipping year ${year} due to non-positive total count (${totalCount})`);
      continue;
    }
    if (intlCount < 0) {
      console.warn(`[${METRIC_ID}] WARN skipping year ${year} due to negative international count (${intlCount})`);
      continue;
    }
    const precise = round2((intlCount / totalCount) * 100);
    const rounded = round1(precise);
    joined.push({
      year,
      total: totalCount,
      international: intlCount,
      sharePrecise: precise,
      value: rounded,
    });
  }
  return joined;
}

function toSeries(points: JoinedPoint[]): SeriesPoint[] {
  return points.map((point) => ({ year: point.year, value: point.value }));
}

function computeGaisum(
  points: JoinedPoint[],
  filters: { totalFilter: string; intlFilter: string },
  extraNotes?: Record<string, unknown>,
) {
  if (!points.length) {
    throw new Error(`[${METRIC_ID}] no points to compute GAISUM`);
  }
  const years = points.map((p) => p.year);
  const preciseValues = points.map((p) => p.sharePrecise);
  const rows = points.length;
  const minYear = Math.min(...years);
  const maxYear = Math.max(...years);
  const mean = round1(preciseValues.reduce((sum, val) => sum + val, 0) / rows);
  const coverage = round1((rows / (maxYear - minYear + 1)) * 100);
  const deltas: YearDelta[] = [];
  for (let i = 1; i < points.length; i += 1) {
    const delta = round1(points[i].value - points[i - 1].value);
    deltas.push({ year: points[i].year, delta });
  }
  deltas.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
  const topDeltas = deltas.slice(0, 3);
  const notes: Record<string, unknown> = {
    earliest_year: minYear,
    earliest_value: points[0].value,
    latest_year: maxYear,
    latest_value: points[points.length - 1].value,
    fetch_timestamp_utc: new Date().toISOString(),
    denominator_filter: "countries_distinct_count:>0",
    filters_applied: [filters.totalFilter, filters.intlFilter],
  };
  if (extraNotes) {
    Object.assign(notes, extraNotes);
  }
  return {
    id: METRIC_ID,
    rows,
    min_year: minYear,
    max_year: maxYear,
    mean,
    coverage,
    top_deltas: topDeltas,
    notes,
  };
}

async function writeJsonAtomic(path: string, data: unknown): Promise<void> {
  const dir = dirname(path);
  await mkdir(dir, { recursive: true });
  const tempPath = `${path}.tmp-${Date.now()}`;
  await writeFile(tempPath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
  await rename(tempPath, path);
}

async function shareForYear(
  totalFilter: string,
  intlFilter: string,
  year: number,
): Promise<{ year: number; share: number; total: number; international: number } | null> {
  const totalRows = await fetchGrouped(`${totalFilter},publication_year:${year}-${year}`);
  const intlRows = await fetchGrouped(`${intlFilter},publication_year:${year}-${year}`);
  const totalMatch = totalRows.find((row) => row.year === year);
  const intlMatch = intlRows.find((row) => row.year === year);
  if (!totalMatch || !intlMatch) {
    return null;
  }
  const { count: total } = totalMatch;
  const { count: international } = intlMatch;
  if (total <= 0) {
    return null;
  }
  return { year, share: round1((international / total) * 100), total, international };
}

type YearShareSnapshot = Awaited<ReturnType<typeof shareForYear>>;

function capToComplete(points: JoinedPoint[]): JoinedPoint[] {
  const arr = [...points].sort((a, b) => a.year - b.year);
  for (let k = 0; k < 2; k += 1) {
    const n = arr.length;
    if (n < 2) {
      break;
    }
    const last = arr[n - 1];
    const prev = arr[n - 2];
    if (last.total < 0.95 * prev.total) {
      console.warn(
        `[${METRIC_ID}] WARN ${last.year} appears incomplete (total ${last.total} < 95% of ${prev.year} ${prev.total}); dropping tail year.`,
      );
      arr.pop();
      continue;
    }
    break;
  }
  return arr;
}

async function run(): Promise<void> {
  console.log(`[${METRIC_ID}] fetching article counts with country attribution`);
  const baseTotalFilter =
    "type_crossref:journal-article|proceedings-article,primary_location.is_published:true,primary_location.source.has_issn:true,is_paratext:false,countries_distinct_count:>0";
  const baseIntlFilter =
    "type_crossref:journal-article|proceedings-article,primary_location.is_published:true,primary_location.source.has_issn:true,is_paratext:false,countries_distinct_count:>1";
  const totalFilter = `${baseTotalFilter},publication_year:${START_YEAR}-${END_YEAR}`;
  const intlFilter = `${baseIntlFilter},publication_year:${START_YEAR}-${END_YEAR}`;

  const total = await fetchGrouped(totalFilter);
  console.log(`[${METRIC_ID}] fetching internationally co-authored counts`);
  const intl = await fetchGrouped(intlFilter);

  const joined = joinSeries(total, intl);
  if (!joined.length) {
    throw new Error(`[${METRIC_ID}] no overlapping years between total and international series`);
  }

  const cappedJoined = capToComplete(joined);
  if (!cappedJoined.length) {
    throw new Error(`[${METRIC_ID}] no data after completeness cap`);
  }

  const joinedByYear = new Map(cappedJoined.map((point) => [point.year, point] as const));
  const series = toSeries(cappedJoined);
  const first = cappedJoined[0];
  const last = cappedJoined[cappedJoined.length - 1];
  console.log(
    `[${METRIC_ID}] computed ${series.length} points (${first.year}-${last.year}); first=${first.value}%, last=${last.value}%`,
  );

  const sanityStart = series.find((point) => point.year === START_YEAR);
  if (sanityStart && (sanityStart.value < 6 || sanityStart.value > 14)) {
    console.warn(
      `[${METRIC_ID}] WARN 1990 share ${sanityStart.value}% outside expected ~10% band (6-14%)`,
    );
  }
  const sanity2010 = series.find((point) => point.year === 2010);
  if (sanity2010 && (sanity2010.value < 16 || sanity2010.value > 24)) {
    console.warn(
      `[${METRIC_ID}] WARN 2010 share ${sanity2010.value}% outside expected band (16-24%)`,
    );
  }
  const sanity2021 = series.find((point) => point.year === 2021);
  if (sanity2021 && (sanity2021.value < 22 || sanity2021.value > 29)) {
    console.warn(
      `[${METRIC_ID}] WARN 2021 share ${sanity2021.value}% outside expected band (22-29%)`,
    );
  }
  const referenceYear = 2023;
  const sanityRecent = series.find((point) => point.year === referenceYear);
  if (sanityRecent) {
    if (sanityRecent.value < 24 || sanityRecent.value > 32) {
      console.warn(
        `[${METRIC_ID}] WARN ${referenceYear} share ${sanityRecent.value}% outside expected ~28-30% band`,
      );
    }
  } else {
    console.warn(`[${METRIC_ID}] WARN missing ${referenceYear} data for sanity check`);
  }

  const byYear = new Map(cappedJoined.map((point) => [point.year, point] as const));
  const peekShare = (year: number): number | null => byYear.get(year)?.value ?? null;
  const peekCounts = (year: number): { total: number; international: number } | null => {
    const match = joinedByYear.get(year);
    return match
      ? {
          total: match.total,
          international: match.international,
        }
      : null;
  };
  const consoleDiagYears = [2010, 2020, 2021, 2023, 2024];
  for (const year of consoleDiagYears) {
    const share = peekShare(year);
    const counts = peekCounts(year);
    if (share != null && counts) {
      console.log(
        `[${METRIC_ID}] diagnostic year=${year} share=${share}% international=${counts.international} total=${counts.total}`,
      );
    } else {
      console.log(`[${METRIC_ID}] diagnostic year=${year} missing data`);
    }
  }
  const diagnostics = {
    shares: {
      y2010: peekShare(2010),
      y2020: peekShare(2020),
      y2021: peekShare(2021),
      y2023: peekShare(2023),
      y2024: peekShare(2024),
    },
    counts: {
      y2010: peekCounts(2010),
      y2020: peekCounts(2020),
      y2021: peekCounts(2021),
      y2023: peekCounts(2023),
      y2024: peekCounts(2024),
    },
  };

  const diagYears = [2010, 2020, 2021, 2023];
  const altATotalFilter =
    "type_crossref:journal-article|proceedings-article,primary_location.is_published:true,primary_location.source.type:journal|proceedings,is_paratext:false,countries_distinct_count:>0";
  const altAIntlFilter =
    "type_crossref:journal-article|proceedings-article,primary_location.is_published:true,primary_location.source.type:journal|proceedings,is_paratext:false,countries_distinct_count:>1";
  const altBTotalFilter = `${baseTotalFilter},has_doi:true`;
  const altBIntlFilter = `${baseIntlFilter},has_doi:true`;

  const filterCompare: {
    base: Record<number, YearShareSnapshot | null>;
    altA: Record<number, YearShareSnapshot | null>;
    altB: Record<number, YearShareSnapshot | null>;
  } = {
    base: {},
    altA: {},
    altB: {},
  };

  for (const year of diagYears) {
    filterCompare.base[year] = await shareForYear(baseTotalFilter, baseIntlFilter, year);
    filterCompare.altA[year] = await shareForYear(altATotalFilter, altAIntlFilter, year);
    filterCompare.altB[year] = await shareForYear(altBTotalFilter, altBIntlFilter, year);
  }

  console.log(`[${METRIC_ID}] filter comparison ${JSON.stringify(filterCompare)}`);

  const gaisum = computeGaisum(
    cappedJoined,
    { totalFilter, intlFilter },
    {
      diagnostics,
      completeness_cap_to_year: last.year,
      filter_compare: filterCompare,
    },
  );
  console.log(`[${METRIC_ID}] GAISUM ${JSON.stringify(gaisum)}`);

  await writeJsonAtomic(OUTPUT_PATH, series);
  console.log(`[${METRIC_ID}] wrote dataset ${OUTPUT_PATH}`);

  await writeJsonAtomic(GAISUM_LOG_PATH, gaisum);
  console.log(`[${METRIC_ID}] wrote GAISUM log ${GAISUM_LOG_PATH}`);
}

const isMain = import.meta.url === new URL(process.argv[1], "file://").href;

if (isMain) {
  run().catch((err) => {
    console.error(`[${METRIC_ID}] fatal`, err);
    process.exit(1);
  });
}

export { run };
