import { appendFileSync, readFileSync } from "node:fs";
import { join } from "node:path";

const GAISUM_PATH = join("scripts", "logs", "internet_shutdown_days.gaisum.json");
const SERIES_PATH = join("public", "data", "internet_shutdown_days.json");

function readJson<T>(path: string): T {
  return JSON.parse(readFileSync(path, "utf8")) as T;
}

function formatChecklistLine(label: string, passed: boolean | null): string {
  const box = passed === null ? "[ ]" : passed ? "[x]" : "[ ]";
  return `- ${box} ${label}`;
}

function assertCondition(condition: boolean, message: string): void {
  if (!condition) {
    throw new Error(message);
  }
}

function writeGithubOutput(key: string, value: string): void {
  const outputPath = process.env.GITHUB_OUTPUT;
  if (!outputPath) return;
  appendFileSync(outputPath, `${key}<<EOF\n${value}\nEOF\n`);
}

type SeriesPoint = { year: number; value: number };

type GaisumLog = {
  id: string;
  rows: number;
  min_year: number;
  max_year: number;
  coverage_countries: number;
  mean_days_world: number;
  notes?: Record<string, unknown>;
};

function main(): void {
  const gaisum = readJson<GaisumLog>(GAISUM_PATH);
  const series = readJson<SeriesPoint[]>(SERIES_PATH);
  const isFixture = Boolean(process.env.STOP_FIXTURE_PATH);

  const minYearOk = gaisum.min_year === 2016;
  const minAcceptableMaxYear = isFixture ? 2016 : 2023;
  const maxYearOk = typeof gaisum.max_year === "number" && gaisum.max_year >= minAcceptableMaxYear;
  const valuesOk = Array.isArray(series) && series.every((row) => row.value >= 0 && row.value <= 366);
  const approxPresent = Boolean(gaisum.notes && typeof gaisum.notes.approx_duration_events === "number");

  const perYearShares = (gaisum.notes?.per_year_pop_share_affected ?? {}) as Record<string, unknown>;
  const shareEntries = Object.entries(perYearShares).filter(([, value]) => typeof value === "number");
  const popShareHasEntries = shareEntries.length > 0;
  const popShareYearsAbove5 = shareEntries.filter(([, value]) => (value as number) >= 0.05).length;
  const popShareCoverageOk = popShareYearsAbove5 >= 3;

  const coverageThreshold = isFixture ? 2 : 41;
  const coverageOk = typeof gaisum.coverage_countries === "number" && gaisum.coverage_countries >= coverageThreshold;
  const rowsThreshold = isFixture ? 10 : 1000;
  const rowsOk = typeof gaisum.rows === "number" && gaisum.rows >= rowsThreshold;

  assertCondition(minYearOk, "min_year must equal 2016");
  assertCondition(maxYearOk, `max_year must be ≥ ${minAcceptableMaxYear}`);
  assertCondition(valuesOk, "All series values must lie within [0, 366]");
  assertCondition(popShareHasEntries, "per_year_pop_share_affected must have at least one entry");
  assertCondition(popShareCoverageOk, "At least three years must have ≥5% population share affected");

  const checklistLabels: Array<[string, boolean | null]> = [
    ["min_year = 2016", minYearOk],
    [`max_year ≥ ${minAcceptableMaxYear}`, maxYearOk],
    ["values ∈ [0, 366]; typical 15–30", valuesOk],
    [isFixture ? "coverage_countries ≥ 2 (fixture)" : "coverage_countries > 40", coverageOk],
    [
      isFixture ? "rows ≥ 10 (fixture); approx_duration_events present" : "rows > 1000; approx_duration_events present",
      rowsOk && approxPresent,
    ],
    ["pop share affected ≥5% in ≥3 years", popShareCoverageOk],
    ["Spot sanity: India 2019 ↑, Myanmar 2021 ↑, Ethiopia 2020–21 ↑", null],
  ];

  const checklist = ["**Internet Shutdown Days — QA**", ...checklistLabels.map(([label, result]) => formatChecklistLine(label, result))];
  checklist.push("", "<details><summary>GAISUM</summary>", "", "```json", JSON.stringify(gaisum, null, 2), "```", "", "</details>");

  console.log("GAISUM", JSON.stringify(gaisum, null, 2));
  console.log(checklist.join("\n"));

  writeGithubOutput("comment", checklist.join("\n"));
}

main();
