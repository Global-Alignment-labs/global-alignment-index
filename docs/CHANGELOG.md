# Changelog

## 2025-09-18
- data(u5_mortality): add ≥80% pop-coverage guard to remove early-year spike.
- data(extreme_poverty): enforce 1981+ and ≥80% pop-coverage guard.
- data(extreme_poverty): add WDI SI.POV.DDAY × SP.POP.TOTL pipeline; registry, sources, and METHODS updated.

## 2025-08-29
- data(u5_mortality): add WDI pipeline; METHODS updated.

## 2025-08-30
- build(alignment): add metrics registry, relative helper, dataset schema validation, and scheduler scaffold.

## 2025-08-28
- data(life_expectancy): add pipeline from World Bank API; METHODS updated.

## 2025-08-27
- Use @/... alias for imports instead of relative paths.
- build(data): generalize fetch scripts into scripts/lib + scripts/pipelines; add fetch:all

## 2025-08-25 — v0.1 seed
- Next.js scaffold + Tailwind + Recharts
- Mock datasets for CO₂, life expectancy, internet use
- Placeholder aggregate (z‑score average)
- Docs: CONTEXT, METHODS, CHANGELOG
