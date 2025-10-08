# 2025-09-18
- data(battle_deaths): add UCDP battle-related deaths pipeline (total/by-type/interstate outputs) and register Tier-1 interstate metric; METHODS updated with source, mapping, and license notes.
- docs(README): document the consent-gated UCDP download flow and local `data/raw` placement required before running the battle-deaths fetcher.

# Changelog

## 2025-08-31
- data(firearm_stock_per_100): add SAS + WDI pipeline with offline cache and GAISUM logging.

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
