# Changelog

## 2025-10-17
- ui(internet_shutdown_days): add Truth & Clarity dashboard card, metric detail route, and direction badge copy.
- ops(internet_shutdown_days): add manual STOP bake workflow to commit baked JSON + GAISUM outputs.
- data(internet_shutdown_days): add STOP fixture-driven CI, per-year population share diagnostics, and scripts typecheck scope.

## 2025-10-16
- data(internet_shutdown_days): add STOP + WDI pipeline with GAISUM diagnostics, CI QA automation, and documentation updates.
- data(scientific_coauthorship_share): add Tier-1 OpenAlex pipeline for international co-authorship share with GAISUM logging, registry/source wiring, and dataset publication.
- methods(scientific_coauthorship_share): document OpenAlex scope, continuity cap to 2023, and diagnostics.
- ui(scientific_coauthorship_share): add Truth & Clarity dashboard card + metric detail route with latest-complete footnote.

## 2025-10-14
- data(death_registration_completeness): add Tier-1 Truth & Clarity pipeline for WDI SP.REG.DTHS.ZS with population-weighted global mean, GAISUM logging, registry/source wiring, and dashboard exposure.

## 2025-10-10
- data(firearm_stock_per_100): capture coverage diagnostics in GAISUM logging for pipeline and JS mirror.

## 2025-10-08
- data(homicide_rate): add Tier-1 homicide rate pipeline and dataset.
- data(homicide_rate): switch to hybrid compute with WDI WLD fallback when coverage <95%.

## 2025-09-18
- data(battle_deaths): add UCDP battle-related deaths pipeline (total/by-type/interstate outputs) and register Tier-1 interstate metric; METHODS updated with source, mapping, and license notes.
- docs(README): document the consent-gated UCDP download flow and local `data/raw` placement required before running the battle-deaths fetcher.

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
- Placeholder aggregate (z-score average)
- Docs: CONTEXT, METHODS, CHANGELOG
