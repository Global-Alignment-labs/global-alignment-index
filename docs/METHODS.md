# Methods (v0.1)

- Alignment Index: z‑score per metric, average per year (placeholder).
- Domains: group metrics and average.
- Capability Index: TBD (GDP pc, energy pc, R&D % GDP, diffusion).
- Transparency: sources listed per metric; changes logged in CHANGELOG.

## Relative alignment v0.1
- Metrics metadata lives in `public/data/metrics_registry.json`.
- Relative alignment normalizes each metric to 0–100%.
- For direction `up`: `(value - min) / (max - min)`; for `down`: `(max - value) / (max - min)`.
- Targets (if any) map 100% to the target in the alignment direction.
- Used to render a simple relative percent alongside raw values.

## Pipelines (overview)
- scripts/pipelines/<metric>.ts → fetches raw data; writes /public/data/<metric>.json
- Annualization: simple arithmetic mean of valid monthly values; excludes placeholders (e.g., -99.99)
- CO₂ source: NOAA Global monthly mean CO₂ (ppm), see pipeline

**Life expectancy (years) — World (World Bank)**
- Source: World Bank API (SP.DYN.LE00.IN)
- Unit: years
- Cadence: annual
- Method: use WB annual values; include only numeric values; round 2 decimals.

**Under-5 mortality (per 1,000 live births) — Global (UN IGME / World Bank)**
- Source: UN IGME via World Bank WDI (SH.DYN.MORT)
- Unit: per 1,000 live births
- Cadence: annual
- Method: Population-weighted global mean of national SH.DYN.MORT using SP.POP.TOTL; exclude aggregates; round to 2 decimals.

**Battle-related deaths (deaths per 100k) — Global total (UCDP v25.1)**
- Source & License: UCDP Battle-Related Deaths Dataset v25.1 (released 2025-06-24, academic & non-commercial use permitted); population denominator from World Bank WDI SP.POP.TOTL (CC BY 4.0).
- Conflict-type mapping enforced in code: `1 → interstate`, `2 → intrastate`, `3 → internationalized_intrastate`, `4 → extrasystemic`; unknown codes fail fast.
- Method: fetch the UCDP conflict-level CSV, resolve deaths column preference (`bd_best` (v25.1) then `best`, `best_estimate`), sum conflict-type codes 1–4 by year, inner-join with global population for the continuous 1990 → latest overlapping year range, drop/ warn on extra years, compute deaths per 100,000 people, and round to three decimals.
- Outputs: Tier-1 total series (global battle-related deaths per 100k) plus companion by-type breakdowns (interstate, intrastate, internationalized intrastate, extrasystemic) and an interstate-only series for future UI toggles; all outputs validated for non-negativity, continuity, and per-type sum ≈ total (≤0.02 tolerance).
