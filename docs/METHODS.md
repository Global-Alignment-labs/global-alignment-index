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

**Extreme poverty (% of population below $2.15/day) — Global (World Bank)**
- Source: World Bank WDI (SI.POV.DDAY with SP.POP.TOTL)
- Unit: % of population
- Cadence: annual
- Method: Pop-weighted global mean from national SI.POV.DDAY using SP.POP.TOTL; exclude aggregates; round to 2 decimals; WDI may include modeled/nowcasted values.
