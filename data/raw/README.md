# Raw data snapshots

| File | Source | Retrieved | SHA256 |
| --- | --- | --- | --- |
| `homicide_unodc_who.csv` | World Bank WDI indicator VC.IHR.PSRC.P5 (UNODC/WHO) | 2025-10-07 | ec4b33a74b85f0da1eb5211075b9ac5e5a244bd5185e43ba182e69f47edc8f60 |
| `pop_by_country.csv` | World Bank WDI indicator SP.POP.TOTL | 2025-10-07 | 247af97455f864f515432d1e3e36956294520547f00d1d7d3f23ffbc33d29b40 |

Snapshots retrieved via `https://api.worldbank.org/v2/` bulk download endpoints. Files normalized into tidy CSV format (ISO3-country-year).

To refresh `pop_by_country.csv`, run `npm run fetch:all` (preferred) or invoke the population fetcher used by other Tier-1 pipelines, then commit the updated CSV + checksum. The internet shutdown pipeline will abort with a friendly message if this snapshot is missing.
