# Offline cache: firearm_stock_per_100 pipeline

These snapshots support offline execution of the `firearm_stock_per_100` pipeline. They contain the minimal fields required for the join between the Small Arms Survey civilian firearms holdings and World Bank WDI population totals.

## Files

- `sas_civilian_firearms.csv` — ISO3/year civilian firearm stock estimates (total counts). Source: Small Arms Survey, Global Firearms Holdings Dataset (2017 revision). https://www.smallarmssurvey.org/database/global-firearms-holdings
- `wdi_population.csv` — ISO3/year total population (WDI SP.POP.TOTL). Source: World Bank, World Development Indicators. https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL

## Usage

Set `OFFLINE=1` (or leave the network unreachable) to force the pipeline to use these cached copies. When connectivity is available, refresh the snapshots by downloading the latest CSVs from the providers and replacing these files.

The default npm script calls the prebuilt JS runner (`scripts/pipelines/firearm_stock_per_100.js`). The TypeScript pipeline remains the source of truth—if you update it, commit a matching JS mirror in the same change.
