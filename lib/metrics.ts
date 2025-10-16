export type Direction = 'up_is_better' | 'down_is_better'

export type Metric = {
  id: string
  name: string
  domain: string
  unit: string
  direction: Direction
  source: string
  dataPath?: string
  subtitle?: string
  detailPath?: string
}

export const METRICS: Metric[] = [
  { id: 'co2_ppm', name: 'CO₂ concentration', domain: 'Climate & Environment', unit: 'ppm', direction: 'down_is_better', source: 'NOAA/ESRL' },
  { id: 'life_expectancy', name: 'Life expectancy', domain: 'Health & Wellbeing', unit: 'years', direction: 'up_is_better', source: 'WHO/World Bank' },
  { id: 'internet_use', name: 'Individuals using the internet', domain: 'Education & Digital', unit: '%', direction: 'up_is_better', source: 'ITU' },
  { id: 'u5_mortality', name: 'Under-5 mortality', domain: 'Health & Wellbeing', unit: 'per 1,000 live births', direction: 'down_is_better', source: 'UN IGME / World Bank' },
  { id: 'battle_deaths', name: 'Battle-related deaths', domain: 'Safety & Conflict', unit: 'deaths per 100k', direction: 'down_is_better', source: 'UCDP' },
  {
    id: 'death_registration_completeness',
    name: 'Death registration completeness (%) Test',
    domain: 'Truth & Clarity',
    unit: '%',
    direction: 'up_is_better',
    source: 'World Bank WDI',
  },
  {
    id: 'internet_shutdown_days',
    name: 'Internet shutdown days',
    domain: 'Truth & Clarity',
    unit: 'days',
    direction: 'down_is_better',
    source: 'Access Now #KeepItOn / World Bank',
    dataPath: '/data/internet_shutdown_days.json',
    subtitle: 'Population-weighted average shutdown days per year',
    detailPath: '/metrics/internet-shutdown-days',
  },
  { id: 'homicide_rate', name: 'Intentional homicide rate per 100 000', domain: 'Safety & Care', unit: 'per 100,000 people', direction: 'down_is_better', source: 'UNODC & WHO via WDI' },
  {
    id: 'military_expenditure_per_capita',
    name: 'Military expenditure per capita (constant 2020 USD)',
    domain: 'Safety & Care',
    unit: 'USD per person',
    direction: 'up_is_better',
    source: 'SIPRI via World Bank WDI',
    dataPath: '/data/military_expenditure_per_capita_constant_usd.json',
  },
  { id: 'firearm_stock_per_100', name: 'Firearm stock per 100 residents', domain: 'Safety & Care', unit: 'firearms per 100 residents', direction: 'up_is_better', source: 'Small Arms Survey; World Bank' },
]
