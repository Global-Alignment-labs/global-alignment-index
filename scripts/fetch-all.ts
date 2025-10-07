import { run as co2 } from './pipelines/co2.ts';
import { run as life_expectancy } from './pipelines/life_expectancy.ts';
import { run as u5_mortality } from './pipelines/u5_mortality.ts';
import { run as firearm_stock_per_100 } from './pipelines/firearm_stock_per_100.ts';

export async function runAll() {
  const pipelines = [
    { name: 'co2_ppm', run: co2 },
    { name: 'life_expectancy', run: life_expectancy },
    // Under-5 mortality from WDI
    { name: 'u5_mortality', run: u5_mortality },
    { name: 'firearm_stock_per_100', run: firearm_stock_per_100 },
  ];
  for (const p of pipelines) {
    console.log(`[fetch-all] start ${p.name}`);
    const data = await p.run();
    const n = Array.isArray(data) ? data.length : 0;
    console.log(`[fetch-all] done ${p.name} (${n})`);
    if (!Array.isArray(data) || n === 0) {
      console.error(`[fetch-all] ${p.name} returned empty data — failing run`);
      throw new Error(`empty dataset: ${p.name}`);
    }
  }
}

// Node ESM-safe entrypoint check
const isEntry = import.meta.url === new URL(process.argv[1], 'file://').href;
if (isEntry) {
  runAll().catch((err) => {
    console.error('[fetch-all] fatal:', err?.message || err);
    process.exit(1);
  });
}
