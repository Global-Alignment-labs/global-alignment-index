const { readdir, readFile } = require('node:fs/promises');
const { join, extname, basename } = require('node:path');

async function run() {
  const dataDir = 'public/data';
  const files = await readdir(dataDir);
  const datasetFiles = files
    .filter((f) => extname(f) === '.json')
    .filter((f) => !['sources.json', 'metrics_registry.json'].includes(f));

  let manifest = {};
  try {
    const text = await readFile(join(dataDir, 'sources.json'), 'utf8');
    manifest = JSON.parse(text);
  } catch (err) {
    console.error('Failed to read public/data/sources.json');
    process.exit(1);
  }

  const idsFromFiles = datasetFiles.map((f) => basename(f, '.json'));
  const missing = idsFromFiles.filter((id) => !manifest[id]);

  if (missing.length) {
    console.error('Missing manifest entries for:', missing.join(', '));
    process.exit(1);
  }

  console.log(`Validated: ${idsFromFiles.length} data files present and all have manifest entries.`);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});