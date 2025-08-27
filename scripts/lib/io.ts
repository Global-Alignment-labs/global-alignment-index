import { promises as fs } from 'node:fs';
import { dirname } from 'node:path';

export async function writeJson(path: string, data: unknown): Promise<void> {
  await fs.mkdir(dirname(path), { recursive: true });
  await fs.writeFile(path, JSON.stringify(data, null, 2));
}
