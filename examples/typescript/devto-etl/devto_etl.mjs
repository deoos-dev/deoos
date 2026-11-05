// Fetch DEV articles and atomically replace a CSV on the worker's local disk.
import { open, rename, unlink } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { basename, dirname, isAbsolute, join } from 'node:path';

export const HANDLER = 'devto.etl.v1';
export const SOURCE = 'https://dev.to/api/articles';
export const FIELDS = ['id', 'title', 'published_at', 'url', 'comments_count',
  'positive_reactions_count', 'tag_list', 'user.username'];

export function etlInputs({ output_path, source = SOURCE, pages = 3, per_page = 30 }) {
  if (typeof output_path !== 'string' || !isAbsolute(output_path) || output_path === dirname(output_path)) {
    throw new Error('output_path must be an absolute local file path');
  }
  if (typeof source !== 'string') throw new Error('source must be an HTTP(S) URL');
  const url = new URL(source);
  if (!['http:', 'https:'].includes(url.protocol) || !url.hostname || url.username
      || url.password || url.search || url.hash) {
    throw new Error('source must be an HTTP(S) URL without credentials, query, or fragment');
  }
  for (const [name, value] of Object.entries({ pages, per_page })) {
    if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive safe integer`);
  }
  return { output_path, source, pages, per_page };
}

export async function fetchPage(source, page, per_page) {
  const response = await fetch(`${source}?${new URLSearchParams({ page, per_page })}`, {
    headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(30000),
  });
  if (!response.ok) throw new Error(`DEV HTTP ${response.status}`);
  const articles = await response.json();
  if (!Array.isArray(articles) || articles.some(article => !article || typeof article !== 'object' || Array.isArray(article))) {
    throw new Error('articles response must be an array of objects');
  }
  return articles;
}

export function normalize(pages) {
  const rows = [], seen = new Set();
  for (const articles of pages) for (const article of articles) {
    if (!Number.isSafeInteger(article.id) || article.id < 1) throw new Error('article id must be a positive safe integer');
    if (seen.has(article.id)) continue; // Pagination overlap: the first captured article wins.
    seen.add(article.id);
    const user = article.user ?? {};
    if (typeof user !== 'object' || Array.isArray(user)) throw new Error('article user must be an object or null');
    const row = Object.fromEntries(FIELDS.slice(0, -1).map(field => [field, article[field] ?? null]));
    row['user.username'] = user.username ?? null;
    rows.push(row);
  }
  return rows;
}

export function csvValue(value) {
  return value == null ? '' : typeof value === 'object' ? JSON.stringify(value) : String(value);
}

export async function writeCsv(output_path, rows) {
  const cell = value => { const text = csvValue(value); return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text; };
  const text = [FIELDS.join(','), ...rows.map(row => FIELDS.map(field => cell(row[field])).join(','))].join('\r\n') + '\r\n';
  const temporary = join(dirname(output_path), `.${basename(output_path)}.${randomUUID()}.tmp`);
  let file;
  try {
    file = await open(temporary, 'wx', 0o600);
    await file.writeFile(text, 'utf8');
    await file.sync();
    await file.close(); file = undefined;
    await rename(temporary, output_path); // Replay replaces the same destination with identical bytes.
    return { output_path, articles: rows.length };
  } finally {
    try { await file?.close(); } finally { await unlink(temporary).catch(error => { if (error.code !== 'ENOENT') throw error; }); }
  }
}

export async function etl(ctx, inputs) {
  const validated = etlInputs(inputs), pages = [];
  for (let page = 1; page <= validated.pages; page++) {
    pages.push(await ctx.step(`fetch-page-${page}`, () => fetchPage(validated.source, page, validated.per_page)));
  }
  const rows = await ctx.step('normalize', () => normalize(pages));
  return ctx.step('write-csv', () => writeCsv(validated.output_path, rows));
}

export const HANDLERS = { [HANDLER]: etl };
