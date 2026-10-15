// Local UTF-8 documents -> durable read/index checkpoints -> searchable JSON.
// Search uses ASCII letters/digits; original Unicode text is retained exactly.
import { open, readFile, rename, unlink } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { basename, dirname, isAbsolute, join, resolve } from 'node:path';

export const HANDLER = 'document.index.v1';

export function indexInputs({ documents, output_path }) {
  const absoluteFile = value => typeof value === 'string' && isAbsolute(value) && value !== dirname(value);
  if (!Array.isArray(documents) || !documents.length || !documents.every(absoluteFile)) {
    throw new Error('documents must be a nonempty list of absolute local file paths');
  }
  documents = documents.map(path => resolve(path));
  if (new Set(documents).size !== documents.length) throw new Error('document paths must be unique');
  if (!absoluteFile(output_path)) {
    throw new Error('output_path must be an absolute local file path distinct from the documents');
  }
  output_path = resolve(output_path);
  if (!absoluteFile(output_path) || documents.includes(output_path)) {
    throw new Error('output_path must be an absolute local file path distinct from the documents');
  }
  return { documents, output_path };
}

export function tokenize(text) {
  return [...new Set((text.match(/[A-Za-z0-9]+/g) ?? []).map(word => word.toLowerCase()))].sort();
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    // Build sorted keys explicitly: JSON.stringify reorders numeric object keys.
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

export async function publish(output_path, documents) {
  const terms = Object.create(null);
  for (const document of documents) for (const token of document.tokens) {
    (terms[token] ??= []).push(document.id);
  }
  const text = canonicalJson({ documents, terms }) + '\n';
  const temporary = join(dirname(output_path), `.${basename(output_path)}.${randomUUID()}.tmp`);
  let file;
  try {
    file = await open(temporary, 'wx', 0o600);
    await file.writeFile(text, 'utf8');
    await file.sync();
    await file.close(); file = undefined;
    await rename(temporary, output_path); // Replay publishes identical bytes to the same path.
    return { output_path, documents: documents.length, terms: Object.keys(terms).length };
  } finally {
    try { await file?.close(); } finally { await unlink(temporary).catch(error => { if (error.code !== 'ENOENT') throw error; }); }
  }
}

export function search(index, query) {
  if (typeof query !== 'string') throw new Error('query must be a string');
  const tokens = tokenize(query);
  if (!tokens.length) return [];
  const ids = token => Object.hasOwn(index.terms, token) ? index.terms[token] : [];
  let matches = new Set(ids(tokens[0]));
  for (const token of tokens.slice(1)) {
    const next = new Set(ids(token));
    matches = new Set([...matches].filter(id => next.has(id)));
  }
  return index.documents.filter(document => matches.has(document.id));
}

export async function indexDocuments(ctx, inputs) {
  const validated = indexInputs(inputs), documents = [];
  for (const [index, path] of validated.documents.entries()) {
    const number = index + 1;
    const text = await ctx.step(`read-${number}`, async () =>
      new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(await readFile(path)));
    documents.push(await ctx.step(`index-${number}`, () => ({
      id: `doc-${number}`, path, text, tokens: tokenize(text),
    })));
  }
  return ctx.step('publish', () => publish(validated.output_path, documents));
}

export const HANDLERS = { [HANDLER]: indexDocuments };
