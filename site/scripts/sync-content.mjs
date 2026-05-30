import { cp, mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const siteDir = path.resolve(fileURLToPath(import.meta.url), '../..');
const repoRoot = path.resolve(siteDir, '..');
const docsSource = path.join(repoRoot, 'docs');
const docsOut = path.join(siteDir, 'src/content/docs');
const demoSource = path.join(repoRoot, 'demo');
const demoOut = path.join(siteDir, 'public/wasm-demo');

const titleOverrides = new Map([
  ['index.md', 'DuckDB OpenTelemetry Extension'],
  ['guides/index.md', 'How-to Guides'],
  ['examples/otel-demo/index.md', 'OpenTelemetry Demo Example'],
]);

function toPosix(filePath) {
  return filePath.split(path.sep).join('/');
}

function routeForSource(sourceRelative) {
  const parsed = path.posix.parse(sourceRelative);
  const withoutExt = path.posix.join(parsed.dir, parsed.name);
  if (parsed.name === 'README' || parsed.name === 'index') {
    return `/${parsed.dir ? `${parsed.dir}/` : ''}`;
  }
  return `/${withoutExt}/`;
}

function relativeRoute(fromSourceRelative, toSourceRelative) {
  const fromRoute = routeForSource(fromSourceRelative).replace(/\/$/, '') || '/';
  const toRoute = routeForSource(toSourceRelative).replace(/\/$/, '') || '/';
  const relative = path.posix.relative(fromRoute, toRoute);
  return relative ? `${relative}/` : './';
}

function outputRelativeForSource(sourceRelative) {
  const parsed = path.posix.parse(sourceRelative);
  if (parsed.base === 'README.md') {
    return path.posix.join(parsed.dir, 'index.md');
  }
  return sourceRelative;
}

function titleFromPath(sourceRelative) {
  const parsed = path.posix.parse(sourceRelative);
  const base = parsed.name === 'README' || parsed.name === 'index' ? path.posix.basename(parsed.dir) || 'Docs' : parsed.name;
  return base
    .split(/[-_]/g)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function escapeYaml(value) {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

function rewriteMarkdownLinks(markdown, sourceRelative) {
  const sourceDir = path.posix.dirname(sourceRelative);
  return markdown.replace(/(\[[^\]]*\]\()([^)\s]+)(\))/g, (match, prefix, target, suffix) => {
    if (
      target.startsWith('http://') ||
      target.startsWith('https://') ||
      target.startsWith('#') ||
      target.startsWith('mailto:')
    ) {
      return match;
    }

    const [rawPath, hash = ''] = target.split('#');
    if (!rawPath.endsWith('.md')) {
      return match;
    }

    const normalized = path.posix.normalize(path.posix.join(sourceDir, rawPath));
    if (normalized.startsWith('../') || normalized === '..' || path.posix.isAbsolute(normalized)) {
      const repoPath = path.posix.normalize(path.posix.join('docs', sourceDir, rawPath));
      const cleanRepoPath = repoPath.replace(/^(\.\.\/)+/, '');
      return `${prefix}https://github.com/smithclay/duckdb-otlp/blob/main/${cleanRepoPath}${hash ? `#${hash}` : ''}${suffix}`;
    }

    return `${prefix}${relativeRoute(sourceRelative, normalized)}${hash ? `#${hash}` : ''}${suffix}`;
  });
}

async function collectMarkdown(dir, prefix = '') {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const sourcePath = path.join(dir, entry.name);
    const relative = prefix ? path.posix.join(prefix, entry.name) : entry.name;
    if (entry.isDirectory()) {
      files.push(...await collectMarkdown(sourcePath, relative));
    } else if (entry.isFile() && entry.name.endsWith('.md')) {
      files.push(relative);
    }
  }
  return files.sort();
}

async function syncDocs() {
  await rm(docsOut, { recursive: true, force: true });
  await mkdir(docsOut, { recursive: true });

  const files = await collectMarkdown(docsSource);
  for (const sourceRelative of files) {
    const sourcePath = path.join(docsSource, ...sourceRelative.split('/'));
    let markdown = await readFile(sourcePath, 'utf8');
    const headingMatch = markdown.match(/^#\s+(.+?)\s*$/m);
    const outputRelative = outputRelativeForSource(sourceRelative);
    const title = titleOverrides.get(outputRelative) ?? headingMatch?.[1]?.trim() ?? titleFromPath(sourceRelative);

    if (headingMatch) {
      const headingStart = headingMatch.index ?? 0;
      const headingEnd = headingStart + headingMatch[0].length;
      const beforeHeading = markdown.slice(0, headingStart);
      const afterHeading = markdown.slice(headingEnd).replace(/^\n+/, '');
      markdown = `${beforeHeading}${afterHeading}`;
    }

    markdown = rewriteMarkdownLinks(markdown.trimStart(), sourceRelative);

    const outputPath = path.join(docsOut, ...outputRelative.split('/'));
    await mkdir(path.dirname(outputPath), { recursive: true });
    await writeFile(outputPath, `---\ntitle: "${escapeYaml(title)}"\n---\n\n${markdown}`, 'utf8');
  }
}

async function syncDemo() {
  await rm(demoOut, { recursive: true, force: true });
  await mkdir(path.dirname(demoOut), { recursive: true });
  await cp(demoSource, demoOut, { recursive: true });
}

await syncDocs();
await syncDemo();

console.log(`Synced docs to ${toPosix(path.relative(repoRoot, docsOut))}`);
console.log(`Synced demo to ${toPosix(path.relative(repoRoot, demoOut))}`);
