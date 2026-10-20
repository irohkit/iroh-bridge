#!/usr/bin/env node
// Lays out the npm packages that carry the native addon prebuilds (iroh-fetch-<platform>) and
// pins them, at iroh-fetch's version, in iroh-fetch's optionalDependencies. npm installs only the
// one whose os/cpu/libc match; iroh-fetch falls back to WASM when none is there.
//
//   node scripts/native-packages.mjs <prebuilds-dir> <out-dir>
//
// <prebuilds-dir> holds iroh-fetch.<platform>.node files (any depth); every platform listed below
// must be present. Writes <out-dir>/<platform>/{package.json,README.md,iroh-fetch.<platform>.node}
// and updates packages/iroh-fetch/package.json in place.
import { readFileSync, writeFileSync, mkdirSync, copyFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

const PLATFORMS = {
  "linux-x64-gnu": { os: "linux", cpu: "x64", libc: "glibc" },
  "linux-arm64-gnu": { os: "linux", cpu: "arm64", libc: "glibc" },
  "linux-x64-musl": { os: "linux", cpu: "x64", libc: "musl" },
  "darwin-x64": { os: "darwin", cpu: "x64" },
  "darwin-arm64": { os: "darwin", cpu: "arm64" },
  "win32-x64-msvc": { os: "win32", cpu: "x64" },
};

const [src, out] = process.argv.slice(2);
if (!src || !out) {
  console.error("usage: node scripts/native-packages.mjs <prebuilds-dir> <out-dir>");
  process.exit(2);
}

const files = new Map();
const walk = (dir) => {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) walk(path);
    else if (/^iroh-fetch\..+\.node$/.test(name)) files.set(name, path);
  }
};
walk(src);

const mainPath = new URL("../packages/iroh-fetch/package.json", import.meta.url);
const main = JSON.parse(readFileSync(mainPath, "utf8"));
const optional = {};
for (const [tag, { os, cpu, libc }] of Object.entries(PLATFORMS)) {
  const file = `iroh-fetch.${tag}.node`;
  const from = files.get(file);
  if (!from) throw new Error(`missing prebuild ${file} in ${src}`);
  const name = `iroh-fetch-${tag}`;
  const dir = join(out, tag);
  mkdirSync(dir, { recursive: true });
  copyFileSync(from, join(dir, file));
  const pkg = {
    name,
    version: main.version,
    description: `Native addon of iroh-fetch for ${tag} (installed automatically by iroh-fetch)`,
    license: main.license,
    author: main.author,
    repository: { ...main.repository, directory: "crates/node-client" },
    homepage: main.homepage,
    main: file,
    files: [file],
    os: [os],
    cpu: [cpu],
    ...(libc && { libc: [libc] }),
    engines: main.engines,
    publishConfig: main.publishConfig,
  };
  writeFileSync(join(dir, "package.json"), JSON.stringify(pkg, null, 2) + "\n");
  writeFileSync(
    join(dir, "README.md"),
    `# ${name}\n\nThe native Node.js addon of [iroh-fetch](https://www.npmjs.com/package/iroh-fetch) for ${tag}.\n` +
      "Don't install it directly: iroh-fetch lists it as an optional dependency and uses it when present.\n",
  );
  optional[name] = main.version;
  console.log(`${name}@${main.version}: ${statSync(from).size} bytes`);
}
main.optionalDependencies = optional;
writeFileSync(mainPath, JSON.stringify(main, null, 2) + "\n");
