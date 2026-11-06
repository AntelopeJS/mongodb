#!/usr/bin/env node
/**
 * Compiles this folder with the `src/` it imports into a scratch folder and
 * runs `check.ts` there, passing the arguments through. Exits as the check
 * does: 0 when every rule matches the record in SKILL.md, 1 when a rule
 * changed, 2 when the check could not run.
 */
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const TOOL_ERROR = 2;
const skill = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(skill, "../../..");
const out = mkdtempSync(path.join(tmpdir(), "formal-query-index-rebuild-"));

function run() {
  symlinkSync(path.join(root, "node_modules"), path.join(out, "node_modules"), "dir");
  const tsc = path.join(root, "node_modules", ".bin", "tsc");
  const compiled = spawnSync(tsc, ["-p", path.join(skill, "tsconfig.json"), "--outDir", out], {
    encoding: "utf8",
  });
  if (compiled.status !== 0) {
    console.error(`tsc failed:\n${compiled.stdout}${compiled.stderr}`);
    return TOOL_ERROR;
  }
  const entry = path.join(out, path.relative(root, skill), "check.js");
  const checked = spawnSync(process.execPath, [entry, ...process.argv.slice(2)], {
    stdio: "inherit",
  });
  return checked.status ?? TOOL_ERROR;
}

try {
  process.exitCode = run();
} finally {
  rmSync(out, { recursive: true, force: true });
}
