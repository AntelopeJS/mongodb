#!/usr/bin/env node
/**
 * Compiles this folder with the `src/` it imports into a scratch folder and
 * runs `check.ts` there, passing the arguments through. Exits as the check
 * does: 0 when every rule matches the record in SKILL.md, 1 when a rule
 * changed, 2 when the check could not run. SIGINT and SIGTERM stop the
 * check and remove the scratch folder too.
 */
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const TOOL_ERROR = 2;
const skill = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(skill, "../../..");
const out = mkdtempSync(path.join(tmpdir(), "formal-query-index-rebuild-"));

function removeScratch() {
  rmSync(out, { recursive: true, force: true });
}

function compile() {
  symlinkSync(path.join(root, "node_modules"), path.join(out, "node_modules"), "dir");
  const tsc = path.join(root, "node_modules", ".bin", "tsc");
  const compiled = spawnSync(tsc, ["-p", path.join(skill, "tsconfig.json"), "--outDir", out], {
    encoding: "utf8",
  });
  if (compiled.status !== 0) {
    console.error(`tsc failed:\n${compiled.stdout}${compiled.stderr}`);
  }
  return compiled.status === 0;
}

if (compile()) {
  const entry = path.join(out, path.relative(root, skill), "check.js");
  const child = spawn(process.execPath, [entry, ...process.argv.slice(2)], { stdio: "inherit" });
  const stopOn = (/** @type {NodeJS.Signals} */ signal, /** @type {number} */ code) =>
    process.on(signal, () => {
      child.kill(signal);
      removeScratch();
      process.exit(code);
    });
  stopOn("SIGINT", 130);
  stopOn("SIGTERM", 143);
  child.on("exit", (code) => {
    removeScratch();
    process.exit(code ?? TOOL_ERROR);
  });
} else {
  removeScratch();
  process.exit(TOOL_ERROR);
}
