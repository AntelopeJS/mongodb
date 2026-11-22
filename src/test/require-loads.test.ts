import { expect } from "chai";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

const DIST_DIRECTORY = path.resolve(__dirname, "..");
const NATIVE_IMPORT = /\bimport\s*\(/;

async function FilesWithNativeImport(): Promise<string[]> {
  const entries = await readdir(DIST_DIRECTORY, { recursive: true });
  const scripts = entries.filter((entry) => entry.endsWith(".js"));
  const offenders: string[] = [];
  for (const script of scripts) {
    const source = await readFile(path.join(DIST_DIRECTORY, script), "utf8");
    if (NATIVE_IMPORT.test(source)) {
      offenders.push(script);
    }
  }
  return offenders;
}

describe("Compiled module loading", () => {
  it("loads every module through require, so core resolves the interfaces from this module's directory", async () => {
    expect(await FilesWithNativeImport()).to.deep.equal([]);
  });
});
