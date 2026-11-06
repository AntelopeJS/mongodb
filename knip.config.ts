import { antelopeKnipConfig } from "@antelopejs/tooling-configs/knip";

export default antelopeKnipConfig({
  entry: [
    "src/test-external/**/*.test.ts",
    ".agents/skills/formal-query-index-rebuild/check.{mjs,ts}",
  ],
  ignoreDependencies: ["reflect-metadata", "@types/mocha"],
});
