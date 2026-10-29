import { antelopeKnipConfig } from "@antelopejs/tooling-configs/knip";

export default antelopeKnipConfig({
  entry: ["src/test-external/**/*.test.ts"],
  ignoreDependencies: ["reflect-metadata", "@types/mocha"],
});
