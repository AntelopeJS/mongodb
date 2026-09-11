import { antelopeKnipConfig } from "@antelopejs/tooling-configs/knip";

export default antelopeKnipConfig({
  // `ajs` comes from @antelopejs/core, which CI installs globally rather than
  // pulling the whole CLI into every module's dependency tree.
  ignoreBinaries: ["ajs"],
  ignoreDependencies: [
    // emitDecoratorMetadata makes tsc emit Reflect.metadata calls, so the
    // polyfill has to be loaded at runtime even though nothing imports it.
    "reflect-metadata",
    // Mocha's globals, supplied to the suites `ajs module test` runs.
    "@types/mocha",
  ],
});
