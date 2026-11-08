import type { VerifyConfig } from "@antelopejs/tooling-configs/verify";

import { catalogJourneys } from "./journeys-catalog.ts";
import { externalJourneys } from "./journeys-external.ts";
import { indexJourneys } from "./journeys-indexes.ts";
import { moduleJourneys } from "./journeys-module.ts";
import { queryJourneys } from "./journeys-queries.ts";
import { smokeJourneys } from "./journeys-smoke.ts";
import { MONGO_IMAGE, MONGO_SERVICE, SERVICE, ownedConfig } from "./mongo.ts";

const FORMAL_QUERY_INDEX_REBUILD = ".agents/skills/formal-query-index-rebuild";

const config: VerifyConfig = {
  doors: {
    data: {
      globs: [
        "src/connection.ts",
        "src/implementations/database/schema.ts",
        "src/implementations/database/selection.ts",
        "src/implementations/database/atomic.ts",
        "src/implementations/database/instances.ts",
        "src/implementations/database/external-writes.ts",
      ],
      funnels: [
        "src/implementations/database/selection.ts",
        "src/implementations/database/atomic.ts",
        "src/implementations/database/external-writes.ts",
      ],
      why: "these modules create collections, drop and rebuild their indexes, and decide which collection and instance a document is written to: a change that drops the wrong index or stamps the wrong instance leaves a client's database with duplicates or documents an upgrade cannot find",
    },
    format: {
      globs: [
        "src/implementations/database/indexes.ts",
        "src/implementations/database/utils.ts",
      ],
      funnels: [],
      why: "the `<schema>__<table>` collection names, the `_instance` field, the `<index>__i` index names and the `__antelope_instances` bookkeeping collection are the layout every stored database already has: an upgrade that renames them stops finding what the last release wrote",
    },
    contract: {
      globs: ["package.json", "src/config.ts"],
      funnels: [],
      why: "the config keys (`url`, `database`, `options`, `mode`, `statementTimeoutMs`, `catalog`) and the interfaces listed under `antelopeJs.implements`",
    },
    outbound: null,
    exposure: null,
    publish: {
      globs: [
        "package.json",
        ".release-it.json",
        "changelogen.config.json",
        ".github/workflows/release.yml",
      ],
      funnels: [],
      why: "the package name and release workflow claim @antelopejs/mongodb on npm",
    },
  },
  userFacing: ["src/*.ts", "src/implementations/**", "docs/**/*.md"],
  setup: [
    ["pnpm", "run", "build"],
    ["docker", "build", "--quiet", "--tag", MONGO_IMAGE, ".agents/skills/verify-module/mongo"],
  ],
  services: { [SERVICE]: MONGO_SERVICE },
  moduleConfig(services) {
    const mongo = services.find((service) => service.name === SERVICE);
    if (mongo === undefined) throw new Error(`no ${SERVICE} service in this run`);
    return ownedConfig(mongo);
  },
  journeys: { ...smokeJourneys, ...moduleJourneys, ...queryJourneys, ...indexJourneys, ...externalJourneys, ...catalogJourneys },
  formal: [
    {
      name: "formal-query-index-rebuild",
      covers: [
        "src/schema-initialization.ts",
        "src/connection.ts",
        "src/implementations/database/schema.ts",
        "src/implementations/database/query.ts",
        "src/implementations/database/selection.ts",
      ],
      files: [`${FORMAL_QUERY_INDEX_REBUILD}/**`],
      command: ["node", `${FORMAL_QUERY_INDEX_REBUILD}/check.mjs`, "--full"],
      baseCommand: ["node", `${FORMAL_QUERY_INDEX_REBUILD}/check.mjs`],
      violation: /^(?:finding|BROKEN|HOLDS) +(\w+)/gm,
      passing: /(?<=^holds +(?:\w+, )*)(\w+)(?=,| \()/gm,
      behaviour: { kind: "emit" },
    },
  ],
};

export default config;
