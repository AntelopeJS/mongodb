import { defineConfig } from "@antelopejs/interface-core/config";
import { MongoMemoryReplSet } from "mongodb-memory-server-core";

let mongod: MongoMemoryReplSet;

export default defineConfig({
  name: "mongodb-test",
  cacheFolder: ".antelope/cache",
  modules: {
    local: {
      source: { type: "local", path: "." },
    },
  },
  test: {
    folder: "dist/test",
    async setup() {
      mongod = await MongoMemoryReplSet.create({ replSet: { count: 1 } });
      return {
        modules: {
          local: {
            config: { url: mongod.getUri() },
          },
        },
      };
    },
    async cleanup() {
      await mongod.stop();
    },
  },
});
