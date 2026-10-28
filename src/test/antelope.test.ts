import { MongoMemoryReplSet } from "mongodb-memory-server-core";
import { defineConfig } from "@antelopejs/interface-core/config";

import {
  ACCOUNT_PASSWORD,
  ADMIN_URL_VARIABLE,
  CLIENT_DATABASE,
  SeedClientDatabase,
  WRITER_USER,
} from "../test-external/fixture";

const MONGODB_VERSION = "8.0.8";
const EXTERNAL_MODE = "external";
const ROOT_USER = "root";
const ROOT_PASSWORD = "root-password";
const ADMIN_DATABASE = "admin";

let mongod: MongoMemoryReplSet;

function WithAccount(
  uri: string,
  user: string,
  password: string,
  authSource: string,
): string {
  const target = new URL(uri);
  target.username = user;
  target.password = password;
  target.searchParams.set("authSource", authSource);
  return target.toString();
}

async function ExternalConfig() {
  mongod = await MongoMemoryReplSet.create({
    instanceOpts: [{ args: ["--setParameter", "enableTestCommands=1"] }],
    replSet: {
      count: 1,
      auth: {
        enable: true,
        customRootName: ROOT_USER,
        customRootPwd: ROOT_PASSWORD,
      },
    },
    binary: { version: MONGODB_VERSION },
  });
  const uri = mongod.getUri();
  const adminUrl = WithAccount(uri, ROOT_USER, ROOT_PASSWORD, ADMIN_DATABASE);
  await SeedClientDatabase(adminUrl);
  process.env[ADMIN_URL_VARIABLE] = adminUrl;
  return {
    url: WithAccount(uri, WRITER_USER, ACCOUNT_PASSWORD, CLIENT_DATABASE),
    database: CLIENT_DATABASE,
    mode: EXTERNAL_MODE,
    catalog: { enabled: true },
    options: { monitorCommands: true },
  };
}

async function OwnedConfig() {
  mongod = await MongoMemoryReplSet.create({
    replSet: { count: 1 },
    binary: { version: MONGODB_VERSION },
  });
  return {
    url: mongod.getUri(),
    database: "antelopejs_test",
    options: { monitorCommands: true },
  };
}

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
      const config =
        process.env.MONGO_TEST_MODE === EXTERNAL_MODE
          ? await ExternalConfig()
          : await OwnedConfig();
      return { modules: { local: { config } } };
    },
    async cleanup() {
      await mongod.stop();
    },
  },
});
