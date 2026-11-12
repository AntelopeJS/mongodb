import { expect } from "chai";
import { ListRelations } from "@antelopejs/interface-database-catalog";

import { GetOptions, type Options, SetOptions } from "../config";
import { GetDatabase } from "../connection";

const COLLECTION = "catalog_without_auth";
const VIEW = "catalog_without_auth_view";

describe("what the catalog of a MongoDB without access control answers", () => {
  let configured: Options;

  before(async () => {
    const db = await GetDatabase();
    await db.createCollection(COLLECTION);
    await db.createCollection(VIEW, { viewOn: COLLECTION, pipeline: [] });
  });

  beforeEach(() => {
    configured = { ...GetOptions() };
    SetOptions({ ...configured, mode: "external", catalog: { enabled: true } });
  });

  afterEach(() => {
    SetOptions(configured);
  });

  after(async () => {
    const db = await GetDatabase();
    await db.dropCollection(VIEW);
    await db.dropCollection(COLLECTION);
  });

  it("answers every write as allowed on a collection, and none on a view", async () => {
    const relations = await ListRelations();
    const writes = Object.fromEntries(
      relations.map((relation) => [relation.name, relation.writes]),
    );
    expect(writes[COLLECTION]).to.deep.equal({
      insert: true,
      update: true,
      delete: true,
    });
    expect(writes[VIEW]).to.deep.equal({
      insert: false,
      update: false,
      delete: false,
    });
  });
});
