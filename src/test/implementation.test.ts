import { ImplementInterface } from "@antelopejs/interface-core";
import * as queryDeclaration from "@antelopejs/interface-database/query";
import * as schemaDeclaration from "@antelopejs/interface-database/schema";
import { expect } from "chai";
import * as queryImplementation from "../implementations/database/query";
import * as schemaImplementation from "../implementations/database/schema";

describe("Interface implementation wiring", () => {
  it("passes strict validation with the real declarations", async () => {
    const query = await ImplementInterface(
      Promise.resolve(queryDeclaration),
      Promise.resolve(queryImplementation),
    );
    const schema = await ImplementInterface(
      Promise.resolve(schemaDeclaration),
      Promise.resolve(schemaImplementation),
    );

    expect(query.implementation.RunQuery).to.equal(
      queryImplementation.RunQuery,
    );
    expect(schema.implementation.Schemas).to.equal(
      schemaImplementation.Schemas,
    );
  });
});
