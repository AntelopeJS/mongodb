import { expect } from "chai";
import { ImplementInterface } from "@antelopejs/interface-core";
import * as queryDeclaration from "@antelopejs/interface-database/query";
import * as schemaDeclaration from "@antelopejs/interface-database/schema";
import * as transactionDeclaration from "@antelopejs/interface-database/transactions";

import * as queryImplementation from "../implementations/database/query";
import * as schemaImplementation from "../implementations/database/schema";
import * as transactionImplementation from "../implementations/database/transactions";

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
    const transactions = await ImplementInterface(
      Promise.resolve(transactionDeclaration),
      Promise.resolve(transactionImplementation),
    );

    expect(query.implementation.RunQuery).to.equal(
      queryImplementation.RunQuery,
    );
    expect(schema.implementation.Schemas).to.equal(
      schemaImplementation.Schemas,
    );
    expect(transactions.implementation.RunInTransaction).to.equal(
      transactionImplementation.RunInTransaction,
    );
  });
});
