import { expect } from "chai";
import type { CollectionInfo, Document } from "mongodb";

import { ValidatorOf } from "../implementations/database/external-probe";

function listed(validator?: Document): CollectionInfo {
  return {
    name: "customers",
    type: "collection",
    options: validator === undefined ? {} : { validator },
  };
}

describe("the $jsonSchema validator of a listed collection", () => {
  it("reads a validator whose parts have their MongoDB types", () => {
    const schema = {
      description: "Customers",
      required: ["name"],
      properties: {
        name: { bsonType: "string", description: "The name" },
        points: { bsonType: ["int", "long"] },
        segment: { enum: ["a", "b"] },
      },
    };

    expect(ValidatorOf(listed({ $jsonSchema: schema }))).to.deep.equal(schema);
  });

  it("reads no validator from a collection without one", () => {
    expect(ValidatorOf(listed())).to.equal(undefined);
    expect(ValidatorOf(listed({ $expr: { $gt: ["$points", 0] } }))).to.equal(
      undefined,
    );
  });

  for (const [part, schema] of [
    ["description", { description: 5 }],
    ["required list", { required: "name" }],
    ["required name", { required: [1] }],
    ["properties", { properties: "name" }],
    ["property type", { properties: { name: { bsonType: 3 } } }],
    ["property enum", { properties: { name: { enum: "a" } } }],
    ["property description", { properties: { name: { description: 1 } } }],
  ] as const) {
    it(`reads no validator when its ${part} has the wrong type`, () => {
      expect(ValidatorOf(listed({ $jsonSchema: schema }))).to.equal(undefined);
    });
  }
});
