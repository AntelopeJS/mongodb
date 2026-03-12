import { Schema } from "@ajs/database/beta";
import { expect } from "chai";
import { Vehicle, vehicles } from "../../../datasets/vehicles";

const tableName = "test-table";
const schema = new Schema<{ [tableName]: Vehicle }>("test-basic-operations", {
  [tableName]: Vehicle,
});
const table = schema.instance("default").table(tableName);

let insertedKeys: string[] = [];
const expectedVehicles: Vehicle[] = vehicles.map((v) => ({ ...v }));

describe("Basic Operations", () => {
  it("Insert", InsertTest);
  it("Insert conflict update (existing doc)", InsertConflictUpdateTest);
  it("Insert conflict replace (existing doc)", InsertConflictReplaceTest);
  it("Insert conflict update (new doc)", InsertConflictUpdateNewDocTest);
  it("Get", GetTest);
  it("Get All", GetAllTest);
  it("Get By Index", GetAllMultiKeyByIndex);
  it("Get All by primary keys", GetAllMultiKeyByPrimaryKey);
  it("Get All With OrderBy", GetAllWithOrderBy);
  it("Update", UpdateTest);
  it("Replace", ReplaceTest);
  it("Delete", DeleteTest);

  before(async () => {
    await schema.createInstance("default").run();
  });

  after(async () => {
    await table.delete().run();
    await schema.destroyInstance("default").run();
  });
});

async function InsertTest() {
  const response = await table.insert(vehicles).run();
  expect(response).to.be.an("array");

  expect(response).to.have.lengthOf(vehicles.length);
  response.forEach((val) => {
    expect(val).to.be.a("string");
  });
  insertedKeys = response;
}

async function InsertConflictUpdateTest() {
  const existingKey = insertedKeys[0];
  const response = await table
    .insert({ _id: existingKey, price: 9999 }, { conflict: "update" })
    .run();

  expect(response).to.be.an("array").with.lengthOf(1);
  expect(response[0]).to.equal(existingKey);

  const doc = await table.get(existingKey).run();
  expect(doc.price).to.equal(9999);
  expect(doc.car).to.equal(expectedVehicles[0].car);

  expectedVehicles[0].price = 9999;
}

async function InsertConflictReplaceTest() {
  const existingKey = insertedKeys[1];
  const replacementData = {
    _id: existingKey,
    car: "BMW",
    manufactured: new Date("2020-06-15"),
    price: 35000,
    isElectric: false,
    kilometers: 50000,
  };

  const response = await table
    .insert(replacementData, { conflict: "replace" })
    .run();

  expect(response).to.be.an("array").with.lengthOf(1);
  expect(response[0]).to.equal(existingKey);

  const doc = await table.get(existingKey).run();
  expect(doc.car).to.equal("BMW");
  expect(doc.price).to.equal(35000);
  expect(doc.isElectric).to.equal(false);
  expect(doc.kilometers).to.equal(50000);

  expectedVehicles[1] = {
    car: "BMW",
    manufactured: new Date("2020-06-15"),
    price: 35000,
    isElectric: false,
    kilometers: 50000,
  };
}

async function InsertConflictUpdateNewDocTest() {
  const newDoc = {
    car: "Audi",
    manufactured: new Date("2022-03-01"),
    price: 45000,
    isElectric: true,
    kilometers: 10000,
  };

  const response = await table
    .insert(newDoc, { conflict: "update" })
    .run();

  expect(response).to.be.an("array").with.lengthOf(1);
  expect(response[0]).to.be.a("string");

  const doc = await table.get(response[0]).run();
  expect(doc.car).to.equal("Audi");
  expect(doc.price).to.equal(45000);

  await table.get(response[0]).delete().run();
}

async function GetTest() {
  for (const key of insertedKeys) {
    const result = await table.get(key).run();

    validateDocumentStructure(result, key);
    validateDocumentContent(result);
  }
}

function findExpectedData(doc: Vehicle): Vehicle | undefined {
  return expectedVehicles.find(
    (item) =>
      item.car === doc.car &&
      item.manufactured.getTime() === doc.manufactured.getTime() &&
      item.price === doc.price &&
      item.isElectric === doc.isElectric &&
      Number(item.kilometers) === Number(doc.kilometers),
  );
}

function validateDocumentStructure(result: Vehicle, expectedId: string) {
  expect(result).to.be.an("object");
  expect(result).to.have.property("_id", expectedId).and.to.be.a("string");
  expect(result).to.have.property("car").and.to.be.a("string");
  expect(result).to.have.property("manufactured").and.to.be.a("Date");
  expect(result).to.have.property("price").and.to.be.a("number");
  expect(result).to.have.property("isElectric").and.to.be.a("boolean");
  expect(result).to.have.property("kilometers").and.to.be.a("number");
}

function validateDocumentContent(result: Vehicle) {
  const expected = findExpectedData(result);
  expect(expected).to.not.equal(undefined);
  expect(result).to.deep.include({
    car: expected?.car,
    price: expected?.price,
    isElectric: expected?.isElectric,
    kilometers: Number(expected?.kilometers),
  });
  expect(result.manufactured.getTime()).to.equal(
    expected?.manufactured.getTime(),
  );
}

async function GetAllTest() {
  const result = await table.getAll(false, "isElectric").run();

  expect(result).to.be.an("array");
  const expectedCount = expectedVehicles.filter((v) => !v.isElectric).length;
  expect(result).to.have.lengthOf(expectedCount);

  for (const doc of result) {
    validateDocumentStructure(doc, doc._id as string);
    expect(doc.isElectric).to.equal(false);

    const expected = findExpectedData(doc);
    expect(expected).to.not.equal(undefined);
  }
}

const TARGET_PRICES = [3000, 0];

async function GetAllMultiKeyByIndex() {
  const result = await table.getAll(TARGET_PRICES, "price").run();

  expect(result).to.be.an("array");
  const expectedCount = expectedVehicles.filter((v) =>
    TARGET_PRICES.includes(v.price),
  ).length;
  expect(result).to.have.lengthOf(expectedCount);

  for (const doc of result) {
    validateDocumentStructure(doc, doc._id as string);
    expect(TARGET_PRICES).to.include(doc.price);
  }
}

async function GetAllMultiKeyByPrimaryKey() {
  const targetKeys = [insertedKeys[0], insertedKeys[2]];
  const result = await table.getAll(targetKeys).run();

  expect(result).to.be.an("array");
  expect(result).to.have.lengthOf(targetKeys.length);

  for (const doc of result) {
    expect(targetKeys).to.include(doc._id);
  }
}

async function GetAllWithOrderBy() {
  const result = await table
    .getAll(false, "isElectric")
    .orderBy("price", "asc")
    .run();

  expect(result).to.be.an("array");
  const expectedCount = expectedVehicles.filter((v) => !v.isElectric).length;
  expect(result).to.have.lengthOf(expectedCount);

  const expectedOrder = ["Peugeot", "BMW"];
  result.forEach((doc, index) => {
    expect(doc.car).to.equal(expectedOrder[index]);
    expect(doc.isElectric).to.equal(false);
  });
}

async function UpdateTest() {
  const result = await table.get(insertedKeys[0]).update({ price: 4000 }).run();

  expect(result).to.equal(1);

  expectedVehicles[0].price = 4000;
  const doc = await table.get(insertedKeys[0]).run();
  expect(doc).to.not.equal(undefined);
  validateDocumentStructure(doc, insertedKeys[0]);
  validateDocumentContent(doc);
  expect(doc.price).to.equal(expectedVehicles[0].price);
}

async function ReplaceTest() {
  const replacementData = {
    car: "Tesla",
    manufactured: new Date("2023-01-01"),
    price: 50000,
    isElectric: true,
    kilometers: 100000,
  };

  const result = await table
    .get(insertedKeys[1])
    .replace(replacementData)
    .run();

  expect(result).to.equal(1);

  const doc = await table.get(insertedKeys[1]).run();
  expect(doc).to.not.equal(undefined);
  validateDocumentStructure(doc, insertedKeys[1]);
  expect(doc.car).to.equal(replacementData.car);
  expect(doc.price).to.equal(replacementData.price);
  expect(doc.isElectric).to.equal(replacementData.isElectric);
  expect(doc.kilometers).to.equal(Number(replacementData.kilometers));
}

async function DeleteTest() {
  const result = await table.get(insertedKeys[2]).delete().run();

  expect(result).to.equal(1);

  const doc = await table.get(insertedKeys[2]).run();
  expect(doc).to.equal(undefined);
}
