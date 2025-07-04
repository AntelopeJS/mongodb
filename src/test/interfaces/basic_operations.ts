import { Database } from '@ajs/database/beta';
import { expect } from 'chai';
import { vehicles, Vehicle } from '../datasets/vehicles';

const db = Database<{ [table]: Vehicle }>('test-basic-operations');

const table = 'test-table';

let insertedKeys: string[] = [];

describe('Basic Operations', () => {
  it('Insert', InsertTest);
  it('Get', GetTest);
  it('Get All', GetAllTest);
  it('Get By Index', async () => {});
  it('Update', UpdateTest);
  it('Replace', ReplaceTest);
  it('Delete', DeleteTest);
});

async function InsertTest() {
  const response = await db.table(table).insert(vehicles).run();
  expect(response).to.have.property('inserted', vehicles.length);
  expect(response).to.have.property('generated_keys');
  expect(response.generated_keys).to.be.an('object');

  const keys = Object.values(response.generated_keys ?? {});
  expect(keys).to.have.lengthOf(vehicles.length);
  keys.forEach((val) => {
    expect(val).to.be.a('string');
  });
  insertedKeys = keys;
}

async function GetTest() {
  for (const key of insertedKeys) {
    const result = await db.table(table).get(key).run();

    validateDocumentStructure(result, key);
    validateDocumentContent(result);
  }
}

function findOriginalTestData(doc: any): Vehicle | undefined {
  return vehicles.find(
    (item) =>
      item.car === doc.car &&
      item.manufactured.getTime() === doc.manufactured.getTime() &&
      item.price === doc.price &&
      item.isElectric === doc.isElectric &&
      Number(item.kilometers) === Number(doc.kilometers),
  );
}

function validateDocumentStructure(result: any, expectedId: string) {
  expect(result).to.be.an('object');
  expect(result).to.have.property('_id', expectedId).and.to.be.a('string');
  expect(result).to.have.property('car').and.to.be.a('string');
  expect(result).to.have.property('manufactured').and.to.be.a('Date');
  expect(result).to.have.property('price').and.to.be.a('number');
  expect(result).to.have.property('isElectric').and.to.be.a('boolean');
  expect(result).to.have.property('kilometers').and.to.be.a('number');
}

function validateDocumentContent(result: any) {
  const originalData = findOriginalTestData(result);
  expect(originalData).to.not.equal(undefined);
  expect(result).to.deep.include({
    car: originalData!.car,
    price: originalData!.price,
    isElectric: originalData!.isElectric,
    kilometers: Number(originalData!.kilometers),
  });
  expect(result.manufactured.getTime()).to.equal(originalData!.manufactured.getTime());
}

async function GetAllTest() {
  const result = await db.table(table).getAll('isElectric', false).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);

  for (const doc of result) {
    validateDocumentStructure(doc, (doc as any)._id);
    expect(doc.isElectric).to.equal(false);

    const originalData = findOriginalTestData(doc);
    expect(originalData).to.not.equal(undefined);
  }
}

async function UpdateTest() {
  const result = await db.table(table).get(insertedKeys[0]).update({ price: 4000 }).run();

  expect(result).to.have.property('acknowledged', true);
  expect(result).to.have.property('modifiedCount', 1);
  expect(result).to.have.property('matchedCount', 1);
  expect(result).to.have.property('upsertedCount', 0);

  vehicles[0].price = 4000;
  const doc = await db.table(table).get(insertedKeys[0]).run();
  expect(doc).to.not.equal(undefined);
  validateDocumentStructure(doc!, insertedKeys[0]);
  validateDocumentContent(doc!);
  expect(doc!.price).to.equal(vehicles[0].price);
}

async function ReplaceTest() {
  const replacementData = {
    car: 'Tesla',
    manufactured: new Date('2023-01-01'),
    price: 50000,
    isElectric: true,
    kilometers: BigInt(100000),
  };

  const result = await db.table(table).get(insertedKeys[1]).update(replacementData).run();

  expect(result).to.have.property('acknowledged', true);
  expect(result).to.have.property('modifiedCount', 1);
  expect(result).to.have.property('matchedCount', 1);
  expect(result).to.have.property('upsertedCount', 0);

  const doc = await db.table(table).get(insertedKeys[1]).run();
  expect(doc).to.not.equal(undefined);
  validateDocumentStructure(doc!, insertedKeys[1]);
  expect(doc!.car).to.equal(replacementData.car);
  expect(doc!.price).to.equal(replacementData.price);
  expect(doc!.isElectric).to.equal(replacementData.isElectric);
  expect(doc!.kilometers).to.equal(Number(replacementData.kilometers));
}

async function DeleteTest() {
  const result = await db.table(table).get(insertedKeys[2]).delete().run();

  expect(result).to.have.property('acknowledged', true);
  expect(result).to.have.property('deletedCount', 1);

  const doc = await db.table(table).get(insertedKeys[2]).run();
  expect(doc).to.equal(undefined);
}
