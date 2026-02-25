import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { vehicles, Vehicle } from '../../../datasets/vehicles';

const tableName = 'test-table';
const schema = new Schema<{ [tableName]: Vehicle }>('test-between-operations', { [tableName]: Vehicle });
const table = schema.default.table(tableName);

let insertedKeys: string[] = [];

describe('Between Operations', () => {
  before(async () => {
    await table.delete().run();
  });

  after(async () => {
    await table.delete().run();
  });

  it('Insert Test Data', InsertTestData);
  it('Between Inclusive Low Exclusive High', BetweenInclusiveExclusive);
  it('Between All', BetweenAll);
  it('Between Empty', BetweenEmpty);
  it('Between Chained With Filter', BetweenChainedWithFilter);
  it('Cleanup', CleanupTest);
});

async function InsertTestData() {
  const response = await table.insert(vehicles).run();
  expect(response).to.be.an('array');
  expect(response).to.have.lengthOf(vehicles.length);
  response.forEach((val) => {
    expect(val).to.be.a('string');
  });
  insertedKeys = response;
}

async function BetweenInclusiveExclusive() {
  const result = await table.between('price', -1000, 3000).run();
  expect(result).to.be.an('array');
  const expectedPrices = vehicles.filter((v) => v.price >= -1000 && v.price < 3000).map((v) => v.price);
  expect(result).to.have.lengthOf(expectedPrices.length);
  result.forEach((doc) => {
    expect(doc.price).to.be.gte(-1000);
    expect(doc.price).to.be.lt(3000);
  });
}

async function BetweenAll() {
  const result = await table.between('price', -2000, 5000).run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(vehicles.length);
}

async function BetweenEmpty() {
  const result = await table.between('price', 5000, 10000).run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(0);
}

async function BetweenChainedWithFilter() {
  const result = await table
    .between('price', -2000, 5000)
    .filter((doc) => doc.key('isElectric').eq(true))
    .run();
  expect(result).to.be.an('array');
  const expected = vehicles.filter((v) => v.price >= -2000 && v.price < 5000 && v.isElectric);
  expect(result).to.have.lengthOf(expected.length);
  result.forEach((doc) => {
    expect(doc.isElectric).to.equal(true);
  });
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await table.get(key).delete().run();
  }
}
