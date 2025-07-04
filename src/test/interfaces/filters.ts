import { Database } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../datasets/users';

const db = Database<{ [table]: User }>('test-filters');

const table = 'test-table';

// Utiliser le dataset unifié
const testData = getUniqueUsers();

let insertedKeys: string[] = [];

describe('Filter Operations', () => {
  it('Insert Test Data', InsertTestData);
  it('Filter by String Equality', FilterByStringEquality);
  it('Filter by Number Comparison', FilterByNumberComparison);
  it('Filter by Boolean', FilterByBoolean);
  it('Cleanup', CleanupTest);
});

async function InsertTestData() {
  const response = await db.table(table).insert(testData).run();
  expect(response).to.have.property('inserted', testData.length);
  expect(response).to.have.property('generated_keys');
  expect(response.generated_keys).to.be.an('object');

  const keys = Object.values(response.generated_keys ?? {});
  expect(keys).to.have.lengthOf(testData.length);
  keys.forEach((val) => {
    expect(val).to.be.a('string');
  });
  insertedKeys = keys;
}

async function FilterByStringEquality() {
  const result = await db
    .table(table)
    .filter((doc) => doc('department').eq('Development'))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((doc) => {
    expect(doc.department).to.equal('Development');
  });

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Antoine', 'Camille', 'Emilie']);
}

async function FilterByNumberComparison() {
  const result = await db
    .table(table)
    .filter((doc) => doc('age').gt(25))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((doc) => {
    expect(doc.age).to.be.greaterThan(25);
  });

  const ages = result.map((doc) => doc.age).sort();
  expect(ages).to.deep.equal([28, 30, 35]);
}

async function FilterByBoolean() {
  const result = await db
    .table(table)
    .filter((doc) => doc('isActive').eq(true))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(4);

  result.forEach((doc) => {
    expect(doc.isActive).to.equal(true);
  });

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Antoine', 'Camille', 'Dominique', 'Emilie']);
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await db.table(table).get(key).delete().run();
  }
}
