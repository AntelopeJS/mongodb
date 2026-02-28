import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../../../datasets/users';

const tableName = 'test-table';
const schema = new Schema<{ [tableName]: User }>('test-filters', { [tableName]: User });
const table = schema.default.table(tableName);

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
  const response = await table.insert(testData).run();
  expect(response).to.be.an('array');

  expect(response).to.have.lengthOf(testData.length);
  response.forEach((val) => {
    expect(val).to.be.a('string');
  });
  insertedKeys = response;
}

async function FilterByStringEquality() {
  const result = await table.filter((doc) => doc.key('department').eq('Development')).run();

  expect(result).to.be.an('array');
  const expectedCount = testData.filter((user) => user.department === 'Development').length;
  expect(result).to.have.lengthOf(expectedCount);

  result.forEach((doc) => {
    expect(doc.department).to.equal('Development');
  });

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Antoine', 'Camille', 'Emilie']);
}

async function FilterByNumberComparison() {
  const result = await table.filter((doc) => doc.key('age').gt(25)).run();

  expect(result).to.be.an('array');
  const expectedCount = testData.filter((user) => user.age > 25).length;
  expect(result).to.have.lengthOf(expectedCount);

  result.forEach((doc) => {
    expect(doc.age).to.be.greaterThan(25);
  });

  const ages = result.map((doc) => doc.age).sort();
  expect(ages).to.deep.equal([28, 30, 35]);
}

async function FilterByBoolean() {
  const result = await table.filter((doc) => doc.key('isActive').eq(true)).run();

  expect(result).to.be.an('array');
  const expectedCount = testData.filter((user) => user.isActive === true).length;
  expect(result).to.have.lengthOf(expectedCount);

  result.forEach((doc) => {
    expect(doc.isActive).to.equal(true);
  });

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Antoine', 'Camille', 'Dominique', 'Emilie']);
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await table.get(key).delete().run();
  }
}
