import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../../../datasets/users';

const tableName = 'test-table';
const schema = new Schema<{ [tableName]: User }>('test-date-operations', { [tableName]: User });
const table = schema.instance('default').table(tableName);

const testData = getUniqueUsers();

let insertedKeys: string[] = [];

describe('Date Operations', () => {
  before(async () => {
    await schema.createInstance('default').run();
  });

  after(async () => {
    await table.delete().run();
    await schema.destroyInstance('default').run();
  });

  it('Insert Test Data', InsertTestData);
  it('Filter by Year', FilterByYear);
  it('Filter by Month', FilterByMonth);
  it('Filter by Day', FilterByDay);
  it('Filter by Year Comparison', FilterByYearComparison);
  it('Filter by During', FilterByDuring);
  it('Filter by Epoch Comparison', FilterByEpochComparison);
  it('Filter by Date Greater Than', FilterByDateGreaterThan);
  it('Filter by Date Less Than', FilterByDateLessThan);
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

async function FilterByYear() {
  const result = await table.filter((doc) => doc.key('createdAt').year().eq(2023)).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Antoine', 'Emilie']);
}

async function FilterByMonth() {
  const result = await table.filter((doc) => doc.key('createdAt').month().eq(1)).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(1);

  expect(result[0].name).to.equal('Antoine');
}

async function FilterByDay() {
  const result = await table.filter((doc) => doc.key('createdAt').day().eq(15)).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(1);

  expect(result[0].name).to.equal('Antoine');
}

async function FilterByYearComparison() {
  const result = await table.filter((doc) => doc.key('createdAt').year().gt(2022)).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Antoine', 'Camille', 'Emilie']);
}

async function FilterByDuring() {
  const result = await table
    .filter((doc) => doc.key('createdAt').during(new Date('2022-01-01'), new Date('2023-12-31')))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Alice', 'Antoine', 'Emilie']);
}

async function FilterByEpochComparison() {
  const cutoff = new Date('2023-06-01').getTime() / 1000;
  const result = await table.filter((doc) => doc.key('createdAt').epoch().gt(cutoff)).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Camille', 'Emilie']);
}

async function FilterByDateGreaterThan() {
  const result = await table.filter((doc) => doc.key('createdAt').gt(new Date('2023-06-01'))).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Camille', 'Emilie']);
}

async function FilterByDateLessThan() {
  const result = await table.filter((doc) => doc.key('createdAt').lt(new Date('2023-01-01'))).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Alice', 'Dominique']);
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await table.get(key).delete().run();
  }
}
