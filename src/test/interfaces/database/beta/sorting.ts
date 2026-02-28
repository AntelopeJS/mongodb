import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../../../datasets/users';

const tableName = 'test-table';
const schema = new Schema<{ [tableName]: User }>('test-sorting', { [tableName]: User });
const table = schema.default.table(tableName);

// Utiliser le dataset unifié
const testData = getUniqueUsers();

let insertedKeys: string[] = [];

describe('Sorting Operations', () => {
  it('Insert Test Data', InsertTestData);
  it('Sort by String Ascending', SortByStringAscending);
  it('Sort by String Descending', SortByStringDescending);
  it('Sort by Number Ascending', SortByNumberAscending);
  it('Sort by Number Descending', SortByNumberDescending);
  it('Sort by Boolean Ascending', SortByBooleanAscending);
  it('Sort by Boolean Descending', SortByBooleanDescending);
  it('Sort by Date Ascending', SortByDateAscending);
  it('Sort by Date Descending', SortByDateDescending);
  it('Sort by Multiple Fields', SortByMultipleFields);
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

async function SortByStringAscending() {
  const result = await table.orderBy('name', 'asc').run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  const expectedOrder = ['Alice', 'Antoine', 'Camille', 'Dominique', 'Emilie'];
  result.forEach((doc, index) => {
    expect(doc.name).to.equal(expectedOrder[index]);
  });
}

async function SortByStringDescending() {
  const result = await table.orderBy('name', 'desc').run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  const expectedOrder = ['Emilie', 'Dominique', 'Camille', 'Antoine', 'Alice'];
  result.forEach((doc, index) => {
    expect(doc.name).to.equal(expectedOrder[index]);
  });
}

async function SortByNumberAscending() {
  const result = await table.orderBy('age', 'asc').run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  const expectedOrder = [22, 25, 28, 30, 35];
  result.forEach((doc, index) => {
    expect(doc.age).to.equal(expectedOrder[index]);
  });
}

async function SortByNumberDescending() {
  const result = await table.orderBy('salary', 'desc').run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  const expectedOrder = [90000, 60000, 50000, 0, -15000];
  result.forEach((doc, index) => {
    expect(doc.salary).to.equal(expectedOrder[index]);
  });
}

async function SortByBooleanAscending() {
  const result = await table.orderBy('isActive', 'asc').run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  const falseCount = testData.filter((item) => !item.isActive).length;

  result.forEach((doc, index) => {
    if (index < falseCount) {
      expect(doc.isActive).to.equal(false);
    } else {
      expect(doc.isActive).to.equal(true);
    }
  });
}

async function SortByBooleanDescending() {
  const result = await table.orderBy('isActive', 'desc').run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  const trueCount = testData.filter((item) => item.isActive).length;

  result.forEach((doc, index) => {
    if (index < trueCount) {
      expect(doc.isActive).to.equal(true);
    } else {
      expect(doc.isActive).to.equal(false);
    }
  });
}

async function SortByDateAscending() {
  const result = await table.orderBy('createdAt', 'asc').run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  const expectedDates = [
    new Date('2021-12-05'),
    new Date('2022-06-20'),
    new Date('2023-01-15'),
    new Date('2023-08-30'),
    new Date('2024-03-10'),
  ];

  result.forEach((doc, index) => {
    expect(doc.createdAt!.getTime()).to.equal(expectedDates[index].getTime());
  });
}

async function SortByDateDescending() {
  const result = await table.orderBy('createdAt', 'desc').run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  const expectedDates = [
    new Date('2024-03-10'),
    new Date('2023-08-30'),
    new Date('2023-01-15'),
    new Date('2022-06-20'),
    new Date('2021-12-05'),
  ];

  result.forEach((doc, index) => {
    expect(doc.createdAt!.getTime()).to.equal(expectedDates[index].getTime());
  });
}

async function SortByMultipleFields() {
  const result = await table.orderBy('name', 'asc').orderBy('age', 'desc').run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  const expectedOrder = [
    { name: 'Dominique', age: 35 },
    { name: 'Alice', age: 30 },
    { name: 'Emilie', age: 28 },
    { name: 'Antoine', age: 25 },
    { name: 'Camille', age: 22 },
  ];

  result.forEach((doc, index) => {
    expect(doc.name).to.equal(expectedOrder[index].name);
    expect(doc.age).to.equal(expectedOrder[index].age);
  });
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await table.get(key).delete().run();
  }
}
