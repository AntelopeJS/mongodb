import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { vehicles, Vehicle } from '../../../datasets/vehicles';

const tableName = 'test-table';
const schema = new Schema<{ [tableName]: Vehicle }>('test-aggregation-operations', { [tableName]: Vehicle });
const table = schema.default.table(tableName);

let insertedKeys: string[] = [];

describe('Aggregation & Projection Operations', () => {
  before(async () => {
    await table.delete().run();
  });

  after(async () => {
    await table.delete().run();
  });

  it('Insert Test Data', InsertTestData);
  it('Count All', CountAll);
  it('Count Field', CountField);
  it('Sum', Sum);
  it('Avg', Avg);
  it('Min', Min);
  it('Max', Max);
  it('Nth', Nth);
  it('Slice', Slice);
  it('Pluck', Pluck);
  it('Without', Without);
  it('Distinct', Distinct);
  it('Distinct Without Field', DistinctWithoutField);
  it('Cursor', CursorTest);
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

async function CountAll() {
  const result = await table.count().run();
  expect(result).to.be.a('number');
  expect(result).to.equal(vehicles.length);
}

async function CountField() {
  const result = await table.count('car').run();
  expect(result).to.be.a('number');
  const expectedDistinctCars = new Set(vehicles.map((v) => v.car)).size;
  expect(result).to.equal(expectedDistinctCars);
}

async function Sum() {
  const result = await table.sum('price').run();
  expect(result).to.be.a('number');
  const expectedSum = vehicles.reduce((acc, v) => acc + v.price, 0);
  expect(result).to.equal(expectedSum);
}

async function Avg() {
  const result = await table.avg('price').run();
  expect(result).to.be.a('number');
  const expectedAvg = vehicles.reduce((acc, v) => acc + v.price, 0) / vehicles.length;
  expect(result).to.be.closeTo(expectedAvg, 0.01);
}

async function Min() {
  const result = await table.min('price').run();
  expect(result).to.be.a('number');
  const expectedMin = Math.min(...vehicles.map((v) => v.price));
  expect(result).to.equal(expectedMin);
}

async function Max() {
  const result = await table.max('price').run();
  expect(result).to.be.a('number');
  const expectedMax = Math.max(...vehicles.map((v) => v.price));
  expect(result).to.equal(expectedMax);
}

async function Nth() {
  const sortedVehicles = [...vehicles].sort((a, b) => a.price - b.price);
  const result = await table.orderBy('price', 'asc').nth(0).run();
  expect(result).to.be.an('object');
  expect(result).to.not.be.an('array');
  expect(result).to.have.property('price', sortedVehicles[0].price);
}

async function Slice() {
  const sortedVehicles = [...vehicles].sort((a, b) => a.price - b.price);
  const result = await table.orderBy('price', 'asc').slice(1, 2).run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);
  expect(result[0].price).to.equal(sortedVehicles[1].price);
  expect(result[1].price).to.equal(sortedVehicles[2].price);
}

async function Pluck() {
  const result = await table.pluck('car', 'price').run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(vehicles.length);
  result.forEach((doc: any) => {
    expect(doc).to.have.property('car');
    expect(doc).to.have.property('price');
    expect(doc).to.not.have.property('isElectric');
    expect(doc).to.not.have.property('kilometers');
    expect(doc).to.not.have.property('manufactured');
  });
}

async function Without() {
  const result = await table.without('kilometers').run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(vehicles.length);
  result.forEach((doc: any) => {
    expect(doc).to.not.have.property('kilometers');
    expect(doc).to.have.property('car');
    expect(doc).to.have.property('price');
  });
}

async function Distinct() {
  const result = await table.distinct('isElectric').run();
  expect(result).to.be.an('array');
  const expectedDistinct = [...new Set(vehicles.map((v) => v.isElectric))];
  expect(result).to.have.lengthOf(expectedDistinct.length);
  expectedDistinct.forEach((val) => {
    expect(result).to.include(val);
  });
}

async function DistinctWithoutField() {
  const result = await table.distinct().run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(vehicles.length);
  result.forEach((doc: any) => {
    expect(doc).to.have.property('car');
    expect(doc).to.have.property('price');
    expect(doc).to.have.property('isElectric');
  });
  const cars = result.map((doc: any) => doc.car).sort();
  const expectedCars = vehicles.map((v) => v.car).sort();
  expect(cars).to.deep.equal(expectedCars);
}

async function CursorTest() {
  const sortedVehicles = [...vehicles].sort((a, b) => a.price - b.price);
  const cursor = table.orderBy('price', 'asc').cursor();

  const results: any[] = [];
  let next = await cursor.next();
  while (!next.done) {
    results.push(next.value);
    next = await cursor.next();
  }

  expect(results).to.have.lengthOf(vehicles.length);
  results.forEach((doc, i) => {
    expect(doc).to.have.property('price', sortedVehicles[i].price);
  });
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await table.get(key).delete().run();
  }
}
