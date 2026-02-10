import { Database, JoinType } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../datasets/users';

const db = Database<{ [table]: User }>('test-change-feeds');

const table = 'test-table';

const testData = getUniqueUsers().filter((user) => user.status);

let insertedKeys: string[] = [];

describe('Change Feeds and Advanced Operations', () => {
  it('Insert Test Data', InsertTestData);
  it('Test Changes Feed Iterator', TestChangesFeedIterator);
  it('Test Changes with Options', TestChangesWithOptions);
  it('Test Changes on Single Document', TestChangesOnSingleDocument);
  it('Test Changes with Filter', TestChangesWithFilter);
  it('Test Changes with Map', TestChangesWithMap);
  it('Test Changes with Pluck', TestChangesWithPluck);
  it('Test Changes with Without', TestChangesWithWithout);
  it('Test Changes with WithFields', TestChangesWithWithFields);
  it('Test Changes with OrderBy', TestChangesWithOrderBy);
  it('Test Changes with Slice', TestChangesWithSlice);
  it('Test Changes with Nth', TestChangesWithNth);
  it('Test Changes with Distinct', TestChangesWithDistinct);
  it('Test Changes with Count', TestChangesWithCount);
  it('Test Changes with Sum', TestChangesWithSum);
  it('Test Changes with Avg', TestChangesWithAvg);
  it('Test Changes with Min', TestChangesWithMin);
  it('Test Changes with Max', TestChangesWithMax);
  it('Test Changes with Group', TestChangesWithGroup);
  it('Test Changes with Union', TestChangesWithUnion);
  it('Test Changes with Join', TestChangesWithJoin);
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

async function TestChangesFeedIterator() {
  const changes = db.table(table).changes();

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property('map');
  expect(changes).to.have.property('filter');
  expect(changes).to.have.property('pluck');
  expect(changes).to.have.property('without');
  expect(changes).to.have.property('withFields');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const iterator = changes[Symbol.asyncIterator]();
  expect(iterator).to.be.an('object');
  expect(iterator).to.have.property('next');
  expect(iterator).to.have.property('return');

  const result = await changes.run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  result.forEach((change) => {
    expect(change).to.have.property('new_val');
    expect(change).to.not.have.property('old_val');
    expect(change.new_val).to.have.property('name');
    expect(change.new_val).to.have.property('email');
    expect(change.new_val).to.have.property('age');
  });
}

async function TestChangesWithOptions() {
  const changes = db.table(table).changes({
    squash: true,
    changefeedQueueSize: 100,
    includeInitial: true,
  });

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);
}

async function TestChangesOnSingleDocument() {
  const changes = db.table(table).get(insertedKeys[0]).changes();

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property('map');
  expect(changes).to.have.property('filter');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(1);
  expect(result[0]).to.have.property('new_val');
  expect(result[0].new_val).to.have.property('id', insertedKeys[0]);
}

async function TestChangesWithFilter() {
  const changes = db
    .table(table)
    .changes()
    .filter((change) => change('new_val')('status').eq('active'));

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');

  const activeUsers = testData.filter((user) => user.status === 'active');
  expect(result).to.have.lengthOf(activeUsers.length);

  result.forEach((change) => {
    expect(change.new_val?.status).to.equal('active');
  });
}

async function TestChangesWithMap() {
  const changes = db
    .table(table)
    .changes()
    .map((change) => ({
      type: change('new_val').default(null) ? 'insert' : 'delete',
      document: change('new_val').default(change('old_val')),
    }));

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  result.forEach((mappedChange) => {
    expect(mappedChange).to.have.property('type', 'insert');
    expect(mappedChange).to.have.property('document');
    expect(mappedChange.document).to.have.property('name');
    expect(mappedChange.document).to.have.property('email');
  });
}

async function TestChangesWithPluck() {
  const changes = db.table(table).changes().pluck('new_val', 'old_val');

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  result.forEach((change) => {
    expect(change).to.have.property('new_val');
    expect(change).to.not.have.property('old_val');
    expect(change.new_val).to.have.property('name');
    expect(change.new_val).to.have.property('email');
    expect(change.new_val).to.not.have.property('age');
  });
}

async function TestChangesWithWithout() {
  const changes = db.table(table).changes().without('new_val', 'old_val');

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  result.forEach((change) => {
    expect(change).to.not.have.property('new_val');
    expect(change).to.not.have.property('old_val');
  });
}

async function TestChangesWithWithFields() {
  const changes = db.table(table).changes().withFields('new_val', 'old_val');

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  result.forEach((change) => {
    expect(change).to.have.property('new_val');
    expect(change).to.not.have.property('old_val');
  });
}

async function TestChangesWithOrderBy() {
  const changes = db.table(table).orderBy('name', 'asc').changes();

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(testData.length);

  const sortedNames = testData.map((user) => user.name).sort();
  result.forEach((change, index) => {
    expect(change.new_val?.name).to.equal(sortedNames[index]);
  });
}

async function TestChangesWithSlice() {
  const changes = db.table(table).slice(0, 2).changes();

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);
}

async function TestChangesWithNth() {
  const changes = db.table(table).nth(0).changes();

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(1);
}

async function TestChangesWithDistinct() {
  const changes = db.table(table).distinct('status').changes();

  expect(changes).to.be.an('object');
  expect(changes).to.have.property('run');
  expect(changes).to.have.property(Symbol.asyncIterator);

  const result = await changes.run();
  expect(result).to.be.an('array');

  const uniqueStatuses = [...new Set(testData.map((user) => user.status))];
  expect(result).to.have.lengthOf(uniqueStatuses.length);
}

async function TestChangesWithCount() {
  const count = await db.table(table).count().run();
  expect(count).to.be.a('number');
  expect(count).to.equal(testData.length);
}

async function TestChangesWithSum() {
  const sum = await db.table(table).sum('age').run();
  expect(sum).to.be.a('number');
  expect(sum).to.equal(testData.reduce((acc, item) => acc + item.age, 0));
}

async function TestChangesWithAvg() {
  const avg = await db.table(table).avg('age').run();
  expect(avg).to.be.a('number');
  const expectedAvg = testData.reduce((acc, item) => acc + item.age, 0) / testData.length;
  expect(avg).to.be.closeTo(expectedAvg, 0.1);
}

async function TestChangesWithMin() {
  const min = await db.table(table).min('age').run();
  expect(min).to.be.a('number');
  expect(min).to.equal(Math.min(...testData.map((item) => item.age)));
}

async function TestChangesWithMax() {
  const max = await db.table(table).max('age').run();
  expect(max).to.be.a('number');
  expect(max).to.equal(Math.max(...testData.map((item) => item.age)));
}

async function TestChangesWithGroup() {
  const result = await db
    .table(table)
    .group('status', (stream, group) => ({
      status: group,
      count: stream.count(),
      averageAge: stream.map((row) => row('age')).avg(),
    }))
    .run();

  expect(result).to.be.an('array');
  expect(result.length).to.be.greaterThan(0);

  result.forEach((item) => {
    expect(item).to.have.property('status');
    expect(item).to.have.property('count');
    expect(item).to.have.property('averageAge');
    expect(item.count).to.be.a('number');
    expect(item.averageAge).to.be.a('number');
  });
}

async function TestChangesWithUnion() {
  const result = await db
    .table(table)
    .union(db.table(table).filter((doc) => doc('status').eq('active')))
    .run();

  expect(result).to.be.an('array');
  expect(result.length).to.be.greaterThanOrEqual(testData.length);
}

async function TestChangesWithJoin() {
  const result = await db
    .table(table)
    .join(
      db.table(table),
      JoinType.Inner,
      (left, right) => left.merge({ joined: right }),
      (left, right) => left('status').eq(right('status')),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result.length).to.be.greaterThan(0);

  result.forEach((doc) => {
    expect(doc).to.have.property('joined');
    expect(doc.status).to.equal(doc.joined.status);
  });
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await db.table(table).get(key).delete().run();
  }
}
