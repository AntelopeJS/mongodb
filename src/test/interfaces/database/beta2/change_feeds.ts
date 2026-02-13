import { Query, Schema } from '@ajs/database/beta2';
import { expect } from 'chai';
import { vehicles, Vehicle } from '../../../datasets/vehicles';

const tableName = 'test-table';
const schema = new Schema<{ [tableName]: Vehicle }>('test-change-feeds', { [tableName]: Vehicle });
const table = schema.default.table(tableName);

let insertedKeys: string[] = [];

describe('Change Streams', () => {
  it('Insert Test Data', InsertTest);
  it('Insert Event', InsertEventTest);
  it('Cleanup', CleanupTest);
});

async function InsertTest() {
  const response = await table.insert(vehicles).run();
  expect(response).to.be.an('array');

  expect(response).to.have.lengthOf(vehicles.length);
  response.forEach((val) => {
    expect(val).to.be.a('string');
  });
  insertedKeys = response;
}

async function ReadChanges<T>(stream: Query<T[]>, actor: (results: T[]) => Promise<void>) {
  const results: T[] = [];
  await Promise.race([
    (async () => {
      for await (const doc of stream) {
        results.push(doc);
      }
    })(),
    (async () => {
      // Make sure watcher is ready
      await new Promise((resolve) => setTimeout(resolve, 10));

      await actor(results);

      // Make sure watcher has received the last changes
      await new Promise((resolve) => setTimeout(resolve, 10));
    })(),
  ]);

  return results;
}

async function InsertEventTest() {
  const newDocument: Vehicle = {
    car: 'Renault',
    manufactured: new Date('2000-01-12'),
    price: 12000,
    isElectric: false,
    kilometers: 98812,
  };

  const results = await ReadChanges(table.changes(), async () => {
    await table.insert(newDocument);
  });

  expect(results).to.be.an('array').of.length(1);
  expect(results[0]).to.have.property('changeType', 'added');
  expect(results[0]).to.have.property('newValue').that.deep.equal(newDocument);
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await table.get(key).delete().run();
  }
}
