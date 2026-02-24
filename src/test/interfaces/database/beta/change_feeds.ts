import { Query, Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { vehicles, Vehicle } from '../../../datasets/vehicles';

const tableName = 'test-table';
const schema = new Schema<{ [tableName]: Vehicle }>('test-change-feeds', { [tableName]: Vehicle });
const table = schema.default.table(tableName);

describe('Change Streams', () => {
  it('Insert Test Data', InsertTest);
  it('Insert Event', InsertEventTest);
  it('Update Event', UpdateEventTest);
  it('Delete Event', DeleteEventTest);
  after(async () => {
    await table.delete().run();
  });
});

async function InsertTest() {
  const response = await table.insert(vehicles).run();
  expect(response).to.be.an('array');

  expect(response).to.have.lengthOf(vehicles.length);
  response.forEach((val) => {
    expect(val).to.be.a('string');
  });
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
      await new Promise((resolve) => setTimeout(resolve, 200));

      await actor(results);

      // Make sure watcher has received the last changes
      await new Promise((resolve) => setTimeout(resolve, 200));
    })(),
  ]);

  return results;
}

let inserted: string[];
async function InsertEventTest() {
  const newDocument: Vehicle = {
    car: 'Renault',
    manufactured: new Date('2000-01-12'),
    price: 12000,
    isElectric: false,
    kilometers: 98812,
  };

  const results = await ReadChanges(table.changes(), async () => {
    inserted = await table.insert(newDocument);
  });

  expect(results).to.be.an('array').of.length(1);
  expect(results[0]).to.have.property('changeType', 'added');
  expect(results[0]).to.have.property('newValue').that.deep.include(newDocument);
}

async function UpdateEventTest() {
  const results = await ReadChanges(table.filter((doc) => doc.key('car').eq('Renault')).changes(), async () => {
    await table.filter((doc) => doc.key('car').eq('Renault')).update({ price: 0 });
  });

  expect(results).to.be.an('array').of.length(2);
  for (const entry of results) {
    expect(entry).to.have.property('changeType', 'modified');
    expect(entry).to.have.property('newValue').that.has.property('price', 0);
  }
}

async function DeleteEventTest() {
  const results = await ReadChanges(table.changes(), async () => {
    await table.get(inserted[0]).delete();
  });

  expect(results).to.be.an('array').of.length(1);
  expect(results[0]).to.have.property('changeType', 'removed');
  expect(results[0]).to.have.property('oldValue').that.has.property('_id', inserted[0]);
}
