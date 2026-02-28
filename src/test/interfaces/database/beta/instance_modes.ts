import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { Vehicle } from '../../../datasets/vehicles';

const tableName = 'vehicles';

function vehicle(car: string, price: number): Vehicle {
  return { car, manufactured: new Date('2020-01-01'), price, isElectric: false, kilometers: 1000 };
}

const dbLevelSchema = new Schema<{ [tableName]: Vehicle }>('test-db-level', { [tableName]: Vehicle });
const globalSchema = new Schema<{ [tableName]: Vehicle }>('test-global', { [tableName]: Vehicle });
const rowLevelSchema = new Schema<{ [tableName]: Vehicle }>('test-row-level', { [tableName]: Vehicle }, { rowLevel: true });

describe('Database-Level Instances', () => {
  it('createInstance / destroyInstance', CreateAndDestroyInstance);
  it('Named instance CRUD', NamedInstanceCrud);
  it('Instance isolation', InstanceIsolation);

  after(async () => {
    await dbLevelSchema.instance('a').table(tableName).delete().run();
    await dbLevelSchema.instance('b').table(tableName).delete().run();
    await dbLevelSchema.destroyInstance('a').run();
    await dbLevelSchema.destroyInstance('b').run();
    await dbLevelSchema.destroyInstance('named').run();
  });
});

async function CreateAndDestroyInstance() {
  const id = await dbLevelSchema.createInstance('inst1').run();
  expect(id).to.equal('inst1');
  await dbLevelSchema.destroyInstance('inst1').run();
}

async function NamedInstanceCrud() {
  await dbLevelSchema.createInstance('named').run();
  const table = dbLevelSchema.instance('named').table(tableName);

  const keys = await table.insert([vehicle('Peugeot', 3000)]).run();
  expect(keys).to.have.lengthOf(1);

  const doc = await table.get(keys[0]).run();
  expect(doc).to.have.property('car', 'Peugeot');

  const updated = await table.get(keys[0]).update({ price: 9999 }).run();
  expect(updated).to.equal(1);

  const deleted = await table.get(keys[0]).delete().run();
  expect(deleted).to.equal(1);

  const gone = await table.get(keys[0]).run();
  expect(gone).to.equal(undefined);
}

async function InstanceIsolation() {
  await dbLevelSchema.createInstance('a').run();
  await dbLevelSchema.createInstance('b').run();
  const tableA = dbLevelSchema.instance('a').table(tableName);
  const tableB = dbLevelSchema.instance('b').table(tableName);

  await tableA.insert([vehicle('Peugeot', 3000)]).run();

  const allB = await tableB.run();
  expect(allB).to.be.an('array').with.lengthOf(0);
}

describe('Global Instance', () => {
  it('createInstance(undefined) returns undefined', GlobalCreateInstance);
  it('CRUD on global instance', GlobalInstanceCrud);
  it('Global instance isolation from named', GlobalNamedIsolation);

  after(async () => {
    await globalSchema.instance().table(tableName).delete().run();
    await globalSchema.instance('named').table(tableName).delete().run();
    await globalSchema.destroyInstance(undefined).run();
    await globalSchema.destroyInstance('named').run();
  });
});

async function GlobalCreateInstance() {
  const id = await globalSchema.createInstance(undefined).run();
  expect(id).to.equal(undefined);
}

async function GlobalInstanceCrud() {
  const table = globalSchema.instance().table(tableName);

  const keys = await table.insert([vehicle('Renault', 5000)]).run();
  expect(keys).to.have.lengthOf(1);

  const doc = await table.get(keys[0]).run();
  expect(doc).to.have.property('car', 'Renault');

  const updated = await table.get(keys[0]).update({ price: 7777 }).run();
  expect(updated).to.equal(1);

  const deleted = await table.get(keys[0]).delete().run();
  expect(deleted).to.equal(1);

  const gone = await table.get(keys[0]).run();
  expect(gone).to.equal(undefined);
}

async function GlobalNamedIsolation() {
  await globalSchema.createInstance('named').run();
  const globalTable = globalSchema.instance().table(tableName);
  const namedTable = globalSchema.instance('named').table(tableName);

  await globalTable.insert([vehicle('Peugeot', 3000)]).run();

  const namedDocs = await namedTable.run();
  expect(namedDocs).to.be.an('array').with.lengthOf(0);

  await namedTable.insert([vehicle('Citroen', 4000)]).run();

  const globalDocs = await globalTable.run();
  expect(globalDocs).to.be.an('array').with.lengthOf(1);
}

describe('Row-Level Tenancy', () => {
  it('instance(undefined) throws', RowLevelUndefinedThrows);
  it('createInstance is no-op', RowLevelCreateInstance);
  it('destroyInstance is no-op', RowLevelDestroyInstance);
  it('Insert stamps tenant_id', RowLevelInsertStamps);
  it('Tenant isolation', RowLevelTenantIsolation);
  it('Cross-tenant count', RowLevelCrossCount);
  it('Update scoped to tenant', RowLevelScopedUpdate);
  it('Delete scoped to tenant', RowLevelScopedDelete);

  after(async () => {
    await rowLevelSchema.instance('t1').table(tableName).delete().run();
    await rowLevelSchema.instance('t2').table(tableName).delete().run();
    await rowLevelSchema.instance('cleanup').table(tableName).delete().run();
  });
});

async function RowLevelUndefinedThrows() {
  let threw = false;
  try {
    await rowLevelSchema.instance().table(tableName).count().run();
  } catch {
    threw = true;
  }
  expect(threw).to.equal(true);
}

async function RowLevelCreateInstance() {
  const id = await rowLevelSchema.createInstance('t1').run();
  expect(id).to.equal('t1');
}

async function RowLevelDestroyInstance() {
  await rowLevelSchema.destroyInstance('t1').run();
}

async function RowLevelInsertStamps() {
  const table = rowLevelSchema.instance('cleanup').table(tableName);
  const keys = await table.insert([vehicle('Peugeot', 3000)]).run();
  expect(keys).to.have.lengthOf(1);

  const doc = await table.get(keys[0]).run();
  expect(doc).to.have.property('tenant_id', 'cleanup');
}

async function RowLevelTenantIsolation() {
  const t1Table = rowLevelSchema.instance('t1').table(tableName);
  const t2Table = rowLevelSchema.instance('t2').table(tableName);

  await t1Table.insert([vehicle('Peugeot', 3000)]).run();
  await t2Table.insert([vehicle('Renault', 5000)]).run();

  const t1Docs = await t1Table.run();
  const t2Docs = await t2Table.run();
  expect(t1Docs).to.have.lengthOf(1);
  expect(t2Docs).to.have.lengthOf(1);
  expect(t1Docs[0].car).to.equal('Peugeot');
  expect(t2Docs[0].car).to.equal('Renault');
}

async function RowLevelCrossCount() {
  const t1Table = rowLevelSchema.instance('t1').table(tableName);
  const t2Table = rowLevelSchema.instance('t2').table(tableName);

  await t1Table.insert([vehicle('Citroen', 4000), vehicle('Tesla', 50000)]).run();

  const t1Docs = await t1Table.run();
  const t2Docs = await t2Table.run();
  expect(t1Docs).to.have.lengthOf(3);
  expect(t2Docs).to.have.lengthOf(1);
}

async function RowLevelScopedUpdate() {
  const t1Table = rowLevelSchema.instance('t1').table(tableName);
  const t2Table = rowLevelSchema.instance('t2').table(tableName);

  const t2Before = await t2Table.run();
  const t2PriceBefore = t2Before[0].price;

  await t1Table.filter((doc: any) => doc.key('car').eq('Peugeot')).update({ price: 1 }).run();

  const t2After = await t2Table.run();
  expect(t2After[0].price).to.equal(t2PriceBefore);

  const t1Updated = await t1Table.filter((doc: any) => doc.key('car').eq('Peugeot')).run();
  for (const doc of t1Updated) {
    expect(doc.price).to.equal(1);
  }
}

async function RowLevelScopedDelete() {
  const t1Table = rowLevelSchema.instance('t1').table(tableName);
  const t2Table = rowLevelSchema.instance('t2').table(tableName);

  const t2CountBefore = await t2Table.run();

  await t1Table.delete().run();

  const t1Remaining = await t1Table.run();
  expect(t1Remaining).to.have.lengthOf(0);

  const t2CountAfter = await t2Table.run();
  expect(t2CountAfter).to.have.lengthOf(t2CountBefore.length);
}
