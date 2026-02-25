import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../../../datasets/users';
import { getUniqueOrders, Order } from '../../../datasets/orders';

const ordersTableName = 'orders';
const usersTableName = 'users';

const schema = new Schema<{ [ordersTableName]: Order; [usersTableName]: User }>('test-lookup-operations', {
  [ordersTableName]: Order,
  [usersTableName]: User,
});

const ordersTable = schema.default.table(ordersTableName);
const usersTable = schema.default.table(usersTableName);

const usersData = getUniqueUsers();
const ordersData = getUniqueOrders();

let insertedKeys: {
  users: string[];
  orders: string[];
} = {
  users: [],
  orders: [],
};

describe('Lookup Operations', () => {
  before(async () => {
    await ordersTable.delete().run();
    await usersTable.delete().run();
  });

  after(async () => {
    await ordersTable.delete().run();
    await usersTable.delete().run();
  });

  it('Insert Test Data', InsertTestData);
  it('Lookup Basic', LookupBasic);
  it('Lookup With Filter', LookupWithFilter);
  it('Cleanup', CleanupTest);
});

async function InsertTestData() {
  const usersResponse = await usersTable.insert(usersData).run();
  const ordersResponse = await ordersTable.insert(ordersData).run();

  expect(usersResponse).to.be.an('array');
  expect(ordersResponse).to.be.an('array');

  insertedKeys.users = usersResponse;
  insertedKeys.orders = ordersResponse;
}

async function LookupBasic() {
  const result = await ordersTable.lookup(usersTable, 'customerName', 'name').run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((doc: any) => {
    expect(doc).to.have.property('customerName');
    const customerName = doc.customerName;
    expect(customerName).to.be.an('object');
    expect(customerName).to.have.property('name');
    expect(customerName).to.have.property('age');
    expect(customerName).to.have.property('email');
    expect(customerName).to.have.property('isActive');
  });

  const antoineOrder = result.find((doc: any) => doc.orderId === 'ORD-001');
  expect(antoineOrder).to.not.equal(undefined);
  expect(antoineOrder!.customerName).to.have.property('name', 'Antoine');
  expect(antoineOrder!.customerName).to.have.property('age', 25);
}

async function LookupWithFilter() {
  const result = await ordersTable
    .lookup(usersTable, 'customerName', 'name')
    .filter((doc) => doc.key('isPaid').eq(true))
    .run();

  expect(result).to.be.an('array');
  const expectedCount = ordersData.filter((o) => o.isPaid).length;
  expect(result).to.have.lengthOf(expectedCount);

  result.forEach((doc: any) => {
    expect(doc.isPaid).to.equal(true);
    expect(doc.customerName).to.be.an('object');
    expect(doc.customerName).to.have.property('name');
  });
}

async function CleanupTest() {
  for (const key of insertedKeys.orders) {
    await ordersTable.get(key).delete().run();
  }
  for (const key of insertedKeys.users) {
    await usersTable.get(key).delete().run();
  }
}
