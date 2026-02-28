import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../../../datasets/users';
import { getUniqueProducts, Product } from '../../../datasets/products';
import { getUniqueOrders, Order } from '../../../datasets/orders';

const ordersTableName = 'orders';
const usersTableName = 'users';
const productsTableName = 'products';

const schema = new Schema<{ [ordersTableName]: Order; [usersTableName]: User; [productsTableName]: Product }>(
  'test-joins',
  { [ordersTableName]: Order, [usersTableName]: User, [productsTableName]: Product },
);

const ordersTable = schema.default.table(ordersTableName);
const usersTable = schema.default.table(usersTableName);
const productsTable = schema.default.table(productsTableName);

const usersData = getUniqueUsers();
const productsData = getUniqueProducts();
const ordersData = getUniqueOrders();

let insertedKeys: {
  users: string[];
  products: string[];
  orders: string[];
} = {
  users: [],
  products: [],
  orders: [],
};

describe('Join Operations', () => {
  it('Insert Test Data', InsertTestData);
  it('Inner Join Orders with Users', InnerJoinOrdersWithUsers);
  it('Inner Join Orders with Products', InnerJoinOrdersWithProducts);
  it('Left Join Orders with Users', LeftJoinOrdersWithUsers);
  it('Multiple Joins', MultipleJoins);
  it('Join with Filter', JoinWithFilter);
  it('Cleanup', CleanupTest);
});

async function InsertTestData() {
  const usersResponse = await usersTable.insert(usersData).run();
  const productsResponse = await productsTable.insert(productsData).run();
  const ordersResponse = await ordersTable.insert(ordersData).run();

  expect(usersResponse).to.be.an('array');
  expect(productsResponse).to.be.an('array');
  expect(ordersResponse).to.be.an('array');

  insertedKeys.users = usersResponse;
  insertedKeys.products = productsResponse;
  insertedKeys.orders = ordersResponse;
}

async function InnerJoinOrdersWithUsers() {
  const result = await ordersTable
    .join(
      usersTable,
      (left, right) => left.key('customerEmail').eq(right.key('email')),
      (left, right) => left.merge({ customer: right }),
      true,
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(usersData.length);

  result.forEach((doc) => {
    expect(doc).to.have.property('customerEmail');
    expect(doc).to.have.property('customer');
    expect(doc.customer).to.have.property('email');
    expect(doc.customer).to.have.property('name');
    expect(doc.customer).to.have.property('age');
    expect(doc.customer).to.have.property('isActive');
    expect(doc.customerEmail).to.equal(doc.customer!.email);
  });

  const aliceOrders = result.filter((doc) => doc.customerEmail === 'alice@example.com');
  const expectedAliceOrdersCount = ordersData.filter((order) => order.customerEmail === 'alice@example.com').length;
  expect(aliceOrders).to.have.lengthOf(expectedAliceOrdersCount);
  aliceOrders.forEach((order) => {
    expect(order.customer!.name).to.equal('Alice');
    expect(order.customer!.age).to.equal(30);
    expect(order.customer!.isActive).to.equal(false);
  });
}

async function InnerJoinOrdersWithProducts() {
  const result = await ordersTable
    .join(
      productsTable,
      (left, right) => left.key('productSku').eq(right.key('sku')),
      (left, right) => left.merge({ product: right }),
      true,
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(productsData.length);

  result.forEach((doc) => {
    expect(doc).to.have.property('productSku');
    expect(doc).to.have.property('product');
    expect(doc.product).to.have.property('sku');
    expect(doc.product).to.have.property('name');
    expect(doc.product).to.have.property('price');
    expect(doc.productSku).to.equal(doc.product!.sku);
  });

  const laptopOrders = result.filter((doc) => doc.productSku === 'LAPTOP-001');
  const expectedLaptopOrdersCount = ordersData.filter((order) => order.productSku === 'LAPTOP-001').length;
  expect(laptopOrders).to.have.lengthOf(expectedLaptopOrdersCount);
  expect(laptopOrders[0].product!.name).to.equal('Asell f00');
  expect(laptopOrders[0].product!.price).to.equal(1200);
}

async function LeftJoinOrdersWithUsers() {
  const result = await ordersTable
    .join(
      usersTable,
      (left, right) => left.key('customerEmail').eq(right.key('email')),
      (left, right) => left.merge({ customer: right }),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((doc) => {
    expect(doc).to.have.property('customerEmail');
    expect(doc).to.have.property('customer');
    if (doc.customer) {
      expect(doc.customerEmail).to.equal(doc.customer.email);
    }
  });
}

async function MultipleJoins() {
  const result = await ordersTable
    .join(
      usersTable,
      (left, right) => left.key('customerEmail').eq(right.key('email')),
      (left, right) => left.merge({ customer: right }),
      true,
    )
    .join(
      productsTable,
      (left, right) => left.key('productSku').eq(right.key('sku')),
      (left, right) => left.merge({ product: right }),
      true,
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((doc) => {
    expect(doc).to.have.property('customer');
    expect(doc).to.have.property('product');
    expect(doc.customerEmail).to.equal(doc.customer!.email);
    expect(doc.productSku).to.equal(doc.product!.sku);
  });

  const aliceBookOrder = result.find(
    (doc) => doc.customerEmail === 'alice@example.com' && doc.productSku === 'BOOK-001',
  );
  expect(aliceBookOrder).to.not.equal(undefined);
  expect(aliceBookOrder!.customer!.name).to.equal('Alice');
  expect(aliceBookOrder!.product!.name).to.equal('Programming Fundamentals');
}

async function JoinWithFilter() {
  const result = await ordersTable
    .join(
      usersTable,
      (left, right) => left.key('customerEmail').eq(right.key('email')),
      (left, right) => left.merge({ customer: right }),
      true,
    )
    .filter((order) => order.key('customer').key('isActive').eq(true))
    .run();

  expect(result).to.be.an('array');
  const expectedActiveOrdersCount = ordersData.filter((order) => {
    const user = usersData.find((user) => user.email === order.customerEmail);
    return user && user.isActive;
  }).length;
  expect(result).to.have.lengthOf(expectedActiveOrdersCount);

  result.forEach((doc) => {
    expect(doc.customer?.isActive).to.equal(true);
  });

  const activeUsers = result.filter((doc) => doc.customerEmail === 'antoine@example.com');
  const expectedActiveUsersCount = ordersData.filter(
    (order) =>
      order.customerEmail === 'antoine@example.com' &&
      usersData.find((user) => user.email === order.customerEmail)?.isActive,
  ).length;
  expect(activeUsers).to.have.lengthOf(expectedActiveUsersCount);
}

async function CleanupTest() {
  for (const key of insertedKeys.orders) {
    await ordersTable.get(key).delete().run();
  }
  for (const key of insertedKeys.products) {
    await productsTable.get(key).delete().run();
  }
  for (const key of insertedKeys.users) {
    await usersTable.get(key).delete().run();
  }
}
