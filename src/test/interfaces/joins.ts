import { Database, JoinType } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../datasets/users';
import { getUniqueProducts, Product } from '../datasets/products';
import { getUniqueOrders, Order } from '../datasets/orders';

const db = Database<{
  [ordersTable]: Order;
  [usersTable]: User;
  [productsTable]: Product;
}>('test-joins');

const ordersTable = 'orders';
const usersTable = 'users';
const productsTable = 'products';

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
  const usersResponse = await db.table(usersTable).insert(usersData).run();
  const productsResponse = await db.table(productsTable).insert(productsData).run();
  const ordersResponse = await db.table(ordersTable).insert(ordersData).run();

  expect(usersResponse).to.have.property('inserted', usersData.length);
  expect(productsResponse).to.have.property('inserted', productsData.length);
  expect(ordersResponse).to.have.property('inserted', ordersData.length);

  insertedKeys.users = Object.values(usersResponse.generated_keys ?? {});
  insertedKeys.products = Object.values(productsResponse.generated_keys ?? {});
  insertedKeys.orders = Object.values(ordersResponse.generated_keys ?? {});
}

async function InnerJoinOrdersWithUsers() {
  const result = await db
    .table(ordersTable)
    .join(
      db.table(usersTable),
      JoinType.Inner,
      (left, right) => left.merge({ customer: right }),
      (left, right) => left('customerEmail').eq(right('email')),
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
    expect(doc.customerEmail).to.equal(doc.customer.email);
  });

  const aliceOrders = result.filter((doc) => doc.customerEmail === 'alice@example.com');
  const expectedAliceOrdersCount = ordersData.filter((order) => order.customerEmail === 'alice@example.com').length;
  expect(aliceOrders).to.have.lengthOf(expectedAliceOrdersCount);
  aliceOrders.forEach((order) => {
    expect(order.customer.name).to.equal('Alice');
    expect(order.customer.age).to.equal(30);
    expect(order.customer.isActive).to.equal(false);
  });
}

async function InnerJoinOrdersWithProducts() {
  const result = await db
    .table(ordersTable)
    .join(
      db.table(productsTable),
      JoinType.Inner,
      (left, right) => left.merge({ product: right }),
      (left, right) => left('productSku').eq(right('sku')),
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
    expect(doc.productSku).to.equal(doc.product.sku);
  });

  const laptopOrders = result.filter((doc) => doc.productSku === 'LAPTOP-001');
  const expectedLaptopOrdersCount = ordersData.filter((order) => order.productSku === 'LAPTOP-001').length;
  expect(laptopOrders).to.have.lengthOf(expectedLaptopOrdersCount);
  expect(laptopOrders[0].product.name).to.equal('Asell f00');
  expect(laptopOrders[0].product.price).to.equal(1200);
}

async function LeftJoinOrdersWithUsers() {
  const result = await db
    .table(ordersTable)
    .join(
      db.table(usersTable),
      JoinType.Left,
      (left, right) => left.merge({ customer: right }),
      (left, right) => left('customerEmail').eq(right('email')),
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
  const result = await db
    .table(ordersTable)
    .join(
      db.table(usersTable),
      JoinType.Inner,
      (left, right) => left.merge({ customer: right }),
      (left, right) => left('customerEmail').eq(right('email')),
    )
    .join(
      db.table(productsTable),
      JoinType.Inner,
      (left, right) => left.merge({ product: right }),
      (left, right) => left('productSku').eq(right('sku')),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((doc) => {
    expect(doc).to.have.property('customer');
    expect(doc).to.have.property('product');
    expect(doc.customerEmail).to.equal(doc.customer.email);
    expect(doc.productSku).to.equal(doc.product.sku);
  });

  const aliceBookOrder = result.find(
    (doc) => doc.customerEmail === 'alice@example.com' && doc.productSku === 'BOOK-001',
  );
  expect(aliceBookOrder).to.not.equal(undefined);
  expect(aliceBookOrder!.customer.name).to.equal('Alice');
  expect(aliceBookOrder!.product.name).to.equal('Programming Fundamentals');
}

async function JoinWithFilter() {
  const result = await db
    .table(ordersTable)
    .join(
      db.table(usersTable),
      JoinType.Inner,
      (left, right) => left.merge({ customer: right }),
      (left, right) => left('customerEmail').eq(right('email')),
    )
    .filter((order) => order('customer')('isActive').eq(true))
    .run();

  expect(result).to.be.an('array');
  const expectedActiveOrdersCount = ordersData.filter((order) => {
    const user = usersData.find((user) => user.email === order.customerEmail);
    return user && user.isActive;
  }).length;
  expect(result).to.have.lengthOf(expectedActiveOrdersCount);

  result.forEach((doc) => {
    expect(doc.customer.isActive).to.equal(true);
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
    await db.table(ordersTable).get(key).delete().run();
  }
  for (const key of insertedKeys.products) {
    await db.table(productsTable).get(key).delete().run();
  }
  for (const key of insertedKeys.users) {
    await db.table(usersTable).get(key).delete().run();
  }
}
