import { Database, JoinType } from '@ajs/database/beta';
import { expect } from 'chai';

const db = Database<{
  [ordersTable]: Order;
  [usersTable]: User;
  [productsTable]: Product;
}>('test-joins');

const ordersTable = 'orders';
const usersTable = 'users';
const productsTable = 'products';

type User = {
  email: string;
  name: string;
  age: number;
  isActive: boolean;
};

type Product = {
  sku: string;
  name: string;
  price: number;
};

type Order = {
  orderId: string;
  customerEmail: string;
  productSku: string;
  quantity: number;
  totalPrice: number;
  orderDate: Date;
};

let usersData: User[] = [
  {
    email: 'alice@example.com',
    name: 'Alice',
    age: 28,
    isActive: true,
  },
  {
    email: 'bob@example.com',
    name: 'Bob',
    age: 35,
    isActive: false,
  },
  {
    email: 'camille@example.com',
    name: 'Camille',
    age: 22,
    isActive: true,
  },
];

let productsData: Product[] = [
  {
    sku: 'LAPTOP-001',
    name: 'Asell f00',
    price: 1200,
  },
  {
    sku: 'BOOK-001',
    name: 'Clean code',
    price: 25,
  },
  {
    sku: 'PHONE-001',
    name: 'OneSung X',
    price: 800,
  },
];

let ordersData: Order[] = [
  {
    orderId: 'ORD-001',
    customerEmail: 'alice@example.com',
    productSku: 'LAPTOP-001',
    quantity: 1,
    totalPrice: 1200,
    orderDate: new Date('2024-01-15'),
  },
  {
    orderId: 'ORD-002',
    customerEmail: 'bob@example.com',
    productSku: 'BOOK-001',
    quantity: 2,
    totalPrice: 50,
    orderDate: new Date('2024-02-20'),
  },
  {
    orderId: 'ORD-003',
    customerEmail: 'alice@example.com',
    productSku: 'PHONE-001',
    quantity: 1,
    totalPrice: 800,
    orderDate: new Date('2024-03-10'),
  },
  {
    orderId: 'ORD-004',
    customerEmail: 'camille@example.com',
    productSku: 'BOOK-001',
    quantity: 1,
    totalPrice: 25,
    orderDate: new Date('2024-04-05'),
  },
];

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
    .table<Order>(ordersTable)
    .join(
      db.table<User>(usersTable),
      JoinType.Inner,
      (left, right) => left.merge({ customer: right }),
      (left, right) => left('customerEmail').eq(right('email')),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(4);

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
  expect(aliceOrders).to.have.lengthOf(2);
  aliceOrders.forEach((order) => {
    expect(order.customer.name).to.equal('Alice');
    expect(order.customer.age).to.equal(28);
    expect(order.customer.isActive).to.equal(true);
  });
}

async function InnerJoinOrdersWithProducts() {
  const result = await db
    .table<Order>(ordersTable)
    .join(
      db.table<Product>(productsTable),
      JoinType.Inner,
      (left, right) => left.merge({ product: right }),
      (left, right) => left('productSku').eq(right('sku')),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(4);

  result.forEach((doc) => {
    expect(doc).to.have.property('productSku');
    expect(doc).to.have.property('product');
    expect(doc.product).to.have.property('sku');
    expect(doc.product).to.have.property('name');
    expect(doc.product).to.have.property('price');
    expect(doc.productSku).to.equal(doc.product.sku);
  });

  const laptopOrders = result.filter((doc) => doc.productSku === 'LAPTOP-001');
  expect(laptopOrders).to.have.lengthOf(1);
  expect(laptopOrders[0].product.name).to.equal('Asell f00');
  expect(laptopOrders[0].product.price).to.equal(1200);
}

async function LeftJoinOrdersWithUsers() {
  const result = await db
    .table<Order>(ordersTable)
    .join(
      db.table<User>(usersTable),
      JoinType.Left,
      (left, right) => left.merge({ customer: right }),
      (left, right) => left('customerEmail').eq(right('email')),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(4);

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
    .table<Order>(ordersTable)
    .join(
      db.table<User>(usersTable),
      JoinType.Inner,
      (left, right) => left.merge({ customer: right }),
      (left, right) => left('customerEmail').eq(right('email')),
    )
    .join(
      db.table<Product>(productsTable),
      JoinType.Inner,
      (left, right) => left.merge({ product: right }),
      (left, right) => left('productSku').eq(right('sku')),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(4);

  result.forEach((doc) => {
    expect(doc).to.have.property('customer');
    expect(doc).to.have.property('product');
    expect(doc.customerEmail).to.equal(doc.customer.email);
    expect(doc.productSku).to.equal(doc.product.sku);
  });

  const aliceLaptopOrder = result.find(
    (doc) => doc.customerEmail === 'alice@example.com' && doc.productSku === 'LAPTOP-001',
  );
  expect(aliceLaptopOrder).to.not.equal(undefined);
  expect(aliceLaptopOrder!.customer.name).to.equal('Alice');
  expect(aliceLaptopOrder!.product.name).to.equal('Asell f00');
}

async function JoinWithFilter() {
  const result = await db
    .table<Order>(ordersTable)
    .join(
      db.table<User>(usersTable),
      JoinType.Inner,
      (left, right) => left.merge({ customer: right }),
      (left, right) => left('customerEmail').eq(right('email')),
    )
    .filter((order) => order('customer')('isActive').eq(true))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((doc) => {
    expect(doc.customer.isActive).to.equal(true);
  });

  const inactiveUsers = result.filter((doc) => doc.customerEmail === 'bob@example.com');
  expect(inactiveUsers).to.have.lengthOf(0);
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
