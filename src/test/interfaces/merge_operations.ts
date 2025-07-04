/* eslint-disable @typescript-eslint/no-unused-expressions */
import { Database } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../datasets/users';
import { getUniqueProducts, Product } from '../datasets/products';
import { getUniqueOrders, Order } from '../datasets/orders';

const db = Database<{
  [ordersTable]: Order;
  [usersTable]: User;
  [productsTable]: Product;
}>('test-merge-operations');

const ordersTable = 'orders';
const usersTable = 'users';
const productsTable = 'products';

// Utiliser le dataset unifié
const usersData = getUniqueUsers();
const productsData = getUniqueProducts();
const ordersData = getUniqueOrders().filter((order) => order.customer && order.items);

let insertedKeys: {
  users: string[];
  products: string[];
  orders: string[];
} = {
  users: [],
  products: [],
  orders: [],
};

describe('Merge Operations', () => {
  it('Insert Test Data', InsertTestData);
  it('Merge Customer with User Data', MergeCustomerWithUserData);
  it('Merge Order Items with Product Data', MergeOrderItemsWithProductData);
  it('Merge Multiple Objects', MergeMultipleObjects);
  it('Merge with Conditional Logic', MergeWithConditionalLogic);
  it('Merge Nested Objects', MergeNestedObjects);
  it('Merge with Default Values', MergeWithDefaultValues);
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

async function MergeCustomerWithUserData() {
  const result = await db
    .table(ordersTable)
    .map((row) => row('customer').merge(db.table(usersTable).getAll('email', row('customer')('email')).nth(0)))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((doc) => {
    expect(doc).to.have.property('email');
    expect(doc).to.have.property('name');
    expect(doc).to.have.property('age');
    expect(doc).to.have.property('isActive');
    expect(doc).to.have.property('metadata');
    expect(doc.metadata).to.have.property('preferences');
  });

  const antoineData = result.find((doc) => doc.email === 'antoine@example.com');
  expect(antoineData).to.not.be.undefined;
  const antoine = antoineData!;
  expect(antoine.age).to.equal(25);
  expect(antoine.isActive).to.equal(true);
  expect(antoine.metadata?.preferences).to.be.an('object');
  if (
    typeof antoine.metadata?.preferences === 'object' &&
    antoine.metadata.preferences &&
    'theme' in antoine.metadata.preferences
  ) {
    expect(antoine.metadata.preferences.theme).to.equal('dark');
    expect(antoine.metadata.preferences.language).to.equal('fr');
  }
}

async function MergeOrderItemsWithProductData() {
  const result = await db
    .table(ordersTable)
    .map((row) =>
      row.merge({
        items: row('items').map((item) =>
          item.merge({
            productSku: item('sku'),
            productQuantity: item('quantity'),
          }),
        ),
      }),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((order) => {
    expect(order).to.have.property('items');
    expect(order.items).to.be.an('array');
    order.items.forEach((item) => {
      expect(item).to.have.property('sku');
      expect(item).to.have.property('quantity');
      expect(item).to.have.property('productSku');
      expect(item).to.have.property('productQuantity');
    });
  });

  const orderWithLaptop = result.find((order) => order.items.some((item) => item.sku === 'LAPTOP-001'));
  expect(orderWithLaptop).to.not.be.undefined;
  const laptopItem = orderWithLaptop!.items.find((item) => item.sku === 'LAPTOP-001');
  const laptop = laptopItem!;
  expect(laptop.productSku).to.equal('LAPTOP-001');
  expect(laptop.productQuantity).to.equal(1);
}

async function MergeMultipleObjects() {
  const result = await db
    .table(ordersTable)
    .map((row) =>
      row.merge({
        customer: row('customer').merge({
          customerEmail: row('customer')('email'),
          customerName: row('customer')('name'),
        }),
        items: row('items').map((item) =>
          item.merge({
            itemSku: item('sku'),
            itemQuantity: item('quantity'),
          }),
        ),
        metadata: {
          orderDate: new Date(),
          processedBy: 'system',
        },
      }),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((order) => {
    expect(order).to.have.property('customer');
    expect(order.customer).to.have.property('email');
    expect(order.customer).to.have.property('name');
    expect(order.customer).to.have.property('customerEmail');
    expect(order.customer).to.have.property('customerName');
    expect(order).to.have.property('items');
    expect(order.items).to.be.an('array');
    expect(order).to.have.property('metadata');
    expect(order.metadata).to.have.property('orderDate');
    expect(order.metadata).to.have.property('processedBy');
  });
}

async function MergeWithConditionalLogic() {
  const result = await db
    .table(ordersTable)
    .map((row) =>
      row.merge({
        customer: row('customer').merge(db.table(usersTable).getAll('email', row('customer')('email')).nth(0)),
        status: row('status').eq('completed').default('pending'),
        totalAmount: row('totalAmount'),
        isHighValue: row('totalAmount').gt(1000),
        isPremium: row('totalAmount').gt(1200),
      }),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((order) => {
    expect(order).to.have.property('customer');
    expect(order.customer).to.have.property('age');
    expect(order).to.have.property('status');
    expect(order).to.have.property('totalAmount');
    expect(order).to.have.property('isHighValue');
    expect(order).to.have.property('isPremium');
  });

  const highValueOrder = result.find((order) => order.totalAmount > 1200);
  expect(highValueOrder).to.not.be.undefined;
  if (highValueOrder) {
    expect(highValueOrder.isHighValue).to.equal(true);
    expect(highValueOrder.isPremium).to.equal(true);
  }

  const lowValueOrder = result.find((order) => order.totalAmount <= 1000);
  expect(lowValueOrder).to.not.be.undefined;
  if (lowValueOrder) {
    expect(lowValueOrder.isHighValue).to.equal(false);
    expect(lowValueOrder.isPremium).to.equal(false);
  }
}

async function MergeNestedObjects() {
  const result = await db
    .table(ordersTable)
    .map((row) =>
      row.merge({
        customer: row('customer').merge({
          customerEmail: row('customer')('email'),
          customerName: row('customer')('name'),
        }),
        items: row('items').map((item) =>
          item.merge({
            itemSku: item('sku'),
            itemQuantity: item('quantity'),
            itemPrice: item('quantity').default(0),
          }),
        ),
      }),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((order) => {
    expect(order).to.have.property('customer');
    expect(order.customer).to.have.property('email');
    expect(order.customer).to.have.property('name');
    expect(order.customer).to.have.property('customerEmail');
    expect(order.customer).to.have.property('customerName');
    expect(order).to.have.property('items');
    order.items.forEach((item: any) => {
      expect(item).to.have.property('sku');
      expect(item).to.have.property('quantity');
      expect(item).to.have.property('itemSku');
      expect(item).to.have.property('itemQuantity');
      expect(item).to.have.property('itemPrice');
    });
  });
}

async function MergeWithDefaultValues() {
  const result = await db
    .table(ordersTable)
    .map((row) =>
      row.merge({
        customer: row('customer').merge(
          db
            .table(usersTable)
            .getAll('email', row('customer')('email'))
            .nth(0)
            .default({
              email: 'unknown@example.com',
              name: 'Unknown User',
              age: 0,
              isActive: false,
              metadata: {
                preferences: {
                  theme: 'default',
                  language: 'en',
                },
              },
            }),
        ),
        shipping: {
          method: 'standard',
          cost: 0,
          estimatedDays: 5,
        },
        payment: {
          method: 'credit_card',
          status: 'pending',
        },
      }),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((order) => {
    expect(order).to.have.property('customer');
    expect(order.customer).to.have.property('metadata');
    expect(order.customer.metadata).to.have.property('preferences');
    expect(order).to.have.property('shipping');
    expect(order.shipping).to.have.property('method');
    expect(order.shipping).to.have.property('cost');
    expect(order.shipping).to.have.property('estimatedDays');
    expect(order).to.have.property('payment');
    expect(order.payment).to.have.property('method');
    expect(order.payment).to.have.property('status');
  });
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
