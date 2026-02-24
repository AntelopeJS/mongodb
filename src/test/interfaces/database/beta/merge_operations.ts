/* eslint-disable @typescript-eslint/no-unused-expressions */
import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../../../datasets/users';
import { getUniqueProducts, Product } from '../../../datasets/products';
import { getUniqueOrders, Order } from '../../../datasets/orders';

const ordersTableName = 'orders';
const usersTableName = 'users';
const productsTableName = 'products';

const schema = new Schema<{ [ordersTableName]: Order; [usersTableName]: User; [productsTableName]: Product }>(
  'test-merge-operations',
  { [ordersTableName]: Order, [usersTableName]: User, [productsTableName]: Product },
);

const ordersTable = schema.default.table(ordersTableName);
const usersTable = schema.default.table(usersTableName);
const productsTable = schema.default.table(productsTableName);

// Utiliser le dataset unifié
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

async function MergeCustomerWithUserData() {
  const result = await ordersTable
    .map((row) => row.merge({ orderSummary: { totalAmount: row.key('totalAmount'), isPaid: row.key('isPaid') } }))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((doc) => {
    expect(doc).to.have.property('orderSummary');
    expect(doc.orderSummary).to.have.property('totalAmount');
    expect(doc.orderSummary).to.have.property('isPaid');
  });

  const antoineOrder = result.find((doc) => doc.customer?.email === 'antoine@example.com');
  expect(antoineOrder).to.not.be.undefined;
  expect(antoineOrder!.orderSummary.totalAmount).to.equal(1250);
}

async function MergeOrderItemsWithProductData() {
  const result = await ordersTable
    .map((row) => row.merge({ orderInfo: { totalAmount: row.key('totalAmount'), isPaid: row.key('isPaid') } }))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((order) => {
    expect(order).to.have.property('orderInfo');
    expect(order.orderInfo).to.have.property('totalAmount');
    expect(order.orderInfo).to.have.property('isPaid');
  });

  const antoineOrder = result.find((order) => order.customer?.email === 'antoine@example.com');
  expect(antoineOrder).to.not.be.undefined;
  expect(antoineOrder!.orderInfo.totalAmount).to.equal(1250);
}

async function MergeMultipleObjects() {
  const result = await ordersTable
    .map((row) =>
      row.merge({
        orderDetails: {
          customerEmail: row.key('customerEmail'),
          customerName: row.key('customerName'),
          totalAmount: row.key('totalAmount'),
        },
        metadata: {
          orderDate: new Date(),
          processedBy: 'system',
        },
      }),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((order) => {
    expect(order).to.have.property('orderDetails');
    expect(order.orderDetails).to.have.property('customerEmail');
    expect(order.orderDetails).to.have.property('customerName');
    expect(order.orderDetails).to.have.property('totalAmount');
    expect(order).to.have.property('metadata');
    expect(order.metadata).to.have.property('orderDate');
    expect(order.metadata).to.have.property('processedBy');
  });
}

async function MergeWithConditionalLogic() {
  const result = await ordersTable
    .map((row) =>
      row.merge({
        orderStatus: row.key('status').eq('completed').default('pending'),
        totalAmount: row.key('totalAmount'),
        isHighValue: row.key('totalAmount').gt(1000),
        isPremium: row.key('totalAmount').gt(1200),
      }),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((order) => {
    expect(order).to.have.property('orderStatus');
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
  const result = await ordersTable
    .map((row) =>
      row.merge({
        customerInfo: {
          customerEmail: row.key('customerEmail'),
          customerName: row.key('customerName'),
        },
        orderInfo: {
          totalAmount: row.key('totalAmount'),
          isPaid: row.key('isPaid'),
        },
      }),
    )
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((order) => {
    expect(order).to.have.property('customerInfo');
    expect(order.customerInfo).to.have.property('customerEmail');
    expect(order.customerInfo).to.have.property('customerName');
    expect(order).to.have.property('orderInfo');
    expect(order.orderInfo).to.have.property('totalAmount');
    expect(order.orderInfo).to.have.property('isPaid');
  });
}

async function MergeWithDefaultValues() {
  const result = await ordersTable
    .map((row) =>
      row.merge({
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
  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((order) => {
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
    await ordersTable.get(key).delete().run();
  }
  for (const key of insertedKeys.products) {
    await productsTable.get(key).delete().run();
  }
  for (const key of insertedKeys.users) {
    await usersTable.get(key).delete().run();
  }
}
