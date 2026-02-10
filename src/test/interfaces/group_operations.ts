import { Database } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueOrders, Order } from '../datasets/orders';

const db = Database<{ [table]: Order }>('test-group-operations');

const table = 'test-table';

// Utiliser les données commandes dédupliquées pour les groupements
const testData = getUniqueOrders(); // Prendre seulement les commandes avec un type de livraison

let insertedKeys: string[] = [];

describe('Group Operations', () => {
  it('Insert Test Data', InsertTestData);
  it('Group by Delivery Type with Simple Count', GroupByDeliveryTypeWithCount);
  it('Group by Delivery Type with Average Price', GroupByDeliveryTypeWithAveragePrice);
  it('Group by Delivery Type with Weighted Average', GroupByDeliveryTypeWithWeightedAverage);
  it('Group by Delivery Type with Sum of Totals', GroupByDeliveryTypeWithSumOfTotals);
  it('Group by Category with Complex Calculations', GroupByCategoryWithComplexCalculations);
  it('Group by Payment Status with Multiple Aggregations', GroupByPaymentStatusWithMultipleAggregations);
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

async function GroupByDeliveryTypeWithCount() {
  const result = await db
    .table(table)
    .group('deliveryType', (stream, group) => ({
      deliveryType: group,
      orderCount: stream.count(),
      totalOrders: stream.map((row) => row('orderId')).count(),
    }))
    .run();

  expect(result).to.be.an('array');
  const expectedGroupCount = new Set(testData.map((order) => order.deliveryType)).size;
  expect(result).to.have.lengthOf(expectedGroupCount);

  const expressGroup = result.find((item) => item.deliveryType === 'express');
  const standardGroup = result.find((item) => item.deliveryType === 'standard');

  const expectedExpressCount = testData.filter((order) => order.deliveryType === 'express').length;
  const expectedStandardCount = testData.filter((order) => order.deliveryType === 'standard').length;

  expect(expressGroup!.orderCount).to.equal(expectedExpressCount);
  expect(standardGroup!.orderCount).to.equal(expectedStandardCount);
  expect(expressGroup!.totalOrders).to.equal(expectedExpressCount);
  expect(standardGroup!.totalOrders).to.equal(expectedStandardCount);
}

async function GroupByDeliveryTypeWithAveragePrice() {
  const result = await db
    .table(table)
    .group('deliveryType', (stream, group) => ({
      deliveryType: group,
      totalOrders: stream.count(),
      totalAmount: stream.map((row) => row('totalAmount')).sum(),
    }))
    .run();

  expect(result).to.be.an('array');
  const expectedGroupCount = new Set(testData.map((order) => order.deliveryType)).size;
  expect(result).to.have.lengthOf(expectedGroupCount);

  const expressGroup = result.find((item) => item.deliveryType === 'express');
  const standardGroup = result.find((item) => item.deliveryType === 'standard');

  const expectedExpressCount = testData.filter((order) => order.deliveryType === 'express').length;
  const expectedStandardCount = testData.filter((order) => order.deliveryType === 'standard').length;

  expect(expressGroup!.totalOrders).to.equal(expectedExpressCount);
  const expectedExpressAmount = testData
    .filter((order) => order.deliveryType === 'express')
    .reduce((sum, order) => sum + order.totalAmount, 0);
  expect(expressGroup!.totalAmount).to.equal(expectedExpressAmount);
  expect(standardGroup!.totalOrders).to.equal(expectedStandardCount);
  const expectedStandardAmount = testData
    .filter((order) => order.deliveryType === 'standard')
    .reduce((sum, order) => sum + order.totalAmount, 0);
  expect(standardGroup!.totalAmount).to.equal(expectedStandardAmount);
}

async function GroupByDeliveryTypeWithWeightedAverage() {
  const result = await db
    .table(table)
    .group('deliveryType', (stream, group) => ({
      deliveryType: group,
      totalOrders: stream.count(),
      totalAmount: stream.sum('totalAmount'),
    }))
    .run();

  expect(result).to.be.an('array');
  const expectedGroupCount = new Set(testData.map((order) => order.deliveryType)).size;
  expect(result).to.have.lengthOf(expectedGroupCount);

  const expressGroup = result.find((item) => item.deliveryType === 'express');
  const standardGroup = result.find((item) => item.deliveryType === 'standard');

  const expectedExpressCount = testData.filter((order) => order.deliveryType === 'express').length;
  const expectedStandardCount = testData.filter((order) => order.deliveryType === 'standard').length;

  expect(expressGroup!.totalOrders).to.equal(expectedExpressCount);
  const expectedExpressAmount = testData
    .filter((order) => order.deliveryType === 'express')
    .reduce((sum, order) => sum + order.totalAmount, 0);
  expect(expressGroup!.totalAmount).to.equal(expectedExpressAmount);
  expect(standardGroup!.totalOrders).to.equal(expectedStandardCount);
  const expectedStandardAmount = testData
    .filter((order) => order.deliveryType === 'standard')
    .reduce((sum, order) => sum + order.totalAmount, 0);
  expect(standardGroup!.totalAmount).to.equal(expectedStandardAmount);
}

async function GroupByDeliveryTypeWithSumOfTotals() {
  const result = await db
    .table(table)
    .group('deliveryType', (stream, group) => ({
      deliveryType: group,
      totalOrders: stream.count(),
      totalRevenue: stream.sum('totalAmount'),
    }))
    .run();

  expect(result).to.be.an('array');
  const expectedGroupCount = new Set(testData.map((order) => order.deliveryType)).size;
  expect(result).to.have.lengthOf(expectedGroupCount);

  const expressGroup = result.find((item) => item.deliveryType === 'express');
  const standardGroup = result.find((item) => item.deliveryType === 'standard');

  const expectedExpressCount = testData.filter((order) => order.deliveryType === 'express').length;
  const expectedStandardCount = testData.filter((order) => order.deliveryType === 'standard').length;

  expect(expressGroup!.totalOrders).to.equal(expectedExpressCount);
  const expectedExpressRevenue = testData
    .filter((order) => order.deliveryType === 'express')
    .reduce((sum, order) => sum + order.totalAmount, 0);
  expect(expressGroup!.totalRevenue).to.equal(expectedExpressRevenue);
  expect(standardGroup!.totalOrders).to.equal(expectedStandardCount);
  const expectedStandardRevenue = testData
    .filter((order) => order.deliveryType === 'standard')
    .reduce((sum, order) => sum + order.totalAmount, 0);
  expect(standardGroup!.totalRevenue).to.equal(expectedStandardRevenue);
}

async function GroupByCategoryWithComplexCalculations() {
  const result = await db
    .table(table)
    .group('deliveryType', (stream, group) => ({
      deliveryType: group,
      totalOrders: stream.count(),
      averageAmount: stream.avg('totalAmount'),
    }))
    .run();

  expect(result).to.be.an('array');
  const expectedGroupCount = new Set(testData.map((order) => order.deliveryType)).size;
  expect(result).to.have.lengthOf(expectedGroupCount);

  result.forEach((item) => {
    expect(item).to.have.property('deliveryType');
    expect(item).to.have.property('totalOrders');
    expect(item).to.have.property('averageAmount');
    expect(item.totalOrders).to.be.a('number');
    expect(item.averageAmount).to.be.a('number');
  });
}

async function GroupByPaymentStatusWithMultipleAggregations() {
  const result = await db
    .table(table)
    .group('isPaid', (stream, group) => ({
      isPaid: group,
      orderCount: stream.count(),
      totalRevenue: stream.sum('totalAmount'),
    }))
    .run();

  expect(result).to.be.an('array');
  const expectedGroupCount = new Set(testData.map((order) => order.isPaid)).size;
  expect(result).to.have.lengthOf(expectedGroupCount);

  const paidGroup = result.find((item) => item.isPaid === true);
  const unpaidGroup = result.find((item) => item.isPaid === false);

  const expectedPaidCount = testData.filter((order) => order.isPaid === true).length;
  const expectedUnpaidCount = testData.filter((order) => order.isPaid === false).length;

  expect(paidGroup!.orderCount).to.equal(expectedPaidCount);
  expect(unpaidGroup!.orderCount).to.equal(expectedUnpaidCount);

  const expectedPaidRevenue = testData
    .filter((order) => order.isPaid === true)
    .reduce((sum, order) => sum + order.totalAmount, 0);
  const expectedUnpaidRevenue = testData
    .filter((order) => order.isPaid === false)
    .reduce((sum, order) => sum + order.totalAmount, 0);

  expect(paidGroup!.totalRevenue).to.equal(expectedPaidRevenue);
  expect(unpaidGroup!.totalRevenue).to.equal(expectedUnpaidRevenue);
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await db.table(table).get(key).delete().run();
  }
}
