import { Database } from '@ajs/database/beta';
import { expect } from 'chai';

const db = Database<{ [table]: OrderData }>('test-group-operations');

const table = 'test-table';
type OrderItem = {
  name: string;
  price: number;
  quantity: number;
  category: string;
};

type OrderData = {
  orderId: string;
  customerName: string;
  deliveryType: string;
  orderDate: Date;
  caddy: OrderItem[];
  totalAmount: number;
  isPaid: boolean;
};

let testData: OrderData[] = [
  {
    orderId: 'ORD-001',
    customerName: 'Antoine',
    deliveryType: 'express',
    orderDate: new Date('2024-01-15'),
    caddy: [
      { name: 'Laptop', price: 1200, quantity: 1, category: 'electronics' },
      { name: 'Mouse', price: 25, quantity: 2, category: 'accessories' },
    ],
    totalAmount: 1250,
    isPaid: true,
  },
  {
    orderId: 'ORD-002',
    customerName: 'Alice',
    deliveryType: 'standard',
    orderDate: new Date('2024-01-16'),
    caddy: [
      { name: 'Book', price: 15, quantity: 3, category: 'books' },
      { name: 'Pen', price: 5, quantity: 5, category: 'office' },
    ],
    totalAmount: 70,
    isPaid: false,
  },
  {
    orderId: 'ORD-003',
    customerName: 'Camille',
    deliveryType: 'express',
    orderDate: new Date('2024-01-17'),
    caddy: [
      { name: 'Phone', price: 800, quantity: 1, category: 'electronics' },
      { name: 'Case', price: 20, quantity: 1, category: 'accessories' },
      { name: 'Charger', price: 30, quantity: 2, category: 'accessories' },
    ],
    totalAmount: 880,
    isPaid: true,
  },
  {
    orderId: 'ORD-004',
    customerName: 'Dominique',
    deliveryType: 'standard',
    orderDate: new Date('2024-01-18'),
    caddy: [
      { name: 'Tablet', price: 500, quantity: 1, category: 'electronics' },
      { name: 'Keyboard', price: 80, quantity: 1, category: 'accessories' },
    ],
    totalAmount: 580,
    isPaid: true,
  },
  {
    orderId: 'ORD-005',
    customerName: 'Émilie',
    deliveryType: 'express',
    orderDate: new Date('2024-01-19'),
    caddy: [
      { name: 'Monitor', price: 300, quantity: 2, category: 'electronics' },
      { name: 'Cable', price: 10, quantity: 4, category: 'accessories' },
    ],
    totalAmount: 640,
    isPaid: false,
  },
];

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
    .table<OrderData>(table)
    .group('deliveryType', (stream, group) => ({
      deliveryType: group,
      orderCount: stream.count(),
      totalOrders: stream.map((row) => row('orderId')).count(),
    }))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);

  const expressGroup = result.find((item) => item.deliveryType === 'express');
  const standardGroup = result.find((item) => item.deliveryType === 'standard');

  expect(expressGroup!.orderCount).to.equal(3);
  expect(standardGroup!.orderCount).to.equal(2);
  expect(expressGroup!.totalOrders).to.equal(3);
  expect(standardGroup!.totalOrders).to.equal(2);
}

async function GroupByDeliveryTypeWithAveragePrice() {
  const result = await db
    .table<OrderData>(table)
    .group('deliveryType', (stream, group) => ({
      deliveryType: group,
      averageTotalAmount: stream.map((row) => row('totalAmount')).avg(),
      maxTotalAmount: stream.map((row) => row('totalAmount')).max(),
      minTotalAmount: stream.map((row) => row('totalAmount')).min(),
    }))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);

  const expressGroup = result.find((item) => item.deliveryType === 'express');
  const standardGroup = result.find((item) => item.deliveryType === 'standard');

  expect(expressGroup!.averageTotalAmount).to.be.a('number');
  expect(expressGroup!.averageTotalAmount).to.be.closeTo(923.33, 1);
  expect(expressGroup!.maxTotalAmount).to.equal(1250);
  expect(expressGroup!.minTotalAmount).to.equal(640);

  expect(standardGroup!.averageTotalAmount).to.be.a('number');
  expect(standardGroup!.averageTotalAmount).to.be.closeTo(325, 1);
  expect(standardGroup!.maxTotalAmount).to.equal(580);
  expect(standardGroup!.minTotalAmount).to.equal(70);
}

async function GroupByDeliveryTypeWithWeightedAverage() {
  const result = await db
    .table<OrderData>(table)
    .group('deliveryType', (stream, group) => ({
      deliveryType: group,
      averagePrice: stream
        .map((row) => {
          const weightedSum = row('caddy')
            .map((item) => item('price').mul(item('quantity')))
            .sum()
            .default(0);
          const totalCount = row('caddy')
            .map((item) => item('quantity'))
            .sum()
            .default(0);
          return weightedSum.div(totalCount);
        })
        .avg()
        .default(0),
    }))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);

  const expressGroup = result.find((item) => item.deliveryType === 'express');
  const standardGroup = result.find((item) => item.deliveryType === 'standard');

  expect(expressGroup!.averagePrice).to.be.a('number');
  expect(expressGroup!.averagePrice).to.be.greaterThan(0);
  expect(standardGroup!.averagePrice).to.be.a('number');
  expect(standardGroup!.averagePrice).to.be.greaterThan(0);
}

async function GroupByDeliveryTypeWithSumOfTotals() {
  const result = await db
    .table<OrderData>(table)
    .group('deliveryType', (stream, group) => ({
      deliveryType: group,
      sum: stream
        .map((row) =>
          row('caddy')
            .map((item) => item('price'))
            .sum()
            .default(0),
        )
        .sum()
        .default(0),
      totalRevenue: stream.map((row) => row('totalAmount')).sum(),
    }))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);

  const expressGroup = result.find((item) => item.deliveryType === 'express');
  const standardGroup = result.find((item) => item.deliveryType === 'standard');

  expect(expressGroup!.sum).to.be.a('number');
  expect(expressGroup!.sum).to.be.greaterThan(0);
  expect(expressGroup!.totalRevenue).to.equal(2770);

  expect(standardGroup!.sum).to.be.a('number');
  expect(standardGroup!.sum).to.be.greaterThan(0);
  expect(standardGroup!.totalRevenue).to.equal(650);
}

async function GroupByCategoryWithComplexCalculations() {
  const result = await db
    .table<OrderData>(table)
    .group('caddy', (stream, group) => ({
      category: group,
      totalItems: stream
        .map((row) =>
          row('caddy')
            .filter((item) => item('category').eq(group))
            .map((item) => item('quantity'))
            .sum()
            .default(0),
        )
        .sum()
        .default(0),
      averagePrice: stream
        .map((row) =>
          row('caddy')
            .filter((item) => item('category').eq(group))
            .map((item) => item('price'))
            .avg()
            .default(0),
        )
        .avg()
        .default(0),
    }))
    .run();

  expect(result).to.be.an('array');
  expect(result.length).to.be.greaterThan(0);

  result.forEach((item) => {
    expect(item).to.have.property('category');
    expect(item).to.have.property('totalItems');
    expect(item).to.have.property('averagePrice');
    expect(item.totalItems).to.be.a('number');
    expect(item.averagePrice).to.be.a('number');
  });
}

async function GroupByPaymentStatusWithMultipleAggregations() {
  const result = await db
    .table<OrderData>(table)
    .group('isPaid', (stream, group) => ({
      isPaid: group,
      orderCount: stream.count(),
      totalRevenue: stream.map((row) => row('totalAmount')).sum(),
      averageOrderValue: stream.map((row) => row('totalAmount')).avg(),
      customerCount: stream.map((row) => row('customerName')).count(),
      deliveryTypes: stream.map((row) => row('deliveryType')).distinct(),
    }))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);

  const paidGroup = result.find((item) => item.isPaid === true);
  const unpaidGroup = result.find((item) => item.isPaid === false);

  expect(paidGroup!.orderCount).to.equal(3);
  expect(unpaidGroup!.orderCount).to.equal(2);

  expect(paidGroup!.totalRevenue).to.equal(2710);
  expect(unpaidGroup!.totalRevenue).to.equal(710);

  expect(paidGroup!.averageOrderValue).to.be.closeTo(903.33, 1);
  expect(unpaidGroup!.averageOrderValue).to.be.closeTo(355, 1);

  expect(paidGroup!.customerCount).to.equal(3);
  expect(unpaidGroup!.customerCount).to.equal(2);

  expect(paidGroup!.deliveryTypes).to.be.an('array');
  expect(unpaidGroup!.deliveryTypes).to.be.an('array');
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await db.table(table).get(key).delete().run();
  }
} 