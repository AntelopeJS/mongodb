import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { vehicles, Vehicle } from '../../../datasets/vehicles';
import { getUniqueUsers, User } from '../../../datasets/users';
import { getUniqueOrders, Order } from '../../../datasets/orders';
import { getUniqueProducts, Product } from '../../../datasets/products';

const vehiclesTableName = 'vehicles';

const singleTableSchema = new Schema<{ [vehiclesTableName]: Vehicle }>(
  'test-rl-ops',
  { [vehiclesTableName]: Vehicle },
  { rowLevel: true },
);

const t1Vehicles = singleTableSchema.instance('t1').table(vehiclesTableName);
const t2Vehicles = singleTableSchema.instance('t2').table(vehiclesTableName);

const ordersTableName = 'orders';
const usersTableName = 'users';
const productsTableName = 'products';

const multiTableSchema = new Schema<{
  [ordersTableName]: Order;
  [usersTableName]: User;
  [productsTableName]: Product;
}>(
  'test-rl-multi',
  { [ordersTableName]: Order, [usersTableName]: User, [productsTableName]: Product },
  { rowLevel: true },
);

const t1Orders = multiTableSchema.instance('t1').table(ordersTableName);
const t1Users = multiTableSchema.instance('t1').table(usersTableName);
const t1Products = multiTableSchema.instance('t1').table(productsTableName);
const t2Orders = multiTableSchema.instance('t2').table(ordersTableName);
const t2Users = multiTableSchema.instance('t2').table(usersTableName);
const t2Products = multiTableSchema.instance('t2').table(productsTableName);

let vehicleKeys: string[] = [];
let t2VehicleKeys: string[] = [];

const usersData = getUniqueUsers();
const ordersData = getUniqueOrders();
const productsData = getUniqueProducts();

let multiKeys: {
  users: string[];
  orders: string[];
  products: string[];
} = { users: [], orders: [], products: [] };

describe('Row-Level Single-Table Operations', () => {
  it('Insert + tenant_id stamp', InsertAndTenantStamp);
  it('Get by ID', GetById);
  it('GetAll by index', GetAllByIndex);
  it('Tenant isolation', TenantIsolation);
  it('Filter by equality', FilterByEquality);
  it('Filter by comparison', FilterByComparison);
  it('Filter by boolean', FilterByBoolean);
  it('OrderBy ascending', OrderByAscending);
  it('OrderBy descending', OrderByDescending);
  it('Count', Count);
  it('Count field', CountField);
  it('Sum', Sum);
  it('Avg', Avg);
  it('Min', Min);
  it('Max', Max);
  it('Nth', Nth);
  it('Slice', Slice);
  it('Pluck', Pluck);
  it('Without', Without);
  it('Distinct with field', DistinctWithField);
  it('Distinct without field', DistinctWithoutField);
  it('Between', Between);
  it('Between + filter', BetweenPlusFilter);
  it('Update scoped', UpdateScoped);
  it('Replace scoped', ReplaceScoped);
  it('Delete scoped', DeleteScoped);

  after(async () => {
    await t1Vehicles.delete().run();
    await t2Vehicles.delete().run();
  });
});

async function InsertAndTenantStamp() {
  const keys = await t1Vehicles.insert(vehicles).run();
  expect(keys).to.have.lengthOf(vehicles.length);
  vehicleKeys = keys;

  const doc = await t1Vehicles.get(keys[0]).run();
  expect(doc).to.have.property('tenant_id', 't1');
  expect(doc).to.have.property('car', vehicles[0].car);
}

async function GetById() {
  const doc = await t1Vehicles.get(vehicleKeys[0]).run();
  expect(doc).to.have.property('_id', vehicleKeys[0]);
  expect(doc).to.have.property('car', vehicles[0].car);
  expect(doc).to.have.property('price', vehicles[0].price);
}

async function GetAllByIndex() {
  const result = await t1Vehicles.getAll(false as any, 'isElectric').run();
  const expectedCount = vehicles.filter((v) => !v.isElectric).length;
  expect(result).to.have.lengthOf(expectedCount);
  result.forEach((doc) => {
    expect(doc.isElectric).to.equal(false);
  });
}

async function TenantIsolation() {
  const t2Data: Vehicle[] = [
    { car: 'BMW', manufactured: new Date('2020-01-01'), price: 40000, isElectric: true, kilometers: 5000 },
  ];
  t2VehicleKeys = await t2Vehicles.insert(t2Data).run();

  const t1Docs = await t1Vehicles.run();
  const t2Docs = await t2Vehicles.run();
  expect(t1Docs).to.have.lengthOf(vehicles.length);
  expect(t2Docs).to.have.lengthOf(1);
  expect(t2Docs[0].car).to.equal('BMW');

  const t1Cars = t1Docs.map((d) => d.car).sort();
  expect(t1Cars).to.not.include('BMW');
}

async function FilterByEquality() {
  const result = await t1Vehicles.filter((doc: any) => doc.key('car').eq('Peugeot')).run();
  const expectedCount = vehicles.filter((v) => v.car === 'Peugeot').length;
  expect(result).to.have.lengthOf(expectedCount);
  result.forEach((doc) => {
    expect(doc.car).to.equal('Peugeot');
  });
}

async function FilterByComparison() {
  const result = await t1Vehicles.filter((doc: any) => doc.key('price').gt(0)).run();
  const expectedCount = vehicles.filter((v) => v.price > 0).length;
  expect(result).to.have.lengthOf(expectedCount);
  result.forEach((doc) => {
    expect(doc.price).to.be.greaterThan(0);
  });
}

async function FilterByBoolean() {
  const result = await t1Vehicles.filter((doc: any) => doc.key('isElectric').eq(true)).run();
  const expectedCount = vehicles.filter((v) => v.isElectric).length;
  expect(result).to.have.lengthOf(expectedCount);
  result.forEach((doc) => {
    expect(doc.isElectric).to.equal(true);
  });
}

async function OrderByAscending() {
  const result = await t1Vehicles.orderBy('price', 'asc').run();
  const sorted = [...vehicles].sort((a, b) => a.price - b.price);
  expect(result).to.have.lengthOf(vehicles.length);
  result.forEach((doc, i) => {
    expect(doc.price).to.equal(sorted[i].price);
  });
}

async function OrderByDescending() {
  const result = await t1Vehicles.orderBy('price', 'desc').run();
  const sorted = [...vehicles].sort((a, b) => b.price - a.price);
  expect(result).to.have.lengthOf(vehicles.length);
  result.forEach((doc, i) => {
    expect(doc.price).to.equal(sorted[i].price);
  });
}

async function Count() {
  const result = await t1Vehicles.count().run();
  expect(result).to.equal(vehicles.length);
}

async function CountField() {
  const result = await t1Vehicles.count('car').run();
  const expectedDistinctCars = new Set(vehicles.map((v) => v.car)).size;
  expect(result).to.equal(expectedDistinctCars);
}

async function Sum() {
  const result = await t1Vehicles.sum('price').run();
  const expectedSum = vehicles.reduce((acc, v) => acc + v.price, 0);
  expect(result).to.equal(expectedSum);
}

async function Avg() {
  const result = await t1Vehicles.avg('price').run();
  const expectedAvg = vehicles.reduce((acc, v) => acc + v.price, 0) / vehicles.length;
  expect(result).to.be.closeTo(expectedAvg, 0.01);
}

async function Min() {
  const result = await t1Vehicles.min('price').run();
  const expectedMin = Math.min(...vehicles.map((v) => v.price));
  expect(result).to.equal(expectedMin);
}

async function Max() {
  const result = await t1Vehicles.max('price').run();
  const expectedMax = Math.max(...vehicles.map((v) => v.price));
  expect(result).to.equal(expectedMax);
}

async function Nth() {
  const sorted = [...vehicles].sort((a, b) => a.price - b.price);
  const result = await t1Vehicles.orderBy('price', 'asc').nth(0).run();
  expect(result).to.be.an('object');
  expect(result).to.not.be.an('array');
  expect(result).to.have.property('price', sorted[0].price);
}

async function Slice() {
  const sorted = [...vehicles].sort((a, b) => a.price - b.price);
  const result = await t1Vehicles.orderBy('price', 'asc').slice(0, 2).run();
  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(2);
  expect(result[0].price).to.equal(sorted[0].price);
  expect(result[1].price).to.equal(sorted[1].price);
}

async function Pluck() {
  const result = await t1Vehicles.pluck('car', 'price').run();
  expect(result).to.have.lengthOf(vehicles.length);
  result.forEach((doc: any) => {
    expect(doc).to.have.property('car');
    expect(doc).to.have.property('price');
    expect(doc).to.not.have.property('isElectric');
    expect(doc).to.not.have.property('kilometers');
    expect(doc).to.not.have.property('tenant_id');
  });
}

async function Without() {
  const result = await t1Vehicles.without('kilometers').run();
  expect(result).to.have.lengthOf(vehicles.length);
  result.forEach((doc: any) => {
    expect(doc).to.not.have.property('kilometers');
    expect(doc).to.have.property('car');
    expect(doc).to.have.property('price');
    expect(doc).to.have.property('tenant_id');
  });
}

async function DistinctWithField() {
  const result = await t1Vehicles.distinct('isElectric').run();
  const expectedDistinct = [...new Set(vehicles.map((v) => v.isElectric))];
  expect(result).to.have.lengthOf(expectedDistinct.length);
  expectedDistinct.forEach((val) => {
    expect(result).to.include(val);
  });
}

async function DistinctWithoutField() {
  const result = await t1Vehicles.distinct().run();
  expect(result).to.have.lengthOf(vehicles.length);
  result.forEach((doc: any) => {
    expect(doc).to.have.property('car');
    expect(doc).to.have.property('price');
    expect(doc).to.have.property('isElectric');
  });
}

async function Between() {
  const result = await t1Vehicles.between('price', -1000, 3000).run();
  const expected = vehicles.filter((v) => v.price >= -1000 && v.price < 3000);
  expect(result).to.have.lengthOf(expected.length);
  result.forEach((doc) => {
    expect(doc.price).to.be.gte(-1000);
    expect(doc.price).to.be.lt(3000);
  });
}

async function BetweenPlusFilter() {
  const result = await t1Vehicles
    .between('price', -2000, 5000)
    .filter((doc: any) => doc.key('isElectric').eq(true))
    .run();
  const expected = vehicles.filter((v) => v.price >= -2000 && v.price < 5000 && v.isElectric);
  expect(result).to.have.lengthOf(expected.length);
  result.forEach((doc) => {
    expect(doc.isElectric).to.equal(true);
  });
}

async function UpdateScoped() {
  const t2Before = await t2Vehicles.run();
  const t2PriceBefore = t2Before[0].price;

  await t1Vehicles.filter((doc: any) => doc.key('car').eq('Peugeot')).update({ price: 1 }).run();

  const t2After = await t2Vehicles.run();
  expect(t2After[0].price).to.equal(t2PriceBefore);

  const t1Updated = await t1Vehicles.filter((doc: any) => doc.key('car').eq('Peugeot')).run();
  for (const doc of t1Updated) {
    expect(doc.price).to.equal(1);
  }
}

async function ReplaceScoped() {
  const replacementData: Vehicle = {
    car: 'Fiat',
    manufactured: new Date('2022-06-15'),
    price: 12000,
    isElectric: false,
    kilometers: 50000,
  };

  const t2Before = await t2Vehicles.run();
  const t2CountBefore = t2Before.length;

  await t1Vehicles.get(vehicleKeys[0]).replace(replacementData).run();

  const replaced = await t1Vehicles.get(vehicleKeys[0]).run();
  expect(replaced).to.have.property('car', 'Fiat');
  expect(replaced).to.have.property('tenant_id', 't1');

  const t2After = await t2Vehicles.run();
  expect(t2After).to.have.lengthOf(t2CountBefore);
}

async function DeleteScoped() {
  const t2CountBefore = await t2Vehicles.count().run();

  await t1Vehicles.delete().run();

  const t1Remaining = await t1Vehicles.run();
  expect(t1Remaining).to.have.lengthOf(0);

  const t2CountAfter = await t2Vehicles.count().run();
  expect(t2CountAfter).to.equal(t2CountBefore);
}

describe('Row-Level Multi-Table Operations', () => {
  it('Insert multi-table', InsertMultiTable);
  it('Group with aggregations', GroupWithAggregations);
  it('Inner join', InnerJoin);
  it('Lookup', Lookup);
  it('Map with merge', MapWithMerge);
  it('Do with sub-query', DoWithSubQuery);

  after(async () => {
    await t1Orders.delete().run();
    await t1Users.delete().run();
    await t1Products.delete().run();
    await t2Orders.delete().run();
    await t2Users.delete().run();
    await t2Products.delete().run();
  });
});

async function InsertMultiTable() {
  multiKeys.users = await t1Users.insert(usersData).run();
  multiKeys.orders = await t1Orders.insert(ordersData).run();
  multiKeys.products = await t1Products.insert(productsData).run();

  expect(multiKeys.users).to.have.lengthOf(usersData.length);
  expect(multiKeys.orders).to.have.lengthOf(ordersData.length);
  expect(multiKeys.products).to.have.lengthOf(productsData.length);

  const t2UserCount = await t2Users.count().run();
  const t2OrderCount = await t2Orders.count().run();
  const t2ProductCount = await t2Products.count().run();
  expect(t2UserCount).to.equal(0);
  expect(t2OrderCount).to.equal(0);
  expect(t2ProductCount).to.equal(0);
}

async function GroupWithAggregations() {
  const result = await t1Orders
    .group('deliveryType', (stream, group) => ({
      deliveryType: group,
      orderCount: stream.count(),
      totalRevenue: stream.sum('totalAmount'),
    }))
    .run();

  const expectedGroupCount = new Set(ordersData.map((o) => o.deliveryType)).size;
  expect(result).to.have.lengthOf(expectedGroupCount);

  const expressGroup = result.find((item) => item.deliveryType === 'express');
  const expectedExpressCount = ordersData.filter((o) => o.deliveryType === 'express').length;
  expect(expressGroup!.orderCount).to.equal(expectedExpressCount);

  const expectedExpressRevenue = ordersData
    .filter((o) => o.deliveryType === 'express')
    .reduce((sum, o) => sum + o.totalAmount, 0);
  expect(expressGroup!.totalRevenue).to.equal(expectedExpressRevenue);
}

async function InnerJoin() {
  const result = await t1Orders
    .join(
      t1Users,
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
    expect(doc.customerEmail).to.equal(doc.customer!.email);
  });
}

async function Lookup() {
  const result = await t1Orders.lookup(t1Users, 'customerName', 'name').run();

  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((doc: any) => {
    expect(doc).to.have.property('customerName');
    expect(doc.customerName).to.be.an('object');
    expect(doc.customerName).to.have.property('name');
  });

  const antoineOrder = result.find((doc: any) => doc.orderId === 'ORD-001');
  expect(antoineOrder).to.not.equal(undefined);
  expect(antoineOrder!.customerName).to.have.property('name', 'Antoine');
}

async function MapWithMerge() {
  const result = await t1Orders
    .map((row) =>
      row.merge({
        orderSummary: {
          totalAmount: row.key('totalAmount'),
          isPaid: row.key('isPaid'),
        },
      }),
    )
    .run();

  expect(result).to.have.lengthOf(ordersData.length);

  result.forEach((doc) => {
    expect(doc).to.have.property('orderSummary');
    expect(doc.orderSummary).to.have.property('totalAmount');
    expect(doc.orderSummary).to.have.property('isPaid');
  });
}

async function DoWithSubQuery() {
  const result = await t1Users
    .get(multiKeys.users[0])
    .do((u) =>
      u.merge({
        otherSkills: t1Users.get(multiKeys.users[1]).key('skills'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', usersData[0].name);
  expect(result).to.have.property('otherSkills');
  expect(result.otherSkills).to.be.an('array');
  const expectedSkills = usersData[1].skills || [];
  expect(result.otherSkills).to.have.lengthOf(expectedSkills.length);
}
