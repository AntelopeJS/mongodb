import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../../../datasets/users';
import { getUniqueProducts, Product } from '../../../datasets/products';

const usersTableName = 'users';
const productsTableName = 'products';

const schema = new Schema<{ [usersTableName]: User; [productsTableName]: Product }>('test-union', {
  [usersTableName]: User,
  [productsTableName]: Product,
});

const usersTable = schema.instance('default').table(usersTableName);
const productsTable = schema.instance('default').table(productsTableName);

const usersData = getUniqueUsers();
const productsData = getUniqueProducts();

let insertedKeys: {
  users: string[];
  products: string[];
} = {
  users: [],
  products: [],
};

describe('Union Operations', () => {
  before(async () => {
    await schema.createInstance('default').run();
  });

  after(async () => {
    await usersTable.delete().run();
    await productsTable.delete().run();
    await schema.destroyInstance('default').run();
  });

  it('Insert Test Data', InsertTestData);
  it('Union Same Table With Filter', UnionSameTableWithFilter);
  it('Union Different Tables', UnionDifferentTables);
  it('Union With Right Map', UnionWithRightMap);
  it('Union Preserves Duplicates', UnionPreservesDuplicates);
  it('Union With Chained Operations', UnionWithChainedOperations);
  it('Chained Unions', ChainedUnions);
  it('Cleanup', CleanupTest);
});

async function InsertTestData() {
  const usersResponse = await usersTable.insert(usersData).run();
  const productsResponse = await productsTable.insert(productsData).run();

  expect(usersResponse).to.be.an('array');
  expect(productsResponse).to.be.an('array');

  insertedKeys.users = usersResponse;
  insertedKeys.products = productsResponse;
}

async function UnionSameTableWithFilter() {
  const activeUsers = usersTable.filter((u) => u.key('isActive').eq(true));
  const inactiveUsers = usersTable.filter((u) => u.key('isActive').eq(false));

  const result = await activeUsers.union(inactiveUsers).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(usersData.length);

  const activeCount = usersData.filter((u) => u.isActive).length;
  const inactiveCount = usersData.filter((u) => !u.isActive).length;
  expect(activeCount + inactiveCount).to.equal(result.length);
}

async function UnionDifferentTables() {
  const userNames = usersTable.map((u) => ({ name: u.key('name') }));
  const productNames = productsTable.map((p) => ({ name: p.key('name') }));

  const result = await userNames.union(productNames).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(usersData.length + productsData.length);

  const names = result.map((r) => r.name);
  for (const user of usersData) {
    expect(names).to.include(user.name);
  }
  for (const product of productsData) {
    expect(names).to.include(product.name);
  }
}

async function UnionWithRightMap() {
  const mappedProducts = productsTable.map((p) => ({ name: p.key('name'), price: p.key('price') }));

  const result = await usersTable.union(mappedProducts).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(usersData.length + productsData.length);

  const userResults = result.filter((r) => 'age' in r);
  const productResults = result.filter((r) => 'price' in r);

  expect(userResults).to.have.lengthOf(usersData.length);
  expect(productResults).to.have.lengthOf(productsData.length);

  for (const user of userResults) {
    expect(user).to.have.property('name');
    expect(user).to.have.property('age');
  }
  for (const product of productResults) {
    expect(product).to.have.property('name');
    expect(product).to.have.property('price');
  }
}

async function UnionPreservesDuplicates() {
  const allUsers = usersTable;

  const result = await allUsers.union(allUsers).run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(usersData.length * 2);
}

async function UnionWithChainedOperations() {
  const activeUsers = usersTable.filter((u) => u.key('isActive').eq(true));
  const inactiveUsers = usersTable.filter((u) => u.key('isActive').eq(false));

  const count = await activeUsers.union(inactiveUsers).count().run();
  expect(count).to.equal(usersData.length);

  const filtered = await activeUsers
    .union(inactiveUsers)
    .filter((u) => u.key('age').gt(25))
    .run();

  const expectedCount = usersData.filter((u) => u.age > 25).length;
  expect(filtered).to.have.lengthOf(expectedCount);
}

async function ChainedUnions() {
  const dev = usersTable.filter((u) => u.key('department').eq('Development'));
  const marketing = usersTable.filter((u) => u.key('department').eq('Marketing'));
  const management = usersTable.filter((u) => u.key('department').eq('Management'));

  const result = await dev.union(marketing).union(management).run();

  expect(result).to.be.an('array');
  const expectedCount = usersData.filter(
    (u) => u.department === 'Development' || u.department === 'Marketing' || u.department === 'Management',
  ).length;
  expect(result).to.have.lengthOf(expectedCount);
}

async function CleanupTest() {
  for (const key of insertedKeys.products) {
    await productsTable.get(key).delete().run();
  }
  for (const key of insertedKeys.users) {
    await usersTable.get(key).delete().run();
  }
}
