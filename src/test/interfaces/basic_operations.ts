import { Database } from '@ajs/database/beta';
import { expect } from 'chai';

const db = Database<{ [table]: TestData }>('test-basic-operations');

const table = 'test-table';
type TestData = {
  car: string;
  manufactured: Date;
  price: number;
  isElectric: boolean;
  kilometers: bigint;
};
const testData: TestData[] = [
  {
    car: 'Peugeot',
    manufactured: new Date('2003-01-01'),
    price: 3000,
    isElectric: false,
    kilometers: BigInt(9876543210),
  },
  {
    car: 'Renault',
    manufactured: new Date('1960-06-30'),
    price: -1000,
    isElectric: false,
    kilometers: BigInt(123456789012345),
  },
  {
    car: 'Citroen',
    manufactured: new Date('2040-12-31'),
    price: 0,
    isElectric: true,
    kilometers: BigInt(-100000000000000),
  },
];

describe('Basic Operations', () => {
  it('Insert', async () => {
    const response = await db.table(table).insert(testData).run();
    expect(response).to.equal(true);
  });
  it('Insert failure', async () => {
    const response = await db.table(table).insert(testData).run();
    expect(response).to.equal(false);
  });
  it('Get', async () => {});
  it('Get All', async () => {});
  it('Get By Index', async () => {});
  it('Update', async () => {});
  it('Replace', async () => {});
  it('Delete', async () => {});
});
