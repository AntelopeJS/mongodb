import { Database } from '@ajs/database/beta';
import { expect } from 'chai';

const db = Database<{ [table]: TestData }>('test-filters');

const table = 'test-table';
type TestData = {
  name: string;
  age: number;
  salary: number;
  isActive: boolean;
  department: string;
  skills: string[];
  createdAt: Date;
  score: bigint;
  metadata: {
    level: number;
    tags: string[];
  };
};

let testData: TestData[] = [
  {
    name: 'Antoine',
    age: 25,
    salary: 50000,
    isActive: true,
    department: 'Développement',
    skills: ['JavaScript', 'TypeScript', 'React'],
    createdAt: new Date('2023-01-15'),
    score: BigInt(1000000000000000),
    metadata: {
      level: 3,
      tags: ['senior', 'frontend'],
    },
  },
  {
    name: 'Alice',
    age: 30,
    salary: -15000,
    isActive: false,
    department: 'Marketing',
    skills: ['Photoshop', 'Illustrator'],
    createdAt: new Date('2022-06-20'),
    score: BigInt(-999999999999999),
    metadata: {
      level: 1,
      tags: ['junior', 'design'],
    },
  },
  {
    name: 'Camille',
    age: 0,
    salary: 0,
    isActive: true,
    department: 'Développement',
    skills: ['Python', 'Django', 'PostgreSQL'],
    createdAt: new Date('2024-03-10'),
    score: BigInt(0),
    metadata: {
      level: 2,
      tags: ['mid-level', 'backend'],
    },
  },
  {
    name: 'Dominique',
    age: 35,
    salary: 90000,
    isActive: false,
    department: 'Management',
    skills: ['Leadership', 'Project Management'],
    createdAt: new Date('2021-12-05'),
    score: BigInt('999999999999999999'),
    metadata: {
      level: 5,
      tags: ['senior', 'management'],
    },
  },
  {
    name: 'Émilie',
    age: 28,
    salary: 60000,
    isActive: true,
    department: 'Développement',
    skills: ['Java', 'Spring', 'Microservices'],
    createdAt: new Date('2023-08-30'),
    score: BigInt(-500000000000000),
    metadata: {
      level: 4,
      tags: ['senior', 'backend'],
    },
  },
];

let insertedKeys: string[] = [];

describe('Filter Operations', () => {
  it('Insert Test Data', InsertTestData);
  it('Filter by String Equality', FilterByStringEquality);
  it('Filter by Number Comparison', FilterByNumberComparison);
  it('Filter by Boolean', FilterByBoolean);
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

async function FilterByStringEquality() {
  const result = await db
    .table(table)
    .filter((doc) => doc('department').eq('Développement'))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((doc) => {
    expect(doc.department).to.equal('Développement');
  });

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Antoine', 'Camille', 'Émilie']);
}

async function FilterByNumberComparison() {
  const result = await db
    .table(table)
    .filter((doc) => doc('salary').gt(0))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((doc) => {
    expect(doc.salary).to.be.greaterThan(0);
  });

  const salaries = result.map((doc) => doc.salary).sort();
  expect(salaries).to.deep.equal([50000, 60000, 90000]);
}

async function FilterByBoolean() {
  const result = await db
    .table(table)
    .filter((doc) => doc('isActive').eq(true))
    .run();

  expect(result).to.be.an('array');
  expect(result).to.have.lengthOf(3);

  result.forEach((doc) => {
    expect(doc.isActive).to.equal(true);
  });

  const names = result.map((doc) => doc.name).sort();
  expect(names).to.deep.equal(['Antoine', 'Camille', 'Émilie']);
}



async function CleanupTest() {
  for (const key of insertedKeys) {
    await db.table(table).get(key).delete().run();
  }
} 