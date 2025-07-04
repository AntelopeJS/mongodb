import { Database } from '@ajs/database/beta';
import { expect } from 'chai';

const db = Database<{ [table]: TestData }>('test-do-operations');

const table = 'test-table';
type TestData = {
  name: string;
  age: number;
  skills: string[];
  metadata: {
    level: number;
    tags: string[];
    preferences: string[];
  };
  scores: number[];
  isActive: boolean;
  createdAt: Date;
};

let testData: TestData[] = [
  {
    name: 'Antoine',
    age: 25,
    skills: ['JavaScript', 'TypeScript', 'React'],
    metadata: {
      level: 3,
      tags: ['senior', 'frontend'],
      preferences: ['remote', 'flexible'],
    },
    scores: [85, 92, 78],
    isActive: true,
    createdAt: new Date('2023-01-15'),
  },
  {
    name: 'Alice',
    age: 30,
    skills: ['Python', 'Django', 'PostgreSQL'],
    metadata: {
      level: 4,
      tags: ['senior', 'backend'],
      preferences: ['office', 'structured'],
    },
    scores: [90, 88, 95],
    isActive: false,
    createdAt: new Date('2022-06-20'),
  },
  {
    name: 'Camille',
    age: 22,
    skills: ['Java', 'Spring', 'Microservices'],
    metadata: {
      level: 2,
      tags: ['junior', 'backend'],
      preferences: ['hybrid', 'learning'],
    },
    scores: [75, 82, 80],
    isActive: true,
    createdAt: new Date('2024-03-10'),
  },
  {
    name: 'Dominique',
    age: 35,
    skills: ['Leadership', 'Project Management'],
    metadata: {
      level: 5,
      tags: ['senior', 'management'],
      preferences: ['office', 'leadership'],
    },
    scores: [95, 98, 92],
    isActive: true,
    createdAt: new Date('2021-12-05'),
  },
];

let insertedKeys: string[] = [];

describe('Do Operations', () => {
  it('Insert Test Data', InsertTestData);
  it('Do with Merge Operation', DoWithMergeOperation);
  it('Do with Prepend Operation', DoWithPrependOperation);
  it('Do with Append Operation', DoWithAppendOperation);
  it('Do with Complex Transformation', DoWithComplexTransformation);
  it('Do with Conditional Logic', DoWithConditionalLogic);
  it('Do with Array Operations', DoWithArrayOperations);
  it('Do with Nested Object Operations', DoWithNestedObjectOperations);
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

async function DoWithMergeOperation() {
  const result = await db
    .table<TestData>(table)
    .nth(0)
    .do((order) =>
      order.merge({
        metadata: {
          level: 10,
          tags: ['expert', 'architect'],
          preferences: db.table<TestData>(table).nth(1)('metadata')('preferences'),
        },
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Antoine');
  expect(result).to.have.property('metadata');
  expect(result.metadata).to.have.property('level', 10);
  expect(result.metadata).to.have.property('tags');
  expect(result.metadata.tags).to.include('expert');
  expect(result.metadata.tags).to.include('architect');
  expect(result.metadata).to.have.property('preferences');
  expect(result.metadata.preferences).to.deep.equal(['office', 'structured']);
}

async function DoWithPrependOperation() {
  const result = await db
    .table<TestData>(table)
    .nth(0)
    .do((order) =>
      order.merge({
        skills: db.table<TestData>(table).nth(1)('skills'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Antoine');
  expect(result).to.have.property('skills');
  expect(result.skills).to.be.an('array');
  expect(result.skills).to.have.lengthOf(3);
  expect(result.skills[0]).to.equal('Python');
  expect(result.skills[1]).to.equal('Django');
  expect(result.skills[2]).to.equal('PostgreSQL');
}

async function DoWithAppendOperation() {
  const result = await db
    .table<TestData>(table)
    .nth(0)
    .do((order) =>
      order.merge({
        scores: db.table<TestData>(table).nth(2)('scores'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Antoine');
  expect(result).to.have.property('scores');
  expect(result.scores).to.be.an('array');
  expect(result.scores).to.have.lengthOf(3);
  expect(result.scores[0]).to.equal(75);
  expect(result.scores[1]).to.equal(82);
  expect(result.scores[2]).to.equal(80);
}

async function DoWithComplexTransformation() {
  const result = await db
    .table<TestData>(table)
    .nth(0)
    .do((order) =>
      order.merge({
        name: 'Antoine - Senior',
        age: order('age').add(5),
        skills: ['Node.js', 'MongoDB'],
        metadata: {
          level: order('metadata')('level').add(2),
          tags: ['fullstack'],
          preferences: order('metadata')('preferences'),
        },
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Antoine - Senior');
  expect(result).to.have.property('age', 30);
  expect(result).to.have.property('skills');
  expect(result.skills).to.include('Node.js');
  expect(result.skills).to.include('MongoDB');
  expect(result.metadata).to.have.property('level', 5);
  expect(result.metadata.tags).to.include('fullstack');
}

async function DoWithConditionalLogic() {
  const result = await db
    .table<TestData>(table)
    .nth(1)
    .do((order) =>
      order.merge({
        status: order('isActive').eq(true).default('inactive'),
        experience: order('age').gt(25).default('junior'),
        skills: order('skills'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Alice');
  expect(result).to.have.property('status');
  expect(result.status).to.be.a('boolean');
  expect(result).to.have.property('experience');
  expect(result.experience).to.be.a('boolean');
  expect(result).to.have.property('skills');
  expect(result.skills).to.be.an('array');
  expect(result.skills).to.have.lengthOf(3);
}

async function DoWithArrayOperations() {
  const result = await db
    .table<TestData>(table)
    .nth(2)
    .do((order) =>
      order.merge({
        averageScore: order('scores').avg(),
        maxScore: order('scores').max(),
        minScore: order('scores').min(),
        totalSkills: order('skills').count(),
        skills: order('skills'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Camille');
  expect(result).to.have.property('averageScore');
  expect(result.averageScore).to.be.a('number');
  expect(result.averageScore).to.be.closeTo(79, 1);
  expect(result).to.have.property('maxScore', 82);
  expect(result).to.have.property('minScore', 75);
  expect(result).to.have.property('totalSkills', 3);
  expect(result).to.have.property('skills');
  expect(result.skills).to.deep.equal(['Java', 'Spring', 'Microservices']);
}

async function DoWithNestedObjectOperations() {
  const result = await db
    .table<TestData>(table)
    .nth(3)
    .do((order) =>
      order.merge({
        profile: {
          basic: {
            name: order('name'),
            age: order('age'),
            isActive: order('isActive'),
          },
          skills: order('skills'),
          metadata: {
            preferences: ['remote-first'],
            tags: ['experienced'],
            level: order('metadata')('level'),
          },
        },
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Dominique');
  expect(result).to.have.property('profile');
  expect(result.profile).to.have.property('basic');
  expect(result.profile.basic).to.have.property('name', 'Dominique');
  expect(result.profile.basic).to.have.property('age', 35);
  expect(result.profile.basic).to.have.property('isActive', true);
  expect(result.profile).to.have.property('skills');
  expect(result.profile.skills).to.deep.equal(['Leadership', 'Project Management']);
  expect(result.profile).to.have.property('metadata');
  expect(result.profile.metadata.preferences[0]).to.equal('remote-first');
  expect(result.profile.metadata.tags).to.include('experienced');
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await db.table(table).get(key).delete().run();
  }
}
