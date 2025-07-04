import { Database } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../datasets/users';

const db = Database<{ [table]: User }>('test-do-operations');

const table = 'test-table';

// Utiliser le dataset unifié
const testData = getUniqueUsers();

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
    .table(table)
    .nth(0)
    .do((order) =>
      order.merge({
        metadata: {
          level: 10,
          tags: ['expert', 'architect'],
          preferences: db.table(table).nth(1)('metadata')('preferences'),
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
  expect(result.metadata.preferences).to.deep.equal({
    theme: 'light',
    language: 'en',
  });
}

async function DoWithPrependOperation() {
  const result = await db
    .table(table)
    .nth(0)
    .do((order) =>
      order.merge({
        skills: db.table(table).nth(1)('skills'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Antoine');
  expect(result).to.have.property('skills');
  expect(result.skills).to.be.an('array');
  expect(result.skills).to.have.lengthOf(3);
  expect(result.skills![0]).to.equal('Photoshop');
  expect(result.skills![1]).to.equal('Illustrator');
  expect(result.skills![2]).to.equal('Design');
}

async function DoWithAppendOperation() {
  const result = await db
    .table(table)
    .nth(0)
    .do((order) =>
      order.merge({
        scores: db.table(table).nth(2)('skills'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Antoine');
  expect(result).to.have.property('scores');
  expect(result.scores).to.be.an('array');
  expect(result.scores).to.have.lengthOf(3);
  expect(result.scores![0]).to.equal('Python');
  expect(result.scores![1]).to.equal('Django');
  expect(result.scores![2]).to.equal('PostgreSQL');
}

async function DoWithComplexTransformation() {
  const result = await db
    .table(table)
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
    .table(table)
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
    .table(table)
    .nth(2)
    .do((order) =>
      order.merge({
        averageScore: order('skills').count(),
        maxScore: order('skills').count(),
        minScore: order('skills').count(),
        totalSkills: order('skills').count(),
        skills: order('skills'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Camille');
  expect(result).to.have.property('averageScore');
  expect(result.averageScore).to.be.a('number');
  expect(result.averageScore).to.equal(3);
  expect(result).to.have.property('maxScore', 3);
  expect(result).to.have.property('minScore', 3);
  expect(result).to.have.property('totalSkills', 3);
  expect(result).to.have.property('skills');
  expect(result.skills).to.deep.equal(['Python', 'Django', 'PostgreSQL']);
}

async function DoWithNestedObjectOperations() {
  const result = await db
    .table(table)
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
  expect(result.profile.skills).to.deep.equal(['Embedded C', 'C++']);
  expect(result.profile).to.have.property('metadata');
  if (
    typeof result.profile.metadata.preferences === 'object' &&
    result.profile.metadata.preferences &&
    'theme' in result.profile.metadata.preferences
  ) {
    expect(result.profile.metadata.preferences.theme).to.equal('dark');
  }
  expect(result.profile.metadata.tags).to.include('senior');
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await db.table(table).get(key).delete().run();
  }
}
