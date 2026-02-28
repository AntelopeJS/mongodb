import { Schema } from '@ajs/database/beta';
import { expect } from 'chai';
import { getUniqueUsers, User } from '../../../datasets/users';

const tableName = 'test-table';
const schema = new Schema<{ [tableName]: User }>('test-do-operations', { [tableName]: User });
const table = schema.default.table(tableName);

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
  const response = await table.insert(testData).run();
  expect(response).to.be.an('array');

  expect(response).to.have.lengthOf(testData.length);
  response.forEach((val) => {
    expect(val).to.be.a('string');
  });
  insertedKeys = response;
}

async function DoWithMergeOperation() {
  const result = await table
    .get(insertedKeys[0])
    .do((order) =>
      order.merge({
        metadata: {
          level: 10,
          tags: ['expert', 'architect'],
          preferences: table.get(insertedKeys[1]).key('metadata').key('preferences'),
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
  const result = await table
    .get(insertedKeys[0])
    .do((order) =>
      order.merge({
        skills: table.get(insertedKeys[1]).key('skills'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Antoine');
  expect(result).to.have.property('skills');
  expect(result.skills).to.be.an('array');
  const expectedSkillsCount = testData[1].skills?.length || 0;
  expect(result.skills).to.have.lengthOf(expectedSkillsCount);
  expect(result.skills![0]).to.equal('Photoshop');
  expect(result.skills![1]).to.equal('Illustrator');
  expect(result.skills![2]).to.equal('Design');
}

async function DoWithAppendOperation() {
  const result = await table
    .get(insertedKeys[0])
    .do((order) =>
      order.merge({
        scores: table.get(insertedKeys[2]).key('skills'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Antoine');
  expect(result).to.have.property('scores');
  expect(result.scores).to.be.an('array');
  const expectedScoresCount = testData[2].skills?.length || 0;
  expect(result.scores).to.have.lengthOf(expectedScoresCount);
  expect(result.scores![0]).to.equal('Python');
  expect(result.scores![1]).to.equal('Django');
  expect(result.scores![2]).to.equal('PostgreSQL');
}

async function DoWithComplexTransformation() {
  const result = await table
    .get(insertedKeys[0])
    .do((order) =>
      order.merge({
        name: 'Antoine - Senior',
        age: order.key('age').add(5),
        skills: ['Node.js', 'MongoDB'],
        metadata: {
          level: order.key('metadata').key('level').add(2),
          tags: ['fullstack'],
          preferences: order.key('metadata').key('preferences'),
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
  const result = await table
    .get(insertedKeys[1])
    .do((order) =>
      order.merge({
        status: order.key('isActive').eq(true).default('inactive'),
        experience: order.key('age').gt(25).default('junior'),
        skills: order.key('skills'),
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
  const expectedSkillsCount = testData[1].skills?.length || 0;
  expect(result.skills).to.have.lengthOf(expectedSkillsCount);
}

async function DoWithArrayOperations() {
  const result = await table
    .get(insertedKeys[2])
    .do((order) =>
      order.merge({
        averageScore: order.key('skills').count(),
        maxScore: order.key('skills').count(),
        minScore: order.key('skills').count(),
        totalSkills: order.key('skills').count(),
        skills: order.key('skills'),
      }),
    )
    .run();

  expect(result).to.be.an('object');
  expect(result).to.have.property('name', 'Camille');
  expect(result).to.have.property('averageScore');
  expect(result.averageScore).to.be.a('number');
  const expectedSkillsCount = testData[2].skills?.length || 0;
  expect(result.averageScore).to.equal(expectedSkillsCount);
  expect(result).to.have.property('maxScore', expectedSkillsCount);
  expect(result).to.have.property('minScore', expectedSkillsCount);
  expect(result).to.have.property('totalSkills', expectedSkillsCount);
  expect(result).to.have.property('skills');
  expect(result.skills).to.deep.equal(['Python', 'Django', 'PostgreSQL']);
}

async function DoWithNestedObjectOperations() {
  const result = await table
    .get(insertedKeys[3])
    .do((order) =>
      order.merge({
        profile: {
          basic: {
            name: order.key('name'),
            age: order.key('age'),
            isActive: order.key('isActive'),
          },
          skills: order.key('skills'),
          metadata: {
            preferences: ['remote-first'],
            tags: ['experienced'],
            level: order.key('metadata').key('level'),
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
  expect(result.profile.metadata.tags).to.include('experienced');
}

async function CleanupTest() {
  for (const key of insertedKeys) {
    await table.get(key).delete().run();
  }
}
