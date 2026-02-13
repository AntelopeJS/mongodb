const { MongoMemoryReplSet } = require("mongodb-memory-server-core");

let mongod;

module.exports.setup = async function() {
  mongod = await MongoMemoryReplSet.create({ replSet: { count: 4 }});

  return  {
    cacheFolder: '.antelope/cache',
    modules: {
      local: {
        source: {
          type: 'local',
          path: '.',
        },
        config: {
          url: mongod.getUri(),
        },
      },
    },
  };
}

module.exports.cleanup = async function() {
  await mongod.stop();
}

