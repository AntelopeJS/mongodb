const { MongoMemoryServer } = require("mongodb-memory-server-core");

let mongod;

module.exports.setup = async function() {
  mongod = await MongoMemoryServer.create();

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

