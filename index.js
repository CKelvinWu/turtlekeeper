require('dotenv').config();
require('./cache/listener');
const Turtlekeeper = require('./turtleKeeper');
const { redis } = require('./cache/redis');
const {
  getMasterConfig,
  getReplicasConfig,
} = require('./util');

const { REPLICA_KEY, PENDING_KEY } = process.env;

(async () => {
  const masterConfig = await getMasterConfig();
  if (masterConfig) {
    const turtlekeeper = new Turtlekeeper(masterConfig, 'master');
  }
  const replicasLength = await redis.scard(PENDING_KEY);
  const replicas = await redis.spop(PENDING_KEY, replicasLength);
  if (replicas.length) {
    const replicasState = [];
    replicas.forEach((replica) => replicasState.push(replica, 1));
    await redis.hmset(REPLICA_KEY, replicasState);
  }
  // Create existing replicas connections
  const replicasConfig = await getReplicasConfig();
  replicasConfig.forEach(async (replicaConfig) => {
    // await createConnection(replicaConfig, 'replica');
    const turtlekeeper = new Turtlekeeper(replicaConfig, 'replica');
  });
})();
