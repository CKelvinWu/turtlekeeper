require('dotenv').config();
require('./cache/subscriber');
const Turtlekeeper = require('./turtleKeeper');
const { redis } = require('./cache/cache');
const { stringToHostAndPort, getReplicas, getMaster } = require('./util');
const { turtlePool } = require('./turtlePool');

const { REPLICA_KEY, PENDING_KEY } = process.env;

(async () => {
  const master = await getMaster();
  if (master) {
    const masterConfig = stringToHostAndPort(master);
    turtlePool[master] = new Turtlekeeper(masterConfig, 'master');
  }

  // make pendings instance become replica
  const replicasLength = await redis.scard(PENDING_KEY);
  const pendingReplicas = await redis.spop(PENDING_KEY, replicasLength);
  if (pendingReplicas.length) {
    const replicasState = [];
    pendingReplicas.forEach((replica) => replicasState.push(replica, 1));
    await redis.hmset(REPLICA_KEY, replicasState);
  }

  // Create existing replicas connections
  const replicas = await getReplicas();
  replicas.forEach(async (replica) => {
    console.log(`replica:${replica}`);
    const replicaConfig = stringToHostAndPort(replica);
    turtlePool[replica] = new Turtlekeeper(replicaConfig);
  });
})();
