require('dotenv').config();
require('./cache/listener');
const Turtlekeeper = require('./turtleKeeper');
const { redis } = require('./cache/redis');
const {
  getMasterConfig,
  getReplicasConfig,
} = require('./util');

const { REPLICA_KEY, PENDING_KEY } = process.env;

// FIXME: don't need this function
// function createConnection(config, role) {
//   const turtleKeeper = new Turtlekeeper(config, role);
//   return turtleKeeper;
// }

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

// // TODO: 抽出
// const subscriber = redis.duplicate();
// subscriber.subscribe(CHANNEL, () => {
//   console.log(`subscribe channel: ${CHANNEL}`);
// });
// subscriber.on('message', async (channel, message) => {
//   const data = JSON.parse(message);
//   if (data.method === 'join') {
//     const master = await getMaster();
//     const isMaster = (master === data.ip);

//     // cluster mode
//     if (data.role === 'replica') {
//       if (isMaster) {
//         await redis.srem(PENDING_KEY, data.ip);
//         return;
//       }
//       const isReplica = await redis.hget(REPLICA_KEY, data.ip);
//       if (isReplica) {
//         return;
//       }
//       await redis.srem(PENDING_KEY, data.ip);
//       await redis.hset(REPLICA_KEY, data.ip, 1);
//       const replicaConfig = stringToHostAndPort(data.ip);
//       // await createConnection(replicaConfig, 'replica');
//       const turtlekeeper = new Turtlekeeper(replicaConfig, 'replica');
//       console.log(`New replica ${data.ip} has joined.`);
//     } else if (data.role === 'master') {
//       if (isMaster) {
//         const masterConfig = stringToHostAndPort(data.ip);
//         const turtlekeeper = new Turtlekeeper(masterConfig, 'master');
//         console.log('new master!!!');
//       }
//     }
//   }
// });
