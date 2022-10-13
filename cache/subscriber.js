require('dotenv').config();
const Turtlekeeper = require('../turtleKeeper');
const { redis } = require('./cache');
const { stringToHostAndPort, publishToChannel } = require('../util');
const { turtlePool } = require('../turtlePool');

const {
  CHANNEL, REPLICA_KEY, PENDING_KEY, MASTER_KEY,
} = process.env;

const subscriber = redis.duplicate();
subscriber.subscribe(CHANNEL, () => {
  console.log(`subscribe channel: ${CHANNEL}`);
});
subscriber.on('message', async (channel, message) => {
  try {
    const data = JSON.parse(message);
    const { method } = data;
    if (method === 'join') {
      if (data.role !== 'replica') {
        return;
      }
      // cluster mode
      const config = stringToHostAndPort(data.ip);
      if (turtlePool[data.ip]) {
        turtlePool[data.ip].checkRole();
        await redis.srem(PENDING_KEY, data.ip);
        return;
      }
      turtlePool[data.ip] = new Turtlekeeper(config);

      await redis.hset(REPLICA_KEY, data.ip, 1);
      await redis.srem(PENDING_KEY, data.ip);

      const newMaster = await redis.getNewMaster(2, MASTER_KEY, REPLICA_KEY);
      if (newMaster) {
        const setMasterMessage = { method: 'setMaster', ip: newMaster };
        // tell replica to become master
        await publishToChannel(setMasterMessage);
        console.log(`${newMaster} is the new master!`);
      }

      console.log(`New replica ${data.ip} has joined.`);
    } else if (method === 'setMaster') {
      console.log('data: ', JSON.stringify(data));
      if (data.deadIp) {
        turtlePool[data.deadIp]?.disconnect();
      }
      if (!data.ip) {
        return;
      }
      if (turtlePool[data.ip]) {
        turtlePool[data.ip].checkRole();
        await redis.srem(PENDING_KEY, data.ip);
      }
    }
  } catch (error) {
    console.log(error);
  }
});
