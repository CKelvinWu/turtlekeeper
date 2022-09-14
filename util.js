require('dotenv').config();
const http = require('http');
const { redis } = require('./redis');

const {
  NODE_ENV, PORT, MASTER_KEY, REPLICA_KEY, STATE_KEY, CHANNEL,
} = process.env;

function stringToHostAndPort(address) {
  if (address) {
    const hostAndPort = { host: address.split(':')[0], port: address.split(':')[1] };
    return hostAndPort;
  }
  return null;
}

async function getCurrentIp() {
  return new Promise((resolve) => {
    http.get({ host: 'api.ipify.org', port: 80, path: '/' }, (res) => {
      res.on('data', (ip) => {
        console.log(`My public IP address is: ${ip}`);
        if (NODE_ENV === 'development') {
          resolve(`localhost:${PORT}`);
        }
        resolve(`${ip}:${PORT}`);
      });
    }).end();
  });
}

async function getMaster() {
  const master = await redis.get(MASTER_KEY);
  return master;
}

async function setMaster(ip) {
  await redis.set(MASTER_KEY, ip);
}

async function getMasterConfig() {
  const master = await getMaster();
  return stringToHostAndPort(master);
}

async function getReplicasConfig() {
  const replicas = await redis.hkeys(REPLICA_KEY);
  const replicasConfig = replicas.map((replica) => stringToHostAndPort(replica));
  return replicasConfig;
}

async function setReplica(key) {
  await redis.hset(REPLICA_KEY, key, 1);
}

async function getReplicas() {
  const replicas = await redis.hkeys(REPLICA_KEY);
  return replicas;
}

async function removeReplica(key) {
  await redis.hdel(REPLICA_KEY, key);
}

async function setState(state) {
  const states = ['active', 'voting'];
  if (states.includes(state)) {
    await redis.set(STATE_KEY, state);
  }
}

async function getState() {
  const state = await redis.get(STATE_KEY);
  return state;
}

function getReqHeader(client) {
  let reqHeader;
  while (true) {
    let reqBuffer = Buffer.from('');
    const buf = client.read();
    if (buf === null) break;

    reqBuffer = Buffer.concat([reqBuffer, buf]);

    // Indicating end of a request
    const marker = reqBuffer.indexOf('\r\n\r\n');
    if (marker !== -1) {
      // Record the data after \r\n\r\n
      const remaining = reqBuffer.slice(marker + 4);
      reqHeader = reqBuffer.slice(0, marker).toString();
      // Push the extra readed data back to the socket's readable stream
      client.unshift(remaining);
      break;
    }
  }
  return reqHeader;
}

async function publishToChannel(message) {
  await redis.publish(CHANNEL, JSON.stringify(message));
}

async function getRole(ip) {
  const master = await getMaster();
  if (ip === master) {
    return 'master';
  }
  const replicas = await getReplicas();
  if (replicas.includes(ip)) {
    return 'replica';
  }
  return false;
}

async function voteInstance(turtlemqIp, myIp) {
  const [, hgetall] = await redis.multi()
    .hset(turtlemqIp, myIp, Date.now())
    .hgetall(turtlemqIp).exec();
  return Object.values(hgetall[1]).filter((time) => (Date.now() - time) < 9000).length;
}

module.exports = {
  stringToHostAndPort,
  getCurrentIp,
  getMaster,
  setMaster,
  getMasterConfig,
  getReplicasConfig,
  setReplica,
  getReplicas,
  removeReplica,
  getState,
  setState,
  getReqHeader,
  publishToChannel,
  getRole,
  voteInstance,
};