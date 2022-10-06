require('dotenv').config();
const http = require('http');
const { redis } = require('./cache/cache');

const {
  NODE_ENV, PORT, MASTER_KEY, REPLICA_KEY, CHANNEL,
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
        // console.log(`My public IP address is: ${ip}`);
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

async function getMasterConfig() {
  const master = await getMaster();
  return stringToHostAndPort(master);
}

async function getReplicasConfig() {
  const replicas = await redis.hkeys(REPLICA_KEY);
  const replicasConfig = replicas.map((replica) => stringToHostAndPort(replica));
  return replicasConfig;
}

async function getReplicas() {
  const replicas = await redis.hkeys(REPLICA_KEY);
  return replicas;
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

module.exports = {
  stringToHostAndPort,
  getCurrentIp,
  getMaster,
  getMasterConfig,
  getReplicas,
  getReplicasConfig,
  getReqHeader,
  publishToChannel,
  getRole,
};
