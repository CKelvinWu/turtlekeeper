const net = require('net');

// const socket = new net.Socket();
const crypto = require('crypto');
const { redis } = require('./redis');

const randomId = () => crypto.randomBytes(8).toString('hex');

const {
  QUORUM, MASTER_KEY, REPLICA_KEY,
} = process.env;

const {
  removeReplica, getReqHeader, publishToChannel,
  setMaster, getReplicas, getRole, getCurrentIp, voteInstance, getMaster,
} = require('./util');

redis.defineCommand('newMaster', {
  lua: `
  local data = redis.call("GET", KEYS[1])
  if (data) then
    return false
  end

  local replicas = redis.call("HKEYS", KEYS[2])
  if (#replicas == 0) then
    return false
  end
  
  local rand = math.random(#replicas)
  local newMaster = replicas[rand]
  redis.call("SET", KEYS[1], newMaster)
  redis.call("HDEL", KEYS[2], newMaster)

  return newMaster`,
});

redis.defineCommand('canVote', {
  lua: `
  local hostIp = KEYS[1]
  local myIp = KEYS[2]
  local REPLICA_KEY = KEYS[3]
  local currentTime = tonumber(ARGV[1])
  local voteCountTime = tonumber(ARGV[2])
  local quorum = tonumber(ARGV[3])
  
  redis.call('HSET', hostIp, myIp, currentTime)
  local votes = redis.call('HVALS', hostIp)
  local vote = 0
  for i=1,#votes do
    if (currentTime - votes[i] < voteCountTime) then
      vote = vote + 1
    end
  end

  if (vote ~= quorum) then
    return false
  end
  
  redis.call('DEL', hostIp)
  return true
  `,
});

class Turtlekeeper {
  constructor(config, role) {
    this.config = config;
    this.hostIp = `${config?.host}:${config?.port}`;
    this.role = role;
    this.unhealthyCount = 0;
    this.heartrate = 3000;
    this.id = randomId();
    this.socket = new net.Socket();
    this.setIp();
  }

  async setIp() {
    this.ip = await getCurrentIp();
    this.connect();
  }

  async reconnect() {
    this.client?.removeAllListeners();
    this.connect();
  }

  async connect() {
    const client = this.socket.connect(this.config);
    client.on('readable', async () => {
      const reqHeader = getReqHeader(client);
      if (!reqHeader) return;
      const object = JSON.parse(reqHeader);
      if (object.success) {
        this[object.method](object);
      }
    });

    client.on('error', () => {});
    client.on('end', () => {
    });

    this.client = client;
    this.send({
      id: this.id,
      role: 'turtlekeeper',
      method: 'heartbeat',
      ip: this.ip,
    });
    const newMaster = await redis.newMaster(2, MASTER_KEY, REPLICA_KEY);
    if (newMaster) {
      const masterInfo = { method: 'setMaster', ip: newMaster };
      this.role = 'master';
      // tell replica to become master
      await publishToChannel(masterInfo);
      console.log(`${newMaster} is the new master!`);
    }

    this.sendHeartbeat();
  }

  disconnect() {
    this.client.end();
  }

  sendHeartbeat() {
    // If not getting response of heart beat, treat it as an connection error.
    this.heartbeatTimeout = setTimeout(() => {
      this.heartbeatError();
    }, this.heartrate);

    // Sending next heratbeat
    this.interval = setTimeout(() => {
      this.sendHeartbeat();
    }, this.heartrate);
    this.send({
      id: this.id,
      role: 'turtlekeeper',
      method: 'heartbeat',
      ip: this.ip,
    });
  }

  heartbeat(object) {
    this.role = object.role;
    this.clearHeartbeatTimeout();
    if (object.role === 'master') {
      console.log(`master ${object.ip} alive\n`);
    } else if (object.role === 'replica') {
      console.log(`replica ${object.ip} alive\n`);
    }
  }

  async heartbeatError() {
    try {
      this.clearHeartbeat();
      const { hostIp } = this;
      const role = await getRole(hostIp);
      if (!role) {
        console.log(`${hostIp} disconnect! `);
        this.client.end();
        return;
      }
      this.role = role;

      // handle unstable connection
      if (this.unhealthyCount < 3) {
        console.log(`unhealthy ${this.role}: ${this.hostIp}`);
        this.unhealthyCount++;
        this.reconnect();
        return;
      }
      await this.vote();
    } catch (error) {
      console.log('connection failed');
    }
    // reset unhealthyCount
    this.unhealthyCount = 0;
  }

  async vote() {
    const { hostIp } = this;
    const voteCountTime = 9000;
    const canVote = await redis.canVote(
      3,
      hostIp,
      this.ip,
      REPLICA_KEY,
      Date.now(),
      voteCountTime,
      QUORUM,
    );

    if (!canVote) {
      this.unhealthyCount = 0;
      this.reconnect();
      return;
    }

    if (this.role === 'replica') {
      console.log('replica down! remove from list');
      await removeReplica(hostIp);
      this.client.end();
      return;
    }

    // One who get the vote is exactly equal to quorum can select the new master;
    console.log('I am selecting the new master');

    // vote a replica from lists
    const replicas = await getReplicas();
    if (!replicas.length) {
      console.log('No turtleMQ is alive...');
      return;
    }
    const newMaster = { method: 'setMaster', ip: replicas[Math.floor(Math.random() * replicas.length)] };
    await removeReplica(newMaster.ip);
    await setMaster(newMaster.ip);

    // tell replica to become master
    await publishToChannel(newMaster);
    console.log(`${newMaster.ip} is the new master!`);
  }

  clearHeartbeat() {
    clearTimeout(this.interval);
  }

  clearHeartbeatTimeout() {
    clearTimeout(this.heartbeatTimeout);
  }

  send(messages) {
    this.client?.write(`${JSON.stringify(messages)}\r\n\r\n`);
  }

  end() {
    this.client.end();
  }
}

module.exports = Turtlekeeper;
