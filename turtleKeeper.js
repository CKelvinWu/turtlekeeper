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
    // this.client?.end();
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
      // console.log('disconnected from server');
    });

    this.client = client;
    this.send({
      id: this.id,
      role: 'turtlekeeper',
      method: 'heartbeat',
      ip: this.ip,
    });
    const newMaster = await redis.newMaster(
      2,
      MASTER_KEY,
      REPLICA_KEY,
    );
    console.log(`new: ${newMaster}`);
    if (newMaster) {
      const masterInfo = { method: 'setMaster', ip: newMaster };
      this.role = 'master';
      // await removeReplica(newMaster);
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
      // console.log(`heartbeat timeout: ${this.hostIp}`);
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
    // this.ip = object.ip;
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
        await this.reconnect();
        return;
      }
      await this.vote();
      this.client.end();
    } catch (error) {
      console.log('connection failed');
    }
    // reset unhealthyCount
    this.unhealthyCount = 0;
  }

  // async unhealthyCheck() {
  //   console.log(`unhealthy ${this.role}: ${this.hostIp}`);
  //   this.unhealthyCount++;
  //   await this.reconnect();
  // }

  async vote() {
    const { hostIp } = this;
    const vote = await voteInstance(hostIp, this.ip);
    // await redis.expire(`vote:${hostIp}`, 9);
    console.log(`vote: ${vote}`);
    if (vote !== +QUORUM) {
      this.unhealthyCount = 0;
      setTimeout(async () => {
        await this.reconnect();
      }, 3);
      return;
    }

    if (this.role === 'replica') {
      console.log('replica down! remove from list');
      await removeReplica(hostIp);
      return;
    }

    // One who get the vote is exactly equal to quorum can select the new master;
    console.log('I am selecting master');

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
