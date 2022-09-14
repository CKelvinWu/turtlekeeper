const net = require('net');
const crypto = require('crypto');

const randomId = () => crypto.randomBytes(8).toString('hex');

const { QUORUM } = process.env;

const {
  removeReplica, setState, getReqHeader, publishToChannel,
  setMaster, getReplicas, getRole, getCurrentIp, voteInstance,
} = require('./util');

class Turtlekeeper {
  constructor(config, role) {
    this.config = config;
    this.hostIp = `${config?.host}:${config?.port}`;
    this.role = role;
    this.unhealthyCount = 0;
    this.heartrate = 3000;
    this.id = randomId();
    this.setIp();
  }

  async setIp() {
    this.ip = await getCurrentIp();
    this.connect();
  }

  async reconnect() {
    await this.connect(this.config);
  }

  async connect() {
    const client = net.connect(this.config);

    this.sendHeartbeat();
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
  }

  disconnect() {
    this.connection.end();
  }

  sendHeartbeat() {
    this.send({
      id: this.id,
      role: 'turtlekeeper',
      method: 'heartbeat',
      ip: this.ip,
    });
    this.heartbeatTimeout = setTimeout(() => {
      this.heartbeatError();
    }, this.heartrate);
    this.interval = setTimeout(() => {
      this.sendHeartbeat();
    }, this.heartrate);
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
        this.connection.end();
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
      this.connection.end();
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
    if (this.role === 'master') {
      // One who get the vote is exactly equal to quorum can select the new master;
      await setState('voting');
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
      await setState('active');
    } else if (this.role === 'replica') {
      await removeReplica(hostIp);
      console.log('replica down! remove from list');
    }
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
