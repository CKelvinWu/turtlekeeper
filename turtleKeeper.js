const net = require('net');
const crypto = require('crypto');

const randomId = () => crypto.randomBytes(8).toString('hex');
const { redis } = require('./redis');

const { QUORUM } = process.env;

const {
  removeReplica, setState, deleteMaster, getReqHeader,
  publishToChannel, setMaster, getReplicas, getRole, getCurrentIp,
} = require('./util');

class Turtlekeeper {
  constructor(config, role) {
    this.config = config;
    this.role = role;
    this.unhealthyCount = 0;
    this.heartrate = 3000;
    this.id = randomId();
    this.connect();
  }

  async reconnect() {
    await this.connect(this.config);
  }

  async connect() {
    const client = net.connect(this.config);

    this.startHeartbeat();
    client.on('readable', async () => {
      const reqHeader = getReqHeader(client);
      if (!reqHeader) return;
      const object = JSON.parse(reqHeader);

      if (object.message === 'connected') {
        // this.connection = new Connection(client);
        // await this.connection.init();
        // resolve(this.connection);
      }
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

  async startHeartbeat() {
    this.ip = await getCurrentIp();

    // start sending heartbeat
    this.sendHeartbeat();
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
    this.ip = object.ip;
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
      const ip = `${this.config.host}:${this.config.port}`;
      const role = await getRole(ip);
      if (!role) {
        this.connection.end();
        return;
      }
      this.role = role;

      // handle unstable connection
      if (this.unhealthyCount < 3) {
        console.log(`unhealthy ${this.role}: ${ip}`);
        this.unhealthyCount++;
        await this.reconnect();
        return;
      }

      const vote = await redis.incr(`vote:${ip}`);
      await redis.expire(`vote:${ip}`, 9);
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
        await deleteMaster();
        console.log('master down! start voting');

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
        await setState('active');
        console.log(`${newMaster.ip} is the new master!`);
        this.connection.end();
      } else if (this.connection.role === 'replica') {
        await removeReplica(this.connection.ip);
        console.log('replica down! remove from list');
        this.connection.end();
      }
    } catch (error) {
      console.log('connection failed');
    }

    // reset unhealthyCount
    this.unhealthyCount = 0;
  }

  clearHeartbeat() {
    clearTimeout(this.interval);
  }

  clearHeartbeatTimeout() {
    clearTimeout(this.heartbeatTimeout);
  }

  send(messages) {
    this.client.write(`${JSON.stringify(messages)}\r\n\r\n`);
  }

  end() {
    this.client.end();
  }
}

module.exports = Turtlekeeper;
