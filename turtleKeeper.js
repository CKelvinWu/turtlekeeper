const net = require('net');
const EventEmitter = require('node:events');
const crypto = require('crypto');

const { QUORUM, MASTER_KEY, REPLICA_KEY } = process.env;
const { redis } = require('./cache/cache');
const { publishToChannel, getRole, getCurrentIp } = require('./util');
const { getNewMasterScript, voteNewMasterScript } = require('./cache/scripts');

const myEmiter = new EventEmitter();
const randomId = () => crypto.randomBytes(8).toString('hex');
redis.defineCommand('getNewMaster', { lua: getNewMasterScript });
redis.defineCommand('voteNewMaster', { lua: voteNewMasterScript });

class Turtlekeeper {
  constructor(config, role) {
    this.config = config;
    this.hostIp = `${config?.host}:${config?.port}`;
    this.role = role;
    this.unhealthyCount = 0;
    this.heartrate = 3000;
    this.id = randomId();
    this.socket = new net.Socket();
    this.socket.setKeepAlive(true, 5000);
    this.setIp();
  }

  async setIp() {
    this.ip = await getCurrentIp();
    this.connect();
  }

  async reconnect() {
    this.client = this.socket.connect(this.config);
    this.sendHeartbeat();
  }

  async connect() {
    // FIXME:
    myEmiter.on('checkRole', async () => {
      console.log('checkRole---------------');
      setTimeout(async () => {
        this.role = await getRole(this.hostIp);
      }, 1000);
    });
    try {
      this.client = this.socket.connect(this.config);
      let reqBuffer = Buffer.from('');
      this.client.on('data', async (buf) => {
        if (!buf) return;
        reqBuffer = Buffer.concat([reqBuffer, buf]);

        while (true) {
          if (reqBuffer === null) break;
          // Indicating end of a request
          const marker = reqBuffer.indexOf('\r\n\r\n');
          // Find no seperator
          if (marker === -1) break;
          // Record the data after \r\n\r\n
          const reqHeader = reqBuffer.slice(0, marker).toString();
          // Keep hte extra readed data in the reqBuffer
          reqBuffer = reqBuffer.slice(marker + 4);

          const object = JSON.parse(reqHeader);
          if (object.success) {
            this[object.method](object);
          }
        }
      });

      this.client.on('error', () => {
        console.log('client on error');
      });
      this.client.on('end', () => {
        console.log('client end');
      });

      const newMaster = await redis.getNewMaster(2, MASTER_KEY, REPLICA_KEY);
      if (newMaster) {
        const masterInfo = { method: 'setMaster', ip: newMaster };
        this.role = 'master';
        // tell replica to become master
        await publishToChannel(masterInfo);
        console.log(`${newMaster} is the new master!`);
      }

      this.sendHeartbeat();
    } catch (error) {
      console.log(error);
    }
  }

  disconnect() {
    this.client.end();
  }

  sendHeartbeat() {
    // this.role = await getRole(this.hostIp);
    // If not getting response of heart beat, treat it as an connection error.
    this.heartbeatTimeout = setTimeout(() => {
      console.log('heartbeatError');
      this.heartbeatError();
    }, this.heartrate);

    // Sending next heratbeat
    this.interval = setTimeout(() => {
      this.sendHeartbeat();
    }, this.heartrate);
    this.send({
      id: this.id,
      role: 'turtlekeeper',
      setRole: this.role,
      method: 'heartbeat',
      ip: this.ip,
    });
  }

  heartbeat(object) {
    // this.role = object.role;
    this.clearHeartbeatTimeout();
    if (this.role === 'master') {
      console.log(`master ${object.ip} alive\n`);
    } else if (this.role === 'replica') {
      console.log(`replica ${object.ip} alive\n`);
    }
  }

  async heartbeatError() {
    try {
      this.clearHeartbeatTimeout();
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
        console.log(`unhealthy ${this.role}: ${this.hostIp}, unhealthCount: ${this.unhealthyCount}`);
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
    const isReplica = this.role === 'replica';
    const [canVote, hasReplica, newMasterIp] = await redis.voteNewMaster(
      4,
      hostIp,
      this.id,
      REPLICA_KEY,
      MASTER_KEY,
      Date.now(),
      voteCountTime,
      QUORUM,
      // TODO: check boolean
      +isReplica,
    );

    myEmiter.emit('checkRole');
    if (!canVote) {
      this.unhealthyCount = 0;
      this.reconnect();
      return;
    }

    if (isReplica) {
      console.log('replica down! remove from list');
      this.client.end();
      return;
    }

    // One who get the vote is exactly equal to quorum can select the new master;
    console.log('I am selecting the new master');

    // vote a replica from lists
    if (!hasReplica) {
      console.log('No turtleMQ is alive...');
      return;
    }
    const newMaster = { method: 'setMaster', ip: newMasterIp };

    // tell replica to become master
    await publishToChannel(newMaster);
    console.log(`${newMasterIp} is the new master!`);
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
