const net = require('net');
const crypto = require('crypto');

const { QUORUM, MASTER_KEY, REPLICA_KEY } = process.env;
const { redis } = require('./cache/cache');
const { publishToChannel, getRole } = require('./util');
const { getNewMasterScript, voteNewMasterScript } = require('./cache/scripts');
const { turtlePool } = require('./turtlePool');

const randomId = () => crypto.randomBytes(8).toString('hex');
redis.defineCommand('getNewMaster', { lua: getNewMasterScript });
redis.defineCommand('voteNewMaster', { lua: voteNewMasterScript });

class Turtlekeeper {
  constructor(config, role) {
    this.config = config;
    this.hostIp = `${config?.host}:${config?.port}`;
    this.role = role || 'replica';
    this.unhealthyCount = 0;
    this.heartrate = 3000;
    this.id = randomId();
    this.socket = new net.Socket();
    this.socket.setKeepAlive(true, 5000);
    this.connect();
  }

  async reconnect() {
    this.role = await getRole(this.hostIp);
    this.client = this.socket.connect(this.config);
    this.sendHeartbeat();
  }

  async checkRole() {
    this.role = await getRole(this.hostIp);
    if (!this.role) {
      this.disconnect();
    }
  }

  async connect() {
    try {
      console.log(this.config);
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
          try {
            const object = JSON.parse(reqHeader);
            if (object.success) {
              this[object.method](object);
            }
          } catch (error) {
            console.log(error);
          }
        }
      });

      this.client.on('error', () => {
        // console.log('client on error');
      });
      this.client.on('end', () => {
        console.log('client end');
      });
      if (this.role === 'replica') {
        const newMaster = await redis.getNewMaster(2, MASTER_KEY, REPLICA_KEY);
        if (newMaster) {
          const masterInfo = { method: 'setMaster', ip: newMaster };
          this.role = 'master';
          // tell replica to become master
          await publishToChannel(masterInfo);
          console.log(`${newMaster} is the new master! start`);
        }
      }

      this.sendHeartbeat();
    } catch (error) {
      console.log(error);
    }
  }

  disconnect() {
    delete turtlePool[this.hostIp];
    this.clearHeartbeatTimeout();
    this.clearHeartbeat();
    this.client.removeAllListeners();
    this.client.end();
    console.log(`${this.hostIp} disconnect!!`);
  }

  sendHeartbeat() {
    // this.role = await getRole(this.hostIp);
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
      setRole: this.role,
      method: 'heartbeat',
      // ip: this.ip,
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
        this.disconnect();
        return;
      }
      this.role = role;

      // handle unstable connection
      if (this.unhealthyCount < 3) {
        console.log(`unhealthy ${this.role}: ${hostIp}, unhealthCount: ${this.unhealthyCount}`);
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

    if (!canVote) {
      this.unhealthyCount = 0;
      this.reconnect();
      return;
    }

    if (isReplica) {
      console.log('replica down! remove from list');
      this.disconnect();
      return;
    }

    // One who get the vote is exactly equal to quorum can select the new master;
    console.log('I am selecting the new master');

    // vote a replica from lists
    if (!hasReplica) {
      console.log('No turtleMQ is alive...');
      const newMaster = { method: 'setMaster', deadIp: this.hostIp };
      await publishToChannel(newMaster);
      this.disconnect();
      return;
    }
    const newMaster = { method: 'setMaster', ip: newMasterIp, deadIp: this.hostIp };

    // tell replica to become master
    await publishToChannel(newMaster);
    console.log(`Publish: ${newMasterIp} is the new master!`);
    this.disconnect();
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
}

module.exports = Turtlekeeper;
