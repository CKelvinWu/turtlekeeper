const net = require('net');
const Connection = require('./connection');
const { redis } = require('./redis');

const { QUORUM } = process.env;

const {
  removeReplica, setState, deleteMaster, getReqHeader,
  publishToChannel, setMaster, getReplicas, getRole,
} = require('./util');

function turtlekeeperErrorHandler() {
  return async function handler() {
    // handle server down
    this.connection?.clearHeartbeat();
    try {
      const ip = `${this.config.host}:${this.config.port}`;
      const role = await getRole(ip);
      if (!role) {
        this.connection.end();
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
        }, 9);
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
  };
}

class Turtlekeeper {
  constructor(config, role) {
    this.config = config;
    this.role = role;
    this.unhealthyCount = 0;
  }

  async reconnect() {
    setTimeout(async () => {
      await this.connect(this.config);
      // reconnect timeout
    }, 3000);
  }

  async connect() {
    return new Promise((resolve, reject) => {
      const client = net.connect(this.config);

      client.once('readable', async () => {
        const reqHeader = getReqHeader(client);
        if (!reqHeader) return;
        const object = JSON.parse(reqHeader);

        if (object.message === 'connected') {
          this.connection = new Connection(client);
          // await this.connection.init();
          resolve(this.connection);
        }
        reject();
      });

      client.on('error', turtlekeeperErrorHandler().bind(this));
      client.on('end', () => {
        // console.log('disconnected from server');
      });
    });
  }

  disconnect() {
    this.connection.end();
  }
}

module.exports = Turtlekeeper;
