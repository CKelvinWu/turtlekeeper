const crypto = require('crypto');

const randomId = () => crypto.randomBytes(8).toString('hex');
const { getCurrentIp, getReqHeader } = require('./util');

module.exports = class Connection {
  constructor(client) {
    this.client = client;
    this.id = randomId();
    this.heartrate = 3000;

    this.client.on('readable', () => {
      const reqHeader = getReqHeader(client);
      if (!reqHeader) return;
      const object = JSON.parse(reqHeader);

      if (object.success) {
        this[object.method](object);
      }
    });
    this.init();
  }

  async init() {
    this.ip = await getCurrentIp();

    // start sending heartbeat
    this.interval = setInterval(() => {
      this.send({
        id: this.id,
        role: 'turtlekeeper',
        method: 'heartbeat',
        ip: this.ip,
      });
      // console.log('sending health check');
    }, this.heartrate);
  }

  heartbeat(object) {
    this.role = object.role;
    this.ip = object.ip;
    if (object.role === 'master') {
      // pingMaster();
      console.log(`master ${object.ip} alive\n`);
    } else if (object.role === 'replica') {
      // pingReplica(object.ip);
      console.log(`replica ${object.ip} alive\n`);
    }
  }

  clearHeartbeat() {
    clearInterval(this.interval);
  }

  send(messages) {
    this.client.write(`${JSON.stringify(messages)}\r\n\r\n`);
  }

  end() {
    this.client.end();
  }
  // initialize() {
  //   // request for master, replicas, and turtlekeepers
  //   this.send({
  //     id: this.id,
  //     role: 'turtlekeeper',
  //     method: 'init',
  //     ip: getCurrentIp(),
  //   });
  // }

  // initialize master, replicas, and turtlekeepers
  // async init(object) {
  //   const { group, role, ip } = object;
  //   this.role = role;
  //   this.ip = ip;
  //   this.master = group.master;
  //   this.replicas = group.replicas;
  //   this.turtlekeepers = group.turtlekeepers;

  //   if (this.role === 'replica') {
  //     await setReplica(ip);
  //   }
  //   console.log(`group: ${group}`);
  //   console.log(`master: ${this.master}`);
  //   console.log(`replicas: ${this.replicas}`);
  //   console.log(`turtlekeepers: ${this.turtlekeepers}\n`);
  // }
};
