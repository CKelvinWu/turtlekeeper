const net = require('net');

// const socket = new net.Socket();
const crypto = require('crypto');
const { redis } = require('./redis');

const randomId = () => crypto.randomBytes(8).toString('hex');

const {
  QUORUM, MASTER_KEY, REPLICA_KEY, PENDING_KEY,
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
  local MASTER_KEY = KEYS[4]
  local currentTime = tonumber(ARGV[1])
  local voteCountTime = tonumber(ARGV[2])
  local quorum = tonumber(ARGV[3])
  local isReplica = tonumber(ARGV[4])
  
  redis.call('HSET', hostIp, myIp, currentTime)
  local votes = redis.call('HVALS', hostIp)
  local vote = 0
  for i=1,#votes do
    if (currentTime - votes[i] < voteCountTime) then
      vote = vote + 1
    end
  end

  if (vote ~= quorum) then
    return { 0, 0, 0 }
  else
    redis.call('DEL', hostIp)
    if (isReplica == 1) then
      redis.call('HDEL', REPLICA_KEY, hostIp)
      return { 1, 0, 0 }
    else
      local replicas = redis.call('HKEYS', REPLICA_KEY)

      if (#replicas == 0) then
        redis.call('DEL', MASTER_KEY);
        return { 1, 0, 0 }
      else
        local rand = math.random(#replicas)
        local newMaster = replicas[rand]
        redis.call('HDEL', REPLICA_KEY, newMaster)
        redis.call('SET', MASTER_KEY, newMaster)
        return { 1, 1, newMaster } 
      end
    end
  end  
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
    this.socket.setKeepAlive(true, 5000);
    this.setIp();
  }

  async setIp() {
    this.ip = await getCurrentIp();
    this.connect();
  }

  async reconnect() {
    this.client = this.socket.connect(this.config);
    if (this.role === 'master') {
      const masterInfo = { method: 'setMaster', ip: this.hostIp };
      await publishToChannel(masterInfo);
    }
    this.sendHeartbeat();
  }

  async connect() {
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

      const newMaster = await redis.newMaster(2, MASTER_KEY, REPLICA_KEY);
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
      method: 'heartbeat',
      ip: this.ip,
    });
  }

  heartbeat(object) {
    this.role = object.role;
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
    const [canVote, hasReplica, newMasterIp] = await redis.canVote(
      4,
      hostIp,
      this.ip,
      REPLICA_KEY,
      MASTER_KEY,
      Date.now(),
      voteCountTime,
      QUORUM,
      +isReplica,
    );

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
