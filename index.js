require('dotenv').config();
const net = require('net');
const Turtlekeeper = require('./turtleKeeper');
const { redis } = require('./redis');
const {
  stringToHostAndPort,
  getMaster,
  getMasterConfig,
  getReplicasConfig,
  getReqHeader,
} = require('./util');

const {
  PORT, CHANNEL, REPLICA_KEY, PENDING_KEY,
} = process.env;

function createConnection(config, role) {
  const turtleKeeper = new Turtlekeeper(config, role);
  return turtleKeeper;
}

(async () => {
  const masterConfig = await getMasterConfig();
  if (masterConfig) {
    createConnection(masterConfig, 'master');
  }
  const replicasLength = await redis.scard(PENDING_KEY);
  const replicas = await redis.spop(PENDING_KEY, replicasLength);
  if (replicas.length) {
    const replicasState = [];
    replicas.forEach((replica) => replicasState.push(replica, 1));
    await redis.hmset(REPLICA_KEY, replicasState);
  }
  // Create existing replicas connections
  const replicasConfig = await getReplicasConfig();
  replicasConfig.forEach(async (replicaConfig) => {
    createConnection(replicaConfig, 'replica');
  });
})();

const subscriber = redis.duplicate();
subscriber.subscribe(CHANNEL, () => {
  console.log(`subscribe channel: ${CHANNEL}`);
});
subscriber.on('message', async (channel, message) => {
  const data = JSON.parse(message);
  if (data.method === 'join') {
    const master = await getMaster();
    const isMaster = (master === data.ip);

    // cluster mode
    if (data.role === 'replica') {
      if (isMaster) {
        return;
      }
      const isReplica = await redis.hget(REPLICA_KEY, data.ip);
      if (isReplica) {
        return;
      }
      await redis.srem(PENDING_KEY, data.ip);
      await redis.hset(REPLICA_KEY, data.ip, 1);
      const replicaConfig = stringToHostAndPort(data.ip);
      await createConnection(replicaConfig, 'replica');
      console.log(`New replica ${data.ip} has joined.`);
    } else if (data.role === 'master') {
      if (isMaster) {
        const masterConfig = stringToHostAndPort(data.ip);
        await createConnection(masterConfig, 'master');
      }
    }
  }
});

function createTurtleMQServer(requestHandler) {
  const server = net.createServer((connection) => {
    connection.on('error', () => {
      console.log('client disconnect forcefully');
    });

    connection.on('end', () => {
      console.log('client disconnect');
    });
  });

  function connectionHandler(socket) {
    socket.on('readable', () => {
      const reqHeader = getReqHeader(socket);

      if (!reqHeader) return;

      const body = JSON.parse(reqHeader);
      const request = {
        body,
        socket,
        send(data) {
          console.log(`\n${new Date().toISOString()} - Response: ${JSON.stringify(data)}`);
          const message = `${JSON.stringify(data)}\r\n\r\n`;
          socket.write(message);
        },
        end() {
          socket.end();
        },
      };

      // Send the request to the handler
      requestHandler(request);
    });

    socket.on('error', () => {
      console.log('socket error ');
    });

    socket.write('{ "message": "connected" }\r\n\r\n');
  }

  server.on('connection', connectionHandler);
  return server;
}

const webServer = createTurtleMQServer();

webServer.listen(PORT, () => {
  console.log(`server is listen on prot ${PORT}....`);
});
