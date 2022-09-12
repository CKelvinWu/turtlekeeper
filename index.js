require('dotenv').config();
const net = require('net');

const { PORT } = process.env;

function getReqHeader(client) {
  let reqHeader;
  while (true) {
    let reqBuffer = Buffer.from('');
    const buf = client.read();
    if (buf === null) break;

    reqBuffer = Buffer.concat([reqBuffer, buf]);

    // Indicating end of a request
    const marker = reqBuffer.indexOf('\r\n\r\n');
    if (marker !== -1) {
      // Record the data after \r\n\r\n
      const remaining = reqBuffer.slice(marker + 4);
      reqHeader = reqBuffer.slice(0, marker).toString();
      // Push the extra readed data back to the socket's readable stream
      client.unshift(remaining);
      break;
    }
  }
  return reqHeader;
}

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
