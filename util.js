require('dotenv').config();
// const http = require('http');

const { HOST, PORT } = process.env;

async function getCurrentIp() {
  return new Promise((resolve) => {
    resolve(`${HOST}:${PORT}`);
    // http.get({ host: 'api.ipify.org', port: 80, path: '/' }, (resp) => {
    //   resp.on('data', (ip) => {
    //     resolve(`${ip}:${PORT}`);
    //     console.log(`My public IP address is: ${ip}`);
    //   });
    // }).end();
  });
}

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

module.exports = {
  getCurrentIp,
  getReqHeader,
};
