require('dotenv').config();
const http = require('http');
const url = require('url');
const { redis } = require('./cache/cache');

const { MASTER_KEY } = process.env;
const HTTPPort = 3006;

async function getMaster() {
  const master = await redis.get(MASTER_KEY);
  return master;
}

const server = http.createServer(async (req, res) => {
  const reqUrl = url.parse(req.url).pathname;
  if (req.method === 'GET') {
    if (reqUrl === '/') {
      const master = await getMaster();
      res.writeHead(200);
      res.write(JSON.stringify({ master }));
      res.end();
    }
  }
});

server.listen(HTTPPort, () => {
  console.log(`HTTP server is listen on prot ${HTTPPort}....`);
});
