FROM node:16.17-alpine3.15

ENV NODE_ENV="production" \
  HOST="localhost" \
  PORT=15566 \
  EXPIRE_TIMEOUT=15 \
  MASTER_KEY="turtlemq:master" \
  REPLICA_KEY="turtlemq:replicas" \
  PENDING_KEY="turtlemq:pending" \  
  CHANNEL="turtlemq:channel" \
  QUORUM=2 \
  HEARTRATE=3 \
  UNHEALTHY_COUNT=3 \
  REDIS_PORT=6379 \
  REDIS_HOST="localhost" \
  REDIS_USER=default \
  REDIS_PASSWORD=""

WORKDIR /turtlekeeper

COPY . .

RUN npm install

CMD [ "node", "index.js" ]
