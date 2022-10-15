FROM node:16.17-alpine3.15

ENV NODE_ENV="production" \
  MASTER_KEY="turtlemq:master" \
  PORT=25566 \
  REDIS_PORT=6379 \
  REDIS_HOST="localhost" \
  REDIS_USER="default" \
  REDIS_PASSWORD=

WORKDIR /turtleFinder

COPY . .

RUN npm install

EXPOSE ${PORT}

CMD [ "node", "turtleFinder.js" ]
