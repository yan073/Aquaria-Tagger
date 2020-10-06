FROM node:14.4
LABEL maintainer="bo.yan@csiro.au"

ADD crawler /app
WORKDIR /app

RUN npm install && npm run build
