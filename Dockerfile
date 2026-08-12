FROM node:24-alpine

RUN addgroup -S queue && adduser -S queue -G queue
WORKDIR /app
COPY --chown=queue:queue package.json ./
COPY --chown=queue:queue src ./src
COPY --chown=queue:queue public ./public
COPY --chown=queue:queue bin ./bin
RUN mkdir /data && chown queue:queue /data

USER queue
ENV PORT=8080 DATA_DIR=/data NODE_ENV=production
EXPOSE 8080
VOLUME ["/data"]
CMD ["node", "src/server.js"]
