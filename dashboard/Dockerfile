FROM node:20-bookworm-slim

WORKDIR /workspace/dashboard

ENV HOST=0.0.0.0
ENV PORT=5173
ENV WA_CONSUMPTION_DIR=/workspace/outputs/consumption
ENV CHOKIDAR_USEPOLLING=true

COPY package.json package-lock.json /opt/dashboard-seed/
RUN cd /opt/dashboard-seed && npm ci

COPY scripts/docker-dashboard.sh /usr/local/bin/docker-dashboard
RUN chmod +x /usr/local/bin/docker-dashboard

ENTRYPOINT ["docker-dashboard"]
CMD ["dev"]
