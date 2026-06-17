FROM node:22-bookworm-slim

WORKDIR /workspace/dashboard

ENV HOST=0.0.0.0
ENV PORT=5173
ENV WA_CONSUMPTION_DIR=/workspace/outputs/consumption
ENV CHOKIDAR_USEPOLLING=true
ENV PNPM_HOME=/pnpm
ENV PATH=$PNPM_HOME:$PATH

RUN corepack enable && corepack prepare pnpm@11.5.3 --activate

COPY package.json pnpm-lock.yaml /opt/dashboard-seed/
RUN cd /opt/dashboard-seed && pnpm install --frozen-lockfile

COPY scripts/docker-dashboard.sh /usr/local/bin/docker-dashboard
RUN chmod +x /usr/local/bin/docker-dashboard

ENTRYPOINT ["docker-dashboard"]
CMD ["dev"]
