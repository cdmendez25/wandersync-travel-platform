# Stage 1: build the static site
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
ARG VITE_GRAPHQL_URL=http://localhost:8080/graphql
ENV VITE_GRAPHQL_URL=$VITE_GRAPHQL_URL
RUN npm run build

# Stage 2: serve it with nginx as a non-root user
FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
