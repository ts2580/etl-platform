# syntax=docker/dockerfile:1.7

FROM node:20-alpine AS tailwind
WORKDIR /app

COPY etl-main-app/package.json etl-main-app/package-lock.json ./etl-main-app/
COPY etl-main-app/src/main/resources/static/css/tailwind-input.css /app/etl-main-app/src/main/resources/static/css/tailwind-input.css
COPY etl-main-app/tailwind.config.js /app/etl-main-app/tailwind.config.js
COPY etl-main-app/src/main/resources/static /app/etl-main-app/src/main/resources/static
COPY etl-main-app/src/main/resources/templates /app/etl-main-app/src/main/resources/templates
COPY etl-file-engine/src/main/resources/static /app/etl-file-engine/src/main/resources/static

RUN cd /app/etl-main-app && npm ci --no-audit --no-fund --silent && npm run build:css

FROM eclipse-temurin:21-jdk AS builder
WORKDIR /app

COPY gradlew gradlew.bat settings.gradle build.gradle gradle.properties ./
COPY gradle ./gradle
COPY etl-common ./etl-common
COPY etl-domain ./etl-domain
COPY etl-api-contract ./etl-api-contract
COPY etl-infra ./etl-infra
COPY etl-main-app ./etl-main-app
COPY etl-routing-engine ./etl-routing-engine
COPY etl-file-engine ./etl-file-engine

# Inject freshly built CSS before Java compile so resources are always deterministic in CI
COPY --from=tailwind /app/etl-main-app/src/main/resources/static/css/tailwind.css /app/etl-main-app/src/main/resources/static/css/tailwind.css
COPY --from=tailwind /app/etl-main-app/src/main/resources/static/css/tailwind.css /app/etl-file-engine/src/main/resources/static/css/tailwind.css

RUN chmod +x ./gradlew \
    && ./gradlew --no-daemon clean :etl-main-app:bootJar :etl-routing-engine:bootJar :etl-file-engine:bootJar

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

ENV APP_HOME=/app \
    MAIN_APP_PORT=8080 \
    ROUTING_APP_PORT=3931 \
    FILE_APP_PORT=9443 \
    APP_DB_ENABLED=false \
    ROUTING_ENGINE_BASE_URL=http://localhost:3931

COPY --from=builder /app/etl-main-app/build/libs/etl-main-app.jar /app/etl-main-app.jar
COPY --from=builder /app/etl-routing-engine/build/libs/etl-routing-engine.jar /app/etl-routing-engine.jar
COPY --from=builder /app/etl-file-engine/build/libs/etl-file-engine.jar /app/etl-file-engine.jar
COPY docker/entrypoint.sh /app/docker/entrypoint.sh

RUN chmod +x /app/docker/entrypoint.sh

EXPOSE 8080 3931 9443

ENTRYPOINT ["/app/docker/entrypoint.sh"]
