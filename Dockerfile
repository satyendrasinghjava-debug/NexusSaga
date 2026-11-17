# =============================================================================
# © 2026 Satyendra Singh. All Rights Reserved.
# PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
# =============================================================================

# ---------- Stage 1: build ----------
FROM maven:3.9-eclipse-temurin-21-alpine AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package \
    && cp target/nexussaga-engine-*.jar app.jar \
    && java -Djarmode=tools -jar app.jar extract --layers --launcher --destination extracted

# ---------- Stage 2: runtime ----------
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S nexus && adduser -S nexus -G nexus
WORKDIR /app
# Layered copy: dependencies change rarely, application code often.
COPY --from=build --chown=nexus:nexus /build/extracted/dependencies/ ./
COPY --from=build --chown=nexus:nexus /build/extracted/spring-boot-loader/ ./
COPY --from=build --chown=nexus:nexus /build/extracted/snapshot-dependencies/ ./
COPY --from=build --chown=nexus:nexus /build/extracted/application/ ./
USER nexus

# Hazelcast needs these module flags for full performance on Java 17+.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError \
 --add-modules java.se \
 --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
 --add-opens java.base/java.lang=ALL-UNNAMED \
 --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
 --add-opens java.management/sun.management=ALL-UNNAMED \
 --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED"

EXPOSE 8080 5701
HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=5 \
  CMD wget -qO- http://localhost:8080/actuator/health/liveness || exit 1
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS org.springframework.boot.loader.launch.JarLauncher"]
