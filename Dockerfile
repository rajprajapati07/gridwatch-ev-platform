# Multi-stage build: separate build and runtime layers
# Builder: Maven + JDK 21
# Runtime: JRE 21 slim (no Maven, no sources)

FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /build

COPY pom.xml .
# Download dependencies first (cached layer if pom.xml unchanged)
RUN --mount=type=cache,target=/root/.m2 \
    mvn dependency:go-offline -q

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 \
    mvn package -DskipTests -q

# ─── Runtime image ───────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Create non-root user
RUN addgroup -S gridwatch && adduser -S gridwatch -G gridwatch
USER gridwatch

COPY --from=builder /build/target/*.jar app.jar

# JVM tuning for container: respect cgroup CPU/memory limits
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "app.jar"]

EXPOSE 8080
