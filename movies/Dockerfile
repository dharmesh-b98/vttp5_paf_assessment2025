FROM maven:3.9.9-eclipse-temurin-23 AS builder

LABEL MAINTAINER="Dharmesh"
LABEL description="PAF 2025 Paper"
LABEL name="PAF 2025 Paper"

WORKDIR /src

COPY pom.xml .
COPY mvnw .

COPY .mvn .mvn
COPY src src

# make mvnw executable
RUN mvn package -Dmaven.test.skip=true
# /src/target/revision-0.0.1-SNAPSHOT.jar

FROM maven:3.9.9-eclipse-temurin-23

WORKDIR /app

COPY --from=builder /src/target/movies-0.0.1-SNAPSHOT.jar app.jar
COPY data data

# check if curl command is available
#RUN apt update && apt install -y curl

ENV PORT=8080
ENV DATA_PATH=

EXPOSE ${PORT}

#HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
#   CMD curl http://localhost:${PORT}/health || exit 1

ENTRYPOINT SERVER_PORT=${PORT} java -jar app.jar