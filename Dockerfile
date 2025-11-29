FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app
ARG GITHUB_ACTOR
ARG GITHUB_TOKEN
COPY pom.xml .
COPY src ./src
RUN mvn -B -U -DskipTests package
