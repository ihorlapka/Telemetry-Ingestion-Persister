FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app
ARG GITHUB_ACTOR
ARG GITHUB_TOKEN
ENV GITHUB_ACTOR=$GITHUB_ACTOR
ENV GITHUB_TOKEN=$GITHUB_TOKEN
COPY pom.xml .
COPY src ./src
COPY /ci/settings.xml /root/.m2/settings.xml
RUN mvn -B -s /root/.m2/settings.xml -U -e -DskipTests package
