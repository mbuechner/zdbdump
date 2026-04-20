FROM maven:3-eclipse-temurin-25 AS build
COPY pom.xml /tmp/
COPY src /tmp/src/
WORKDIR /tmp/
RUN mvn package

FROM eclipse-temurin:25-jre-alpine
LABEL maintainer="Michael Büchner <m.buechner@dnb.de>"
ENV TZ=Europe/Berlin
ENV ZDBDUMP_PORT=8080
ENV XDG_CONFIG_HOME=/tmp
RUN mkdir -p /home/zdbdump /home/zdbdump/data /home/zdbdump/data/tmp && apk add --no-cache curl
COPY --from=build /tmp/target/zdbdump.jar /home/zdbdump/zdbdump.jar
WORKDIR /home/zdbdump/
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 CMD curl --fail --silent http://127.0.0.1:8080/actuator/health || exit 1
CMD ["java", "-Xms256m", "-Xmx512m", "-jar", "zdbdump.jar"]
EXPOSE 8080
