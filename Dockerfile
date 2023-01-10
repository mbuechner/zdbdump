FROM maven:3-openjdk-17-slim AS MAVEN_CHAIN
COPY pom.xml /tmp/
COPY src /tmp/src/
WORKDIR /tmp/
RUN mvn package

FROM openjdk:17-alpine
ENV TZ=Europe/Berlin
ENV ZDBDUMP.PORT=8080
ENV XDG_CONFIG_HOME=/tmp
RUN mkdir /home/zdbdump
COPY --from=MAVEN_CHAIN /tmp/target/zdbdump.jar /home/zdbdump/zdbdump.jar
WORKDIR /home/zdbdump/
CMD ["java", "-Xms256M", "-Xmx512G", "-jar", "zdbdump.jar"]
EXPOSE 8080
