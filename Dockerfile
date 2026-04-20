FROM maven:3-eclipse-temurin-25 AS build
COPY pom.xml /tmp/
COPY src /tmp/src/
WORKDIR /tmp/
RUN mvn package

FROM eclipse-temurin:25-jre-jammy
LABEL maintainer="Michael Büchner <m.buechner@dnb.de>"
ENV TZ=Europe/Berlin
ENV ZDBDUMP.PORT=8080
ENV XDG_CONFIG_HOME=/tmp
RUN mkdir /home/zdbdump && apk add curl
COPY --from=build /tmp/target/zdbdump.jar /home/zdbdump/zdbdump.jar
WORKDIR /home/zdbdump/
CMD ["java", "-Xms256M", "-Xmx512G", "-jar", "zdbdump.jar"]
EXPOSE 8080
