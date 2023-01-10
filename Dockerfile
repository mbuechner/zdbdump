FROM maven:3-openjdk-17-slim AS MAVEN_CHAIN
COPY pom.xml /tmp/
COPY src /tmp/src/
WORKDIR /tmp/
RUN mvn package

FROM debian:bullseye-slim
ENV TZ=Europe/Berlin
ENV ZDBDUMP.PORT=8080
ENV XDG_CONFIG_HOME=/tmp
RUN apt-get -y update && apt-get -y install openjdk-17-jre && mkdir /home/zdbdump
COPY --from=MAVEN_CHAIN /tmp/target/zdbdump.jar /home/zdbdump/zdbdump.jar
WORKDIR /home/zdbdump/
CMD ["java", "-Xms512M", "-Xmx1G", "-Xss512k", "-XX:MaxDirectMemorySize=2G","-XX:+UseShenandoahGC", "-XX:+UnlockExperimentalVMOptions", "-XX:+ShenandoahUncommit", "-XX:ShenandoahGCHeuristics=compact", "-XX:ShenandoahUncommitDelay=1000", "-XX:ShenandoahGuaranteedGCInterval=10000", "-jar", "zdbdump.jar"]

EXPOSE 8080
