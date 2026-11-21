FROM maven:3-amazoncorretto-25 AS build
ARG SOURCE_DATE_EPOCH
WORKDIR /build
COPY pom.xml ./
COPY src ./src
# Use the release commit timestamp when the workflow supplies it.
RUN mvn -B -ntp ${SOURCE_DATE_EPOCH:+"-Dproject.build.outputTimestamp=$SOURCE_DATE_EPOCH"} clean verify

FROM amazoncorretto:25
LABEL maintainer="Michael Büchner <m.buechner@dnb.de>"
ENV TZ=Europe/Berlin
ENV HOME=/home/zdbdump
ENV ZDBDUMP_PORT=8080
ENV XDG_CONFIG_HOME=/tmp
WORKDIR /home/zdbdump
RUN mkdir -p data/tmp && chown -R 10001:10001 data
COPY --from=build /build/target/zdbdump.jar ./
COPY --from=build /build/target/classes/META-INF/sbom/application.cdx.json ./zdbdump-sbom.json
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 CMD curl --fail --silent --show-error --noproxy '*' --output /dev/null "http://127.0.0.1:${ZDBDUMP_PORT}${ZDBDUMP_PATH_PREFIX%/}/actuator/health"
CMD ["java", "-Xms256m", "-Xmx512m", "-jar", "zdbdump.jar"]
