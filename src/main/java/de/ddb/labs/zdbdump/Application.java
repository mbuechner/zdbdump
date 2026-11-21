/*
 * Copyright 2023-2026 Michael Büchner, Deutsche Digitale Bibliothek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.ddb.labs.zdbdump;

import de.ddb.labs.zdbdump.dump.DumpPaths;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestClient;

/**
 * Starts the Spring Boot service and configures the storage and upstream HTTP client used by dump generation.
 * Spring closes the store and the shared HTTP client when the service shuts down.
 */
@SpringBootApplication
@EnableAsync
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    /** Creates the service configuration used by Spring Boot during startup. */
    public Application() {
    }

    /**
     * Launches the service and passes its command-line options to Spring Boot.
     *
     * @param args Spring Boot application arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    /** Resolves storage paths once for both startup validation and runtime access. */
    @Bean
    DumpPaths dumpPaths(@Value("${zdbdump.path.temp}") String tempPath,
            @Value("${zdbdump.path.output}") String outputPath,
            @Value("${zdbdump.output.filename}") String outputFilename,
            @Value("${zdbdump.database}") String databaseName) throws IOException {
        return DumpPaths.prepare(outputPath, tempPath, outputFilename, databaseName);
    }

    /** Opens the on-disk working store after the dump paths have been checked for collisions. */
    @Bean(destroyMethod = "close")
    MVStore mvStore(DumpPaths paths) {
        final Path databasePath = paths.databaseFile();
        try {
            final MVStore store = new MVStore.Builder()
                    .fileName(databasePath.toString())
                    .compress()
                    .open();
            log.info("Opened MVStore database at {}", databasePath);
            return store;
        } catch (MVStoreException e) {
            final String detail = switch (e.getErrorCode()) {
                case DataUtils.ERROR_FILE_LOCKED -> " The database file is already locked.";
                case DataUtils.ERROR_FILE_CORRUPT -> " MVStore reported a corrupt database file.";
                case DataUtils.ERROR_UNSUPPORTED_FORMAT -> " MVStore reported an unsupported database format.";
                default -> " MVStore error code " + e.getErrorCode() + ".";
            };
            throw new IllegalStateException("Could not open MVStore database at " + databasePath + "." + detail, e);
        }
    }

    /** Exposes the named map where dump generation merges RDF records by subject. */
    @Bean
    MVMap<String, String> mvStoreZdbData(MVStore mvStore) {
        try {
            return mvStore.openMap("zdbdump");
        } catch (MVStoreException e) {
            throw new IllegalStateException("Could not open the ZDB data map in MVStore", e);
        }
    }

    /** Shares one connection pool between upstream requests and closes it at shutdown. */
    @Bean(destroyMethod = "close")
    HttpClient httpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(60))
                .build();
    }

    /** Uses the shared client with a read timeout that allows large upstream dump responses. */
    @Bean
    RestClient restClient(HttpClient httpClient) {
        final JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(Duration.ofMinutes(10));

        return RestClient.builder()
                .requestFactory(requestFactory)
                .build();
    }

    /** Controls scheduled runs independently of the manual HTTP trigger. */
    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    @ConditionalOnProperty(name = "scheduler.enabled", havingValue = "true", matchIfMissing = true)
    static class SchedulingConfiguration {
    }
}
