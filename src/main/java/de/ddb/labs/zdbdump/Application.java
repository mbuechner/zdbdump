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

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@ConditionalOnProperty(name = "scheduler.enabled", matchIfMissing = true)
@EnableRetry
@EnableAsync
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    @Value("${zdbdump.path.temp}")
    private String tempPath;

    @Value("${zdbdump.path.output}")
    private String outputPath;

    @Value("${zdbdump.database}")
    private String databaseName;

    private OkHttpClient httpClient; // http client

    private MVStore mvStore; // Key-Value-Store

    private MVMap<String, String> mvStoreZdbData;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    private void afterStartup() throws IOException {
        Files.createDirectories(Path.of(tempPath));
        Files.createDirectories(Path.of(outputPath));
    }

    @PreDestroy
    private void destroy() {
        log.info("Destroy callback triggered: Closing database ...");
        try {
            if (mvStore != null) {
                mvStore.close();
            }
            if (httpClient != null) {
                httpClient.dispatcher().cancelAll();
            }
        } catch (Exception e) {
            log.error("Could not close connection to database. {}", e.getMessage());
        }
    }

    private void initMvStore() {
        if (mvStore == null) {
            try {
                final Path tempDirectory = Path.of(tempPath);
                Files.createDirectories(tempDirectory);
                mvStore = new MVStore.Builder()
                        .fileName(tempDirectory.resolve(databaseName).toString())
                        .compress()
                        //.compressHigh()
                        .open();
            } catch (IOException e) {
                throw new IllegalStateException("Could not initialize temp directory for MVStore", e);
            }
        }
    }

    @Bean
    protected MVMap<String, String> mvStoreZdbData() {
        if (mvStoreZdbData == null) {
            initMvStore();
            mvStoreZdbData = mvStore.openMap("zdbdump");
        }
        return mvStoreZdbData;
    }

    @Bean
    protected OkHttpClient httpClient() {
        if (httpClient == null) {
            final Dispatcher dispatcher = new Dispatcher();
            dispatcher.setMaxRequests(64);
            dispatcher.setMaxRequestsPerHost(8);
            httpClient = new OkHttpClient.Builder()
                    .connectTimeout(60, TimeUnit.SECONDS)
                    .readTimeout(600, TimeUnit.SECONDS)
                    .callTimeout(0, TimeUnit.SECONDS)
                    .dispatcher(dispatcher)
                    .build();
        }
        return httpClient;
    }
}
