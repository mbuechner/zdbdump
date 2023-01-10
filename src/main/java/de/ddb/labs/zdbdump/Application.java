/*
 * Copyright 2023 Michael Büchner, Deutsche Digitale Bibliothek
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
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(exclude = { SecurityAutoConfiguration.class })
@EnableScheduling
@ConditionalOnProperty(name = "scheduler.enabled", matchIfMissing = true)
@EnableRetry
@EnableAsync
@Slf4j
public class Application {

    @Value("${zdbdump.path.temp}")
    private String tempPath;
    
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
    }

    @PreDestroy
    private void destroy() {
        log.info("Destroy callback triggered: Closing database...");
        try {
            mvStore.close();
            httpClient.dispatcher().cancelAll();
        } catch (Exception e) {
            log.error("Could not close connection to database. {}", e.getMessage());
        }
    }

    private void initMvStore() {
        if (mvStore == null) {
            mvStore = new MVStore.Builder()
                    .fileName(tempPath + databaseName)
                    .compress()
                    //.compressHigh()
                    .open();
        }
    }

    @Bean
    protected MVMap<String, String> mvStoreZdbData() {
        if (mvStoreZdbData == null) {
            initMvStore();
            mvStoreZdbData = mvStore.openMap("zdbdumpconfiguration");
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
                    .connectTimeout(0, TimeUnit.SECONDS)
                    .readTimeout(0, TimeUnit.SECONDS)
                    .dispatcher(dispatcher)
                    .build();
        }
        return httpClient;
    }
}
