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
package de.ddb.labs.zdbdump.web;

import de.ddb.labs.zdbdump.dump.DumpPaths;
import de.ddb.labs.zdbdump.dump.ZdbDumpCreationCronJob;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.ContentDisposition;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.client.RestClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the public download routes and manual trigger over a local HTTP server. */
class ZdbDumpControllerTest {

    private static final String AUTHORIZATION = "Basic " + Base64.getEncoder()
            .encodeToString("test-admin:test-password".getBytes(StandardCharsets.UTF_8));

    @TempDir
    Path outputDirectory;

    @Test
    void publicRoutesOnlyServePublishedDumps() throws Exception {
        final String archive = "archive file.rdf.gz";
        final String privateFile = "private.txt";
        final String link = "linked.rdf.gz";
        final String stage = ".zdbdump-stage.rdf.gz";
        Files.writeString(outputDirectory.resolve(archive), "published RDF");
        Files.writeString(outputDirectory.resolve("current.gz"), "configured output");
        Files.writeString(outputDirectory.resolve(privateFile), "private");
        Files.writeString(outputDirectory.resolve(stage), "staging");
        Files.writeString(outputDirectory.resolve(ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME), "cache");
        if (Files.getFileStore(outputDirectory).supportsFileAttributeView("posix")) {
            Files.createSymbolicLink(outputDirectory.resolve(link), Path.of(privateFile));
        }

        try (var application = start(""); var client = HttpClient.newHttpClient()) {
            final URI root = rootUri(application);
            final var listing = request(client, "GET", root);
            assertEquals(200, listing.statusCode());
            assertTrue(listing.body().contains("https://example.test/catalog/dump/archive%20file.rdf.gz"));
            assertTrue(listing.body().contains("https://example.test/catalog/dump/current.gz"));
            assertFalse(listing.body().contains(privateFile));
            assertFalse(listing.body().contains(link));
            assertFalse(listing.body().contains(stage));
            assertFalse(listing.body().contains(ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME));
            assertEquals(404, request(client, "GET", root.resolve(privateFile)).statusCode());
            assertEquals(404, request(client, "GET", root.resolve(link)).statusCode());
            assertEquals(404, request(client, "GET", root.resolve(stage)).statusCode());
            assertEquals(404, request(client, "GET",
                    root.resolve(ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME)).statusCode());
            assertEquals(200, request(client, "GET", root.resolve("actuator/health")).statusCode());
            assertEquals(401, request(client, "POST", root.resolve("createdump"),
                    "Authorization", AUTHORIZATION, "X-ZDBDUMP-TRIGGER", "true").statusCode());

            final URI download = root.resolve("archive%20file.rdf.gz");
            final var response = request(client, "GET", download);
            assertEquals(200, response.statusCode());
            assertEquals("published RDF", response.body());
            assertEquals(archive, ContentDisposition.parse(response.headers()
                    .firstValue("Content-Disposition").orElseThrow()).getFilename());
            final var head = request(client, "HEAD", download);
            assertEquals(200, head.statusCode());
            assertEquals("", head.body());
            assertEquals(response.headers().firstValue("Content-Length"), head.headers().firstValue("Content-Length"));
            assertEquals(304, request(client, "GET", download, "If-Modified-Since",
                    response.headers().firstValue("Last-Modified").orElseThrow()).statusCode());
        }
    }

    @Test
    void reportsUnavailableListingWithoutExposingItsPath() throws Exception {
        final Path unavailableDirectory = outputDirectory.resolve("unavailable-output");
        try (var application = start("", unavailableDirectory); var client = HttpClient.newHttpClient()) {
            Files.delete(unavailableDirectory);
            final URI root = rootUri(application);
            final var listing = request(client, "GET", root);
            assertEquals(500, listing.statusCode());
            assertTrue(listing.body().contains("Can't read the dump directory."));
            assertFalse(listing.body().contains(unavailableDirectory.getFileName().toString()));
            assertEquals(404, request(client, "GET", root.resolve("current.gz")).statusCode());
        }
    }

    @Test
    void manualTriggerRequiresPostCredentialsAndRequestHeader() throws Exception {
        try (var application = start("test-password"); var client = HttpClient.newHttpClient()) {
            final URI trigger = rootUri(application).resolve("createdump");
            final var get = request(client, "GET", trigger, "Authorization", AUTHORIZATION);
            assertEquals(405, get.statusCode());
            assertEquals("POST", get.headers().firstValue("Allow").orElseThrow());
            assertEquals(405, request(client, "HEAD", trigger, "Authorization", AUTHORIZATION).statusCode());
            assertEquals(403, request(client, "POST", trigger, "Authorization", AUTHORIZATION).statusCode());
            assertEquals(401, request(client, "POST", trigger, "X-ZDBDUMP-TRIGGER", "true").statusCode());
            final var preflight = request(client, "OPTIONS", trigger, "Origin", "https://other-origin.test",
                    "Access-Control-Request-Method", "POST", "Access-Control-Request-Headers", "X-ZDBDUMP-TRIGGER");
            assertTrue(preflight.headers().firstValue("Access-Control-Allow-Origin").isEmpty());
            final var run = application.getBean(CountDownLatch.class);
            assertEquals(1, run.getCount());

            assertEquals(202, request(client, "POST", trigger, "Authorization", AUTHORIZATION,
                    "X-ZDBDUMP-TRIGGER", "true").statusCode());
            assertTrue(run.await(5, TimeUnit.SECONDS));
        }
    }

    private ConfigurableApplicationContext start(String password) {
        return start(password, outputDirectory);
    }

    /** Starts the HTTP fixture on a free loopback port with a designated dump directory. */
    private ConfigurableApplicationContext start(String password, Path dumpDirectory) {
        return new SpringApplicationBuilder(HttpTestApplication.class).run(
                "--server.address=127.0.0.1",
                "--server.port=0",
                "--server.servlet.context-path=/dump",
                "--spring.security.user.name=test-admin",
                "--spring.security.user.password=" + password,
                "--debug=false",
                "--spring.main.banner-mode=off",
                "--zdbdump.baseurl=https://example.test/catalog/",
                "--zdbdump.path.output=" + dumpDirectory,
                "--zdbdump.output.filename=current.gz");
    }

    private static URI rootUri(ConfigurableApplicationContext context) {
        final int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        return URI.create("http://127.0.0.1:" + port + "/dump/");
    }

    private static HttpResponse<String> request(HttpClient client, String method, URI uri, String... headers)
            throws IOException, InterruptedException {
        final var request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(10))
                .method(method, HttpRequest.BodyPublishers.noBody());
        if (headers.length > 0) {
            request.headers(headers);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    /** Loads the web layer and uses a latch to observe manual dispatch without generating a dump. */
    @EnableAutoConfiguration
    @EnableAsync
    @Import({BasicConfiguration.class, ZdbDumpController.class})
    static class HttpTestApplication {

        @Bean
        MVStore mvStore() {
            return new MVStore.Builder().open();
        }

        @Bean
        CountDownLatch run() {
            return new CountDownLatch(1);
        }

        @Bean
        DumpPaths dumpPaths(
                @Value("${zdbdump.path.output}") String outputPath,
                @Value("${zdbdump.output.filename}") String outputFilename) throws IOException {
            return DumpPaths.prepare(outputPath, outputPath, outputFilename, "test.mv.db");
        }

        @Bean
        ZdbDumpCreationCronJob dumpCreation(MVStore store, CountDownLatch run, DumpPaths paths) {
            return new ZdbDumpCreationCronJob(store.openMap("test"), RestClient.create(), paths) {
                /** Signals that the HTTP trigger dispatched the job. */
                @Override
                public void run() {
                    run.countDown();
                }
            };
        }
    }
}
