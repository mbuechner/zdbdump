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
package de.ddb.labs.zdbdump.dump;

import de.ddb.labs.zdbdump.dump.oai.OaiHarvester;
import de.ddb.labs.zdbdump.dump.rdf.RdfDump;
import de.ddb.labs.zdbdump.dump.support.DumpFailure;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.xml.stream.XMLStreamException;
import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.retry.RetryException;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/** Coordinates the rebuild and publication of the public ZDB RDF snapshot. */
@Service
public class ZdbDumpCreationCronJob {

    /** Filename of the local copy of the upstream base archive. */
    public static final String BASE_DUMP_CACHE_FILENAME = BaseDumpCache.ARCHIVE_FILENAME;
    /** Sidecar holding the upstream archive URL, data state and validator used for cache reuse. */
    public static final String BASE_DUMP_METADATA_FILENAME = BaseDumpCache.METADATA_FILENAME;

    private static final Logger log = LoggerFactory.getLogger(ZdbDumpCreationCronJob.class);
    private static final int MAX_DUMP_ATTEMPTS = 5;
    private static final Duration DUMP_RETRY_DELAY = Duration.ofMinutes(10);
    private static final Duration HARVEST_WINDOW = Duration.ofMinutes(30);

    private final Path targetDumpPath;
    private final MVMap<String, String> records;
    private final RdfDump rdf;
    private final BaseDumpCache baseDump;
    private final OaiHarvester harvester;
    private final AtomicBoolean running = new AtomicBoolean();
    private final RetryTemplate dumpRetry = new RetryTemplate(RetryPolicy.builder()
            .predicate(DumpFailure::isTransientRequest)
            .maxRetries(MAX_DUMP_ATTEMPTS - 1)
            .delay(DUMP_RETRY_DELAY)
            .build());

    /**
     * Creates the worker used by the scheduler and the manual HTTP trigger.
     *
     * @param records on-disk working map keyed by RDF subject
     * @param restClient shared client for the upstream base dump and OAI repository
     * @param paths validated storage paths also used by the database and the download endpoints
     */
    public ZdbDumpCreationCronJob(
            MVMap<String, String> records,
            RestClient restClient,
            DumpPaths paths) {
        this.records = records;
        this.targetDumpPath = paths.outputFile();
        this.rdf = new RdfDump();
        this.baseDump = new BaseDumpCache(restClient, rdf, paths.cacheDirectory());
        this.harvester = new OaiHarvester(restClient, rdf, DUMP_RETRY_DELAY);
    }

    /**
     * Handles scheduled and manual requests. The guard stays held through retries to protect
     * the shared working map and XML factories. Overlapping calls return immediately. A failed
     * attempt leaves any previously published file available for downloads.
     */
    @Scheduled(cron = "${zdbdump.cron.job}")
    public void run() {
        if (!running.compareAndSet(false, true)) {
            log.info("Skipping dump request because generation is already active for {}", targetDumpPath);
            return;
        }

        try {
            runWithRetries();
        } finally {
            running.set(false);
        }
    }

    /** Rebuilds the working map, applies updates up to a fixed cutoff, and publishes the resulting dump. */
    void createDump() throws IOException, XMLStreamException, InterruptedException {
        harvester.awaitAvailability();
        final long startedAt = System.nanoTime();
        final Instant baseTimestamp = baseDump.load(records);
        final Instant harvestCutoff = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        final HarvestSummary harvest = applyUpdates(baseTimestamp, harvestCutoff);
        final long outputBytes = publishDump();

        log.info("Wrote {} datasets after applying {} updates from {} harvest windows in {}s. Output: {} ({} bytes)",
                records.sizeAsLong(), harvest.updates(), harvest.windows(),
                elapsedSeconds(startedAt), targetDumpPath, outputBytes);
    }

    /** Applies consecutive UTC windows through the cutoff captured after loading the base snapshot. */
    private HarvestSummary applyUpdates(Instant baseTimestamp, Instant harvestCutoff) throws IOException {
        log.info("Applying OAI updates from {} through {}", baseTimestamp, harvestCutoff);
        int windows = 0;
        long updates = 0;
        Instant from = baseTimestamp;
        while (!from.isAfter(harvestCutoff)) {
            // OAI-PMH bounds are inclusive. Advance by one second to cover each timestamp once.
            final Instant windowEnd = from.plus(HARVEST_WINDOW).minusSeconds(1);
            final Instant until = windowEnd.isBefore(harvestCutoff) ? windowEnd : harvestCutoff;
            updates += harvester.applyWindow(from, until, records);
            from = until.plusSeconds(1);
            if (++windows % 500 == 0) {
                log.info("Processed {} harvest windows and applied {} updates ...", windows, updates);
            }
        }
        return new HarvestSummary(windows, updates);
    }

    /** Completes the compressed file before atomically replacing the public path. */
    private long publishDump() throws IOException, XMLStreamException, InterruptedException {
        final Path directory = Files.createDirectories(targetDumpPath.getParent());
        // Stage beside the public file so publication can use a same-filesystem atomic move.
        final Path stagedDump = createPublicationStage(directory);
        try {
            log.info("Writing candidate dump for {} with {} records", targetDumpPath, records.sizeAsLong());
            rdf.write(stagedDump, records.values());
            DumpFailure.checkInterrupted();
            final long outputBytes = Files.size(stagedDump);
            Files.move(stagedDump, targetDumpPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            return outputBytes;
        } finally {
            DumpFailure.discardStage(stagedDump, log);
        }
    }

    /** Gives the staged file the permissions of a regular output file, subject to the process umask. */
    private static Path createPublicationStage(Path directory) throws IOException {
        if (Files.getFileStore(directory).supportsFileAttributeView(PosixFileAttributeView.class)) {
            return Files.createTempFile(directory, ".zdbdump-", ".tmp",
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-rw-rw-")));
        }
        return Files.createTempFile(directory, ".zdbdump-", ".tmp");
    }

    private void runWithRetries() {
        final long startedAt = System.nanoTime();
        final AtomicInteger attempts = new AtomicInteger();
        try {
            dumpRetry.execute(() -> {
                runAttempt(attempts.incrementAndGet());
                return null;
            });
        } catch (RetryException e) {
            logRunFailure(e, attempts.get(), elapsedSeconds(startedAt));
        }
    }

    private void runAttempt(int attempt) throws IOException, XMLStreamException, InterruptedException {
        log.info("Starting RDF dump attempt {}/{} for {}", attempt, MAX_DUMP_ATTEMPTS, targetDumpPath);
        try {
            createDump();
        } catch (Exception e) {
            if (attempt < MAX_DUMP_ATTEMPTS && DumpFailure.isTransientRequest(e)) {
                log.warn("Dump attempt {} failed. Retrying in {} seconds. {}",
                        attempt, DUMP_RETRY_DELAY.toSeconds(), DumpFailure.describe(e));
            }
            throw e;
        }
    }

    /** Restores the interrupt flag for cancellation and reserves error logs for terminal failures. */
    private void logRunFailure(RetryException retry, int attempts, long seconds) {
        if (Thread.currentThread().isInterrupted() || DumpFailure.hasCause(retry, InterruptedException.class)) {
            Thread.currentThread().interrupt();
            log.info("RDF dump generation interrupted after {} seconds before publishing {}", seconds, targetDumpPath);
            return;
        }

        final Throwable failure = retry.getLastException();
        if (DumpFailure.hasCause(failure, DumpFailure.HarvestDeferred.class)) {
            log.warn("RDF dump generation deferred before publishing {}. {}", targetDumpPath, DumpFailure.describe(failure));
            return;
        }

        log.error("RDF dump generation failed after {} attempt(s) in {} seconds. {} was not replaced. {}",
                attempts, seconds, targetDumpPath, DumpFailure.describe(failure));
        if (failure instanceof RuntimeException && !DumpFailure.hasCause(failure, IOException.class)
                && !DumpFailure.hasCause(failure, XMLStreamException.class)
                && !DumpFailure.hasCause(failure, RestClientException.class)) {
            log.debug("Unexpected failure during RDF dump generation", failure);
        }
    }

    private static long elapsedSeconds(long startedAt) {
        return Duration.ofNanos(System.nanoTime() - startedAt).toSeconds();
    }

    /** Counts processed time windows and applied records for the publication log. */
    private record HarvestSummary(int windows, long updates) {}
}
