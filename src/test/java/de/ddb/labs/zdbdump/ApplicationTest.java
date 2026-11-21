package de.ddb.labs.zdbdump;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import de.ddb.labs.zdbdump.dump.ZdbDumpCreationCronJob;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.h2.mvstore.MVStoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Verifies configuration and file safety when opening the working store. */
class ApplicationTest {

    @TempDir
    Path directory;

    @Test
    void rejectsDumpAndCacheFilesAsDatabase() throws IOException {
        final var application = new Application();
        final String outputFilename = "dump.rdf.gz";
        for (String filename : new String[] {outputFilename, ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME,
                ZdbDumpCreationCronJob.BASE_DUMP_METADATA_FILENAME}) {
            final Path existing = directory.resolve(filename);
            Files.writeString(existing, "Existing dump data");

            final var failure = assertThrows(IllegalArgumentException.class,
                    () -> application.dumpPaths(directory.toString(), directory.toString(), outputFilename, filename));

            assertTrue(failure.getMessage().contains("conflicts with a dump file"));
            assertEquals("Existing dump data", Files.readString(existing));
        }
    }

    @Test
    void allowsMatchingNamesInSeparateDirectories() throws IOException {
        final Path output = directory.resolve("output");
        final String filename = "dump.rdf.gz";
        final var application = new Application();
        try (var store = application.mvStore(application.dumpPaths(directory.toString(), output.toString(), filename, filename))) {
            assertTrue(Files.isRegularFile(directory.resolve(filename)));
            assertTrue(Files.isDirectory(output));
        }
    }

    @Test
    void identifiesOutputDirectorySettingWhenDirectoryCannotBePrepared() throws IOException {
        final Path file = Files.createFile(directory.resolve("output"));

        final var failure = assertThrows(IOException.class,
                () -> new Application().dumpPaths(directory.toString(), file.toString(), "dump.rdf.gz", "data.db"));

        assertTrue(failure.getMessage().contains("zdbdump.path.output"));
        assertInstanceOf(IOException.class, failure.getCause());
    }

    @Test
    void identifiesLockedStoreWithoutRecreatingIt() throws IOException {
        final var application = new Application();
        final var paths = application.dumpPaths(directory.toString(), directory.toString(), "dump.rdf.gz", "data.db");
        try (var store = application.mvStore(paths)) {
            store.<String, String>openMap("zdbdump").put("record", "contents");
            store.commit();

            final var failure = assertThrows(IllegalStateException.class,
                    () -> application.mvStore(paths));

            assertTrue(failure.getMessage().contains("already locked"));
            assertInstanceOf(MVStoreException.class, failure.getCause());
            assertEquals("contents", store.openMap("zdbdump").get("record"));
        }
    }

    @Test
    void rejectsNestedStorageFilesBeforeCreatingAnyDirectories() {
        final Path output = directory.resolve("new-output");
        final Path cacheUnderOutputFile = output.resolve("dump.rdf.gz");
        final var application = new Application();
        final var outputCollision = assertThrows(IllegalArgumentException.class,
                () -> application.dumpPaths(cacheUnderOutputFile.toString(), output.toString(), "dump.rdf.gz", "store.db"));
        assertTrue(outputCollision.getMessage().contains("conflicts with a cache file"));
        assertFalse(Files.exists(output));

        final Path cache = directory.resolve("new-cache");
        final Path outputUnderDatabase = cache.resolve("store.db").resolve("dumps");
        final var databaseCollision = assertThrows(IllegalArgumentException.class,
                () -> application.dumpPaths(cache.toString(), outputUnderDatabase.toString(), "dump.rdf.gz", "store.db"));
        assertTrue(databaseCollision.getMessage().contains("MVStore path conflicts"));
        assertFalse(Files.exists(cache));
    }

    @Test
    void rejectsDirectoryAliasesBeforeAnyFilesAreCreated() throws IOException {
        assumeTrue(Files.getFileStore(directory).supportsFileAttributeView("posix"));
        final Path real = Files.createDirectory(directory.resolve("real"));
        final Path alias = Files.createSymbolicLink(directory.resolve("alias"), real);
        final var application = new Application();

        final var cacheCollision = assertThrows(IllegalArgumentException.class,
                () -> application.dumpPaths(real.toString(), alias.toString(),
                        ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME, "store.db"));
        assertTrue(cacheCollision.getMessage().contains("conflicts with a cache file"));
        final var databaseCollision = assertThrows(IllegalArgumentException.class,
                () -> application.dumpPaths(real.toString(), alias.toString(), "output.rdf.gz", "output.rdf.gz"));
        assertTrue(databaseCollision.getMessage().contains("MVStore path conflicts"));
        assertFalse(Files.exists(real.resolve("output.rdf.gz")));
        assertFalse(Files.exists(real.resolve(ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME)));

        final var paths = application.dumpPaths(real.toString(), alias.toString(), "output.rdf.gz", "store.db");
        assertEquals(real.toRealPath().resolve("output.rdf.gz"), paths.outputFile());
        assertEquals(real.toRealPath(), paths.cacheDirectory());
    }
}
