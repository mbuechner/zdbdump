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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.List;

/** Resolves the shared storage paths before the database or dump worker can write to them. */
public final class DumpPaths {

    private final Path outputFile;
    private final Path cacheDirectory;
    private final Path databaseFile;

    private DumpPaths(Path outputFile, Path cacheDirectory, Path databaseFile) {
        this.outputFile = outputFile;
        this.cacheDirectory = cacheDirectory;
        this.databaseFile = databaseFile;
    }

    /**
     * Prepares the directories and verifies that the public dump, cache files and database cannot alias each other.
     *
     * @param outputPath configured directory containing the published dump
     * @param tempPath configured directory containing the upstream archive and metadata
     * @param outputFilename name of the file to publish and serve
     * @param databaseName absolute database path or a path relative to the cache directory
     * @return the validated paths used by the worker and the download endpoints
     * @throws IOException if a storage directory or existing database path cannot be resolved
     * @throws IllegalArgumentException if a filename is invalid or the configured paths overlap
     */
    public static DumpPaths prepare(String outputPath, String tempPath, String outputFilename, String databaseName)
            throws IOException {
        final Path filename = configuredPath(outputFilename, "zdbdump.output.filename");
        if (filename.getNameCount() != 1 || !filename.equals(filename.getFileName()) || outputFilename.isBlank()
                || outputFilename.startsWith(".") || outputFilename.equals("createdump")
                || outputFilename.indexOf('/') >= 0 || outputFilename.indexOf('\\') >= 0
                || outputFilename.indexOf('%') >= 0 || outputFilename.indexOf(';') >= 0
                || outputFilename.codePoints().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("zdbdump.output.filename must be a valid download filename");
        }

        final Path cacheDirectory = resolveDirectory(configuredPath(tempPath, "zdbdump.path.temp"), "zdbdump.path.temp");
        final Path outputDirectory = resolveDirectory(configuredPath(outputPath, "zdbdump.path.output"), "zdbdump.path.output");
        final Path outputFile = outputDirectory.resolve(filename);
        final Path configuredDatabase = cacheDirectory.resolve(configuredPath(databaseName, "zdbdump.database")).normalize();
        if (configuredDatabase.getFileName() == null || Files.isDirectory(configuredDatabase)) {
            throw new IllegalArgumentException("The MVStore path must refer to a file: " + configuredDatabase);
        }
        final Path databaseDirectory = resolveDirectory(configuredDatabase.getParent(), "zdbdump.database");
        final Path databasePath = databaseDirectory.resolve(configuredDatabase.getFileName());
        // A database symlink, unlike a publication path, is followed when H2 opens it.
        final Path databaseFile = Files.isSymbolicLink(databasePath) ? databasePath.toRealPath() : databasePath;
        final List<Path> cacheFiles = List.of(cacheDirectory.resolve(BaseDumpCache.ARCHIVE_FILENAME),
                cacheDirectory.resolve(BaseDumpCache.METADATA_FILENAME));
        for (Path cacheFile : cacheFiles) {
            rejectCollision(outputFile, cacheFile, "The output dump conflicts with a cache file: ");
            rejectCollision(databaseFile, cacheFile, "The MVStore path conflicts with a dump file: ");
        }
        rejectCollision(databaseFile, outputFile, "The MVStore path conflicts with a dump file: ");

        createDirectory(cacheDirectory, "zdbdump.path.temp");
        createDirectory(outputDirectory, "zdbdump.path.output");
        createDirectory(databaseDirectory, "zdbdump.database");
        return new DumpPaths(outputFile, cacheDirectory, databaseFile);
    }

    /** Returns the path the worker replaces and the HTTP controller serves. */
    public Path outputFile() {
        return outputFile;
    }

    /** Returns the directory that holds the upstream archive and its metadata. */
    public Path cacheDirectory() {
        return cacheDirectory;
    }

    /** Returns the path to the MVStore database after any symbolic link has been resolved. */
    public Path databaseFile() {
        return databaseFile;
    }

    private static Path configuredPath(String value, String property) {
        try {
            return Path.of(value);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException("Invalid path configured by " + property, e);
        }
    }

    /** Resolves the existing part of a directory path without creating missing directories. */
    private static Path resolveDirectory(Path directory, String property) throws IOException {
        final Path absolute = directory.toAbsolutePath().normalize();
        try {
            Path ancestor = absolute;
            while (Files.notExists(ancestor, LinkOption.NOFOLLOW_LINKS)) {
                ancestor = ancestor.getParent();
                if (ancestor == null) {
                    throw new NoSuchFileException(absolute.toString());
                }
            }
            final Path real = ancestor.toRealPath();
            if (!Files.isDirectory(real)) {
                throw new NotDirectoryException(real.toString());
            }
            return real.resolve(ancestor.relativize(absolute));
        } catch (IOException | SecurityException e) {
            throw new IOException("Could not resolve directory " + directory + " configured by " + property, e);
        }
    }

    private static void createDirectory(Path directory, String property) throws IOException {
        try {
            Files.createDirectories(directory);
        } catch (IOException | SecurityException e) {
            throw new IOException("Could not prepare directory " + directory + " configured by " + property, e);
        }
    }

    /** Rejects files that share a path, require each other as a directory, or are existing aliases. */
    private static void rejectCollision(Path first, Path second, String message) throws IOException {
        if (first.startsWith(second) || second.startsWith(first)
                || (Files.exists(first) && Files.exists(second) && Files.isSameFile(first, second))) {
            throw new IllegalArgumentException(message + second);
        }
    }
}
