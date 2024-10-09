/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.runtime.command;

import org.apache.nifi.NiFiServer;
import org.apache.nifi.diagnostics.DiagnosticsDump;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

/**
 * Diagnostics Command abstracted for invocation in Shutdown Command
 */
public class DiagnosticsCommand implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosticsCommand.class);

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");

    private static final String DIAGNOSTICS_FILE_FORMAT = "diagnostic-%s.log";

    private final NiFiProperties properties;

    private final NiFiServer server;

    public DiagnosticsCommand(final NiFiProperties properties, final NiFiServer server) {
        this.properties = Objects.requireNonNull(properties, "Properties required");
        this.server = Objects.requireNonNull(server, "Server required");
    }

    @Override
    public void run() {
        if (properties.isDiagnosticsOnShutdownEnabled()) {
            final File diagnosticDirectory = new File(properties.getDiagnosticsOnShutdownDirectory());
            if (diagnosticDirectory.mkdir()) {
                logger.info("Diagnostics Directory created [{}]", diagnosticDirectory);
            }
            purgeOldestFiles(diagnosticDirectory);

            final String localDateTime = DATE_TIME_FORMATTER.format(LocalDateTime.now());
            final String diagnosticsFileName = DIAGNOSTICS_FILE_FORMAT.formatted(localDateTime);
            final File diagnosticsPath = new File(diagnosticDirectory, diagnosticsFileName);
            writeDiagnostics(diagnosticsPath);
        }
    }

    private void purgeOldestFiles(final File diagnosticDirectory) {
        final long maxSize = DataUnit.parseDataSize(properties.getDiagnosticsOnShutdownDirectoryMaxSize(), DataUnit.B).longValue();
        final int maxFileCount = properties.getDiagnosticsOnShutdownMaxFileCount();
        while (isFileCountExceeded(diagnosticDirectory, maxFileCount) || isSizeExceeded(diagnosticDirectory, maxSize)) {
            try {
                final Path oldestFile = getOldestFile(diagnosticDirectory);
                Files.delete(oldestFile);
            } catch (final IOException e) {
                logger.warn("Delete oldest diagnostics failed", e);
            }
        }
    }

    private void writeDiagnostics(final File diagnosticsPath) {
        final DiagnosticsDump diagnosticsDump = server.getDiagnosticsFactory().create(properties.isDiagnosticsOnShutdownVerbose());
        try (final OutputStream fileOutputStream = new FileOutputStream(diagnosticsPath)) {
            diagnosticsDump.writeTo(fileOutputStream);
        } catch (final IOException e) {
            logger.warn("Write Diagnostics failed [{}]", diagnosticsPath, e);
        }
    }

    private Path getOldestFile(final File diagnosticDirectory) throws IOException {
        final Comparator<? super Path> lastModifiedComparator = Comparator.comparingLong(p -> p.toFile().lastModified());

        final Optional<Path> oldestFile;

        try (Stream<Path> paths = Files.walk(diagnosticDirectory.toPath())) {
            oldestFile = paths
                    .filter(Files::isRegularFile)
                    .min(lastModifiedComparator);
        }

        return oldestFile.orElseThrow(
                () -> new RuntimeException(String.format("Could not find oldest file in diagnostic directory: %s", diagnosticDirectory))
        );
    }

    private boolean isFileCountExceeded(final File diagnosticDirectory, final int maxFileCount) {
        final String[] fileNames = diagnosticDirectory.list();
        if (fileNames == null) {
            logger.warn("Diagnostics Directory [{}] listing files failed", diagnosticDirectory);
            return false;
        }
        return fileNames.length >= maxFileCount;
    }

    private boolean isSizeExceeded(final File diagnosticDirectory, final long maxSizeInBytes) {
        return getDirectorySize(diagnosticDirectory.toPath()) >= maxSizeInBytes;
    }

    private long getDirectorySize(Path path) {
        long size = 0;
        try (Stream<Path> walk = Files.walk(path)) {
            size = walk
                    .filter(Files::isRegularFile)
                    .mapToLong(getFileSizeByPathFunction())
                    .sum();

        } catch (IOException e) {
            logger.warn("Directory [{}] size calculation failed", path, e);
        }
        return size;
    }

    private ToLongFunction<Path> getFileSizeByPathFunction() {
        return path -> {
            try {
                return Files.size(path);
            } catch (IOException e) {
                logger.warn("Failed to get size of file {}", path, e);
                return 0L;
            }
        };
    }
}
