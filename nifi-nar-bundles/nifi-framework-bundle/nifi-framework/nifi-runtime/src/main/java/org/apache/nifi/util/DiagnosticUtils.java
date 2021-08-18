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
package org.apache.nifi.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

public final class DiagnosticUtils {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosticUtils.class);

    private DiagnosticUtils() {
        // utility class, not meant to be instantiated
    }

    public static Path getOldestFile(final String diagnosticDirectoryPath) throws IOException {
        Comparator<? super Path> lastModifiedComparator = Comparator.comparingLong(p -> p.toFile().lastModified());

        final Optional<Path> oldestFile;

        try (Stream<Path> paths = Files.walk(Paths.get(diagnosticDirectoryPath))) {
            oldestFile = paths
                    .filter(Files::isRegularFile)
                    .min(lastModifiedComparator);
        }

        return oldestFile.orElseThrow(
                () -> new RuntimeException(String.format("Could not find oldest file in diagnostic directory: %s", diagnosticDirectoryPath)));
    }

    public static boolean isFileCountExceeded(final String diagnosticDirectoryPath, final int maxFileCount) {
        final String[] fileNames = new File(diagnosticDirectoryPath).list();
        if (fileNames == null) {
            logger.error("The diagnostic directory path provided is either invalid or not permitted to be listed.");
            return false;
        }
        return fileNames.length >= maxFileCount;
    }

    public static boolean isSizeExceeded(final String diagnosticDirectoryPath, final long maxSizeInBytes) {
        return getDirectorySize(Paths.get(diagnosticDirectoryPath)) >= maxSizeInBytes;
    }


    public static boolean createDiagnosticDirectory(final String diagnosticDirectoryPath) {
        File file = new File(diagnosticDirectoryPath);
        return file.mkdir();
    }

    private static long getDirectorySize(Path path) {
        long size = 0;
        try (Stream<Path> walk = Files.walk(path)) {
            size = walk
                    .filter(Files::isRegularFile)
                    .mapToLong(getFileSizeByPathFunction())
                    .sum();

        } catch (IOException e) {
            logger.error("Directory [{}] size calculation failed", path, e);
        }
        return size;
    }

    private static ToLongFunction<Path> getFileSizeByPathFunction() {
        return path -> {
            try {
                return Files.size(path);
            } catch (IOException e) {
                logger.error("Failed to get size of file {}", path, e);
                return 0L;
            }
        };
    }
}
