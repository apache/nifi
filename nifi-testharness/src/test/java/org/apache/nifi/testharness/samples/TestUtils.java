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



package org.apache.nifi.testharness.samples;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

final class TestUtils {

    private TestUtils() {
        // no instances allowed
    }

    static File getBinaryDistributionZipFile(File binaryDistributionZipDir) {

        AtomicReference<File> binaryDistributionZipFileRef = new AtomicReference<>();

        List<Path> visitedFiles = new LinkedList<>();

        try {
            Files.walkFileTree(binaryDistributionZipDir.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    visitedFiles.add(file);

                    String name = file.toFile().getName();

                    if (name.startsWith("nifi-") && name.endsWith("-bin.zip")) {
                        binaryDistributionZipFileRef.set(file.toFile());
                    }

                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ioEx) {
            throw new RuntimeException(ioEx);
        }

        File binaryDistributionZipFile = binaryDistributionZipFileRef.get();
        if (binaryDistributionZipFile == null) {

            String visitedFilesString =
                    visitedFiles.stream().map(Path::toString).collect(Collectors.joining(", "));

            throw new IllegalStateException(
                    "No NiFi distribution ZIP file is found in: "
                            + binaryDistributionZipDir + ", while visited: "
                            + visitedFilesString);
        }

        return binaryDistributionZipFile;
    }
}
