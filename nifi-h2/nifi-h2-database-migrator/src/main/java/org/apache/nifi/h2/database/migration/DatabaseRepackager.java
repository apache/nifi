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
package org.apache.nifi.h2.database.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * H2 Database Repackager rewrites org.h2 package references to renamed package references
 */
class DatabaseRepackager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseRepackager.class);

    private static final byte[] SOURCE_PACKAGE_NAME = "org.h2.mvstore".getBytes(StandardCharsets.UTF_8);

    private static final byte[] SOURCE_PACKAGE_ROOT_NAME = new byte[]{'o', 'r', 'g'};

    private static final String REPLACEMENT_PACKAGE_FORMAT = "%s.mvstore";

    private static final int END_OF_FILE = -1;

    /**
     * Repackage Database File replacing org.h2 package with package name from provided Database Version
     *
     * @param databaseVersion Source Database Version
     * @param sourceDatabaseFile Source Database File
     * @param repackagedDatabaseFile Repackaged Database File
     */
    static void repackageDatabaseFile(final DatabaseVersion databaseVersion, final Path sourceDatabaseFile, final Path repackagedDatabaseFile) {
        logger.debug("H2 DB {} repackaging started [{}]", databaseVersion.getVersion(), sourceDatabaseFile);

        final String replacementPackageName = String.format(REPLACEMENT_PACKAGE_FORMAT, databaseVersion.getPackageName());
        final byte[] replacementPackageBytes = replacementPackageName.getBytes(StandardCharsets.UTF_8);
        final ByteArrayOutputStream sourcePackageBuffer = new ByteArrayOutputStream();

        try (
                InputStream inputStream = new BufferedInputStream(Files.newInputStream(sourceDatabaseFile));
                OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(repackagedDatabaseFile))
        ) {
            int read = 0;
            boolean buffering = false;

            while (read != END_OF_FILE) {
                read = inputStream.read();
                if (buffering) {
                    if (sourcePackageBuffer.size() < SOURCE_PACKAGE_NAME.length) {
                        sourcePackageBuffer.write(read);
                    } else {
                        final byte[] packageBytes = sourcePackageBuffer.toByteArray();
                        sourcePackageBuffer.reset();
                        if (Arrays.equals(SOURCE_PACKAGE_NAME, packageBytes)) {
                            outputStream.write(replacementPackageBytes);
                        } else {
                            outputStream.write(packageBytes);
                        }

                        outputStream.write(read);
                        buffering = false;
                    }
                } else if (SOURCE_PACKAGE_ROOT_NAME[0] == read) {
                    read = inputStream.read();
                    if (SOURCE_PACKAGE_ROOT_NAME[1] == read) {
                        read = inputStream.read();
                        if (SOURCE_PACKAGE_ROOT_NAME[2] == read) {
                            buffering = true;
                            sourcePackageBuffer.write(SOURCE_PACKAGE_ROOT_NAME);
                        } else {
                            outputStream.write(SOURCE_PACKAGE_ROOT_NAME[0]);
                            outputStream.write(SOURCE_PACKAGE_ROOT_NAME[1]);
                            outputStream.write(read);
                        }
                    } else {
                        outputStream.write(SOURCE_PACKAGE_ROOT_NAME[0]);
                        outputStream.write(read);
                    }
                } else {
                    outputStream.write(read);
                }
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(String.format("H2 DB %s Repackaging Failed", databaseVersion.getVersion()), e);
        }
    }
}
