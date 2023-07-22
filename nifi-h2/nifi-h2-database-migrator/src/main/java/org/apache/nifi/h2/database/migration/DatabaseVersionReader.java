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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * H2 Database Version Reader
 */
class DatabaseVersionReader {

    private static final int KEY_VALUE_SEPARATOR = ',';

    private static final byte[] FORMAT_PREFIX = "format:".getBytes(StandardCharsets.UTF_8);

    private static final int FORMAT_PREFIX_LENGTH = FORMAT_PREFIX.length;

    private static final int DECIMAL_RADIX = 10;

    private static final int END_OF_FILE = -1;

    /**
     * Read Database Version from H2 Database File Path using format:version header indicator
     *
     * @param filePath H2 Database File Path
     * @return H2 Database Version
     */
    static DatabaseVersion readDatabaseVersion(final Path filePath) {
        DatabaseVersion databaseVersion;

        if (Files.isReadable(filePath)) {
            try (InputStream inputStream = Files.newInputStream(filePath)) {
                databaseVersion = findDatabaseVersion(inputStream);
            } catch (final Exception e) {
                databaseVersion = DatabaseVersion.UNKNOWN;
            }
        } else {
            databaseVersion = DatabaseVersion.UNKNOWN;
        }

        return databaseVersion;
    }

    private static DatabaseVersion findDatabaseVersion(final InputStream inputStream) throws IOException {
        DatabaseVersion databaseVersion = DatabaseVersion.UNKNOWN;

        final ByteBuffer formatPrefixBuffer = ByteBuffer.allocate(FORMAT_PREFIX_LENGTH);
        boolean formatStartFound = false;
        int read = 0;

        while (read != END_OF_FILE) {
            read = inputStream.read();
            if (formatStartFound) {
                if (formatPrefixBuffer.hasRemaining()) {
                    formatPrefixBuffer.put((byte) read);
                } else {
                    final byte[] formatPrefixBytes = new byte[FORMAT_PREFIX_LENGTH];
                    formatPrefixBuffer.flip();
                    formatPrefixBuffer.get(formatPrefixBytes);

                    if (Arrays.equals(FORMAT_PREFIX, formatPrefixBytes)) {
                        final int formatVersion = Character.digit(read, DECIMAL_RADIX);
                        databaseVersion = getDatabaseVersion(formatVersion);
                        break;
                    }

                    formatStartFound = false;
                }
            } else if (KEY_VALUE_SEPARATOR == read) {
                formatPrefixBuffer.clear();
                formatStartFound = true;
            }
        }

        return databaseVersion;
    }

    private static DatabaseVersion getDatabaseVersion(final int formatVersion) {
        DatabaseVersion databaseVersion = DatabaseVersion.UNKNOWN;

        for (final DatabaseVersion currentDatabaseVersion : DatabaseVersion.values()) {
            if (currentDatabaseVersion.getFormatVersion() == formatVersion) {
                databaseVersion = currentDatabaseVersion;
                break;
            }
        }

        return databaseVersion;
    }
}
