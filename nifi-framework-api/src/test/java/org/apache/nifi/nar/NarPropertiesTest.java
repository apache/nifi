/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NarPropertiesTest {

    private static final String PROPERTIES_FILE_SUFFIX = ".properties";

    private static final String NIFI_GROUP_ID = "org.apache.nifi";
    private static final String NIFI_STANDARD_NAR_ID = "nifi-standard-nar";
    private static final String NIFI_STANDARD_NAR_VERSION = "2.0.0-SNAPSHOT";

    private static final String DEPENDENCY_GROUP_ID = "dependency-group";
    private static final String DEPENDENCY_ID = "dependency-id";
    private static final String DEPENDENCY_VERSION = "dependency-version";

    @Test
    public void testNarProperties(@TempDir final Path tempDir) throws IOException {
        final NarProperties narProperties = NarProperties.builder()
                .sourceType(NarSource.UPLOAD.name())
                .narGroup(NIFI_GROUP_ID)
                .narId(NIFI_STANDARD_NAR_ID)
                .narVersion(NIFI_STANDARD_NAR_VERSION)
                .installed(Instant.now())
                .build();

        final Path tempFile = createTempFile(tempDir);
        writeProperties(narProperties, tempFile);

        final NarProperties parsedProperties = readProperties(tempFile);
        assertEqual(narProperties, parsedProperties);
    }

    @Test
    public void testNarPropertiesWithDependencies(@TempDir final Path tempDir) throws IOException {
        final NarProperties narProperties = NarProperties.builder()
                .sourceType(NarSource.UPLOAD.name())
                .narGroup(NIFI_GROUP_ID)
                .narId(NIFI_STANDARD_NAR_ID)
                .narVersion(NIFI_STANDARD_NAR_VERSION)
                .narDependencyGroup(DEPENDENCY_GROUP_ID)
                .narDependencyId(DEPENDENCY_ID)
                .narDependencyVersion(DEPENDENCY_VERSION)
                .installed(Instant.now())
                .build();

        final Path tempFile = createTempFile(tempDir);
        writeProperties(narProperties, tempFile);

        final NarProperties parsedProperties = readProperties(tempFile);
        assertEqual(narProperties, parsedProperties);
    }

    @Test
    public void testInvalidDependencyValues() {
        assertThrows(IllegalArgumentException.class, () -> NarProperties.builder()
                .sourceType(NarSource.UPLOAD.name())
                .narGroup(NIFI_GROUP_ID)
                .narId(NIFI_STANDARD_NAR_ID)
                .narVersion(NIFI_STANDARD_NAR_VERSION)
                .narDependencyGroup(DEPENDENCY_GROUP_ID)
                .installed(Instant.now())
                .build());

        assertThrows(IllegalArgumentException.class, () -> NarProperties.builder()
                .sourceType(NarSource.UPLOAD.name())
                .narGroup(NIFI_GROUP_ID)
                .narId(NIFI_STANDARD_NAR_ID)
                .narVersion(NIFI_STANDARD_NAR_VERSION)
                .narDependencyGroup(DEPENDENCY_GROUP_ID)
                .narDependencyId(DEPENDENCY_ID)
                .installed(Instant.now())
                .build());

        assertThrows(IllegalArgumentException.class, () -> NarProperties.builder()
                .sourceType(NarSource.UPLOAD.name())
                .narGroup(NIFI_GROUP_ID)
                .narId(NIFI_STANDARD_NAR_ID)
                .narVersion(NIFI_STANDARD_NAR_VERSION)
                .narDependencyId(DEPENDENCY_ID)
                .installed(Instant.now())
                .build());

        assertThrows(IllegalArgumentException.class, () -> NarProperties.builder()
                .sourceType(NarSource.UPLOAD.name())
                .narGroup(NIFI_GROUP_ID)
                .narId(NIFI_STANDARD_NAR_ID)
                .narVersion(NIFI_STANDARD_NAR_VERSION)
                .narDependencyVersion(DEPENDENCY_VERSION)
                .installed(Instant.now())
                .build());

    }

    private void assertEqual(final NarProperties narProperties1, final NarProperties narProperties2) {
        assertEquals(narProperties1.getSourceType(), narProperties2.getSourceType());
        assertEquals(narProperties1.getSourceId(), narProperties2.getSourceId());

        assertEquals(narProperties1.getNarGroup(), narProperties2.getNarGroup());
        assertEquals(narProperties1.getNarId(), narProperties2.getNarId());
        assertEquals(narProperties1.getNarVersion(), narProperties2.getNarVersion());

        assertEquals(narProperties1.getNarDependencyGroup(), narProperties2.getNarDependencyGroup());
        assertEquals(narProperties1.getNarDependencyId(), narProperties2.getNarDependencyId());
        assertEquals(narProperties1.getNarDependencyVersion(), narProperties2.getNarDependencyVersion());

        assertEquals(narProperties1.getInstalled(), narProperties2.getInstalled());
    }

    private void writeProperties(final NarProperties narProperties, final Path file) throws IOException {
        try (final OutputStream outputStream = new FileOutputStream(file.toFile())) {
            final Properties properties = narProperties.toProperties();
            properties.store(outputStream, null);
        }
    }

    private NarProperties readProperties(final Path file) throws IOException {
        try (final InputStream inputStream = new FileInputStream(file.toFile())) {
            return NarProperties.parse(inputStream);
        }
    }

    private Path createTempFile(final Path tempDir) throws IOException {
        return Files.createTempFile(tempDir, NarProperties.class.getSimpleName(), PROPERTIES_FILE_SUFFIX);
    }
}
