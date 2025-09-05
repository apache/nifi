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

import org.apache.nifi.bundle.BundleCoordinate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardNarPersistenceProviderTest {

    @TempDir
    private Path tempDir;

    private NarPersistenceProvider persistenceProvider;

    @BeforeEach
    public void setup() throws IOException {
        persistenceProvider = initializeProvider(tempDir);
    }

    private NarPersistenceProvider initializeProvider(final Path tempDir) throws IOException {
        final Map<String, String> properties = Map.of("directory", tempDir.toFile().getAbsolutePath());
        final NarPersistenceProviderInitializationContext initializationContext = new StandardNarPersistenceProviderInitializationContext(properties);
        final NarPersistenceProvider persistenceProvider = new StandardNarPersistenceProvider();
        persistenceProvider.initialize(initializationContext);
        return persistenceProvider;
    }

    @Test
    public void testCrudOperations() throws IOException {
        final File tempFile;
        final String narContents = "Fake NAR Contents";
        try (final InputStream inputStream = new ByteArrayInputStream(narContents.getBytes(StandardCharsets.UTF_8))) {
            tempFile = persistenceProvider.createTempFile(inputStream);
        }

        final NarManifest narManifest = createNarManifest();
        final NarPersistenceContext persistenceContext = createNarPersistenceContext(narManifest);

        final NarPersistenceInfo persistenceInfo = persistenceProvider.saveNar(persistenceContext, tempFile);
        assertNotNull(persistenceInfo);

        final File narFile = persistenceInfo.getNarFile();
        assertNotNull(narFile);
        assertTrue(narFile.exists());
        assertEquals(narContents, readFile(narFile));

        final NarProperties narProperties = persistenceInfo.getNarProperties();
        assertEquals(narManifest.getGroup(), narProperties.getNarGroup());
        assertEquals(narManifest.getId(), narProperties.getNarId());
        assertEquals(narManifest.getVersion(), narProperties.getNarVersion());
        assertEquals(narManifest.getDependencyGroup(), narProperties.getNarDependencyGroup());
        assertEquals(narManifest.getDependencyId(), narProperties.getNarDependencyId());
        assertEquals(narManifest.getDependencyVersion(), narProperties.getNarDependencyVersion());
        assertEquals(persistenceContext.getSource().name(), narProperties.getSourceType());
        assertEquals(persistenceContext.getSourceIdentifier(), narProperties.getSourceId());

        final BundleCoordinate bundleCoordinate = narManifest.getCoordinate();
        assertTrue(persistenceProvider.exists(bundleCoordinate));

        final NarPersistenceInfo retrievedPersistenceInfo = persistenceProvider.getNarInfo(bundleCoordinate);
        assertNotNull(retrievedPersistenceInfo);
        assertEquals(persistenceInfo, retrievedPersistenceInfo);

        final Set<NarPersistenceInfo> allPersistenceInfo = persistenceProvider.getAllNarInfo();
        assertNotNull(allPersistenceInfo);
        assertEquals(1, allPersistenceInfo.size());
        assertEquals(retrievedPersistenceInfo, allPersistenceInfo.stream().findAny().orElse(null));

        try (final InputStream narInputStream = persistenceProvider.readNar(bundleCoordinate)) {
            final String readContent = new String(narInputStream.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(narContents, readContent);
        }

        persistenceProvider.deleteNar(bundleCoordinate);

        final Set<NarPersistenceInfo> persistenceInfoAfterDelete = persistenceProvider.getAllNarInfo();
        assertNotNull(persistenceInfoAfterDelete);
        assertEquals(0, persistenceInfoAfterDelete.size());
    }

    @Test
    public void testRestoreDuringInitialization() throws IOException {
        final File tempFile;
        final String narContents = "Fake NAR Contents";
        try (final InputStream inputStream = new ByteArrayInputStream(narContents.getBytes(StandardCharsets.UTF_8))) {
            tempFile = persistenceProvider.createTempFile(inputStream);
        }

        final NarManifest narManifest = createNarManifest();
        final NarPersistenceContext persistenceContext = createNarPersistenceContext(narManifest);

        final NarPersistenceInfo persistenceInfo = persistenceProvider.saveNar(persistenceContext, tempFile);
        assertNotNull(persistenceInfo);

        // Create a new provider and initialize it pointing at the same storage location
        final NarPersistenceProvider secondPersistenceProvider = initializeProvider(tempDir);

        // Verify the new provider returns the previously persisted NAR
        final Set<NarPersistenceInfo> allPersistenceInfo = secondPersistenceProvider.getAllNarInfo();
        assertNotNull(allPersistenceInfo);
        assertEquals(1, allPersistenceInfo.size());
        assertEquals(persistenceInfo, allPersistenceInfo.stream().findAny().orElse(null));
    }

    private NarPersistenceContext createNarPersistenceContext(final NarManifest narManifest) {
        return NarPersistenceContext.builder()
                .manifest(narManifest)
                .source(NarSource.UPLOAD)
                .sourceIdentifier("1234")
                .clusterCoordinator(false)
                .build();
    }

    private NarManifest createNarManifest() {
        return NarManifest.builder()
                .group("my-group")
                .id("my-id")
                .version("1.0.0")
                .dependencyGroup("dep-group")
                .dependencyId("dep-id")
                .dependencyVersion("2.0.0")
                .build();
    }

    private String readFile(final File file) throws IOException {
        final List<String> fileLines = Files.readAllLines(file.toPath());
        assertEquals(1, fileLines.size());
        return fileLines.getFirst();
    }

}
