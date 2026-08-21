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
package org.apache.nifi.runtime.manifest.impl;

import org.apache.nifi.extension.manifest.ExtensionManifest;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestParser;
import org.apache.nifi.runtime.manifest.ExtensionManifestContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DirectoryExtensionManifestProviderTest {

    private static final String MANIFEST_RELATIVE_PATH = "META-INF/docs/extension-manifest.xml";
    private static final String ADDITIONAL_DETAILS_RELATIVE_PATH = "META-INF/docs/additional-details";

    private final ExtensionManifestParser artifactIdParser = new ArtifactIdExtensionManifestParser();

    @Test
    void testExtensionManifestsReturnedInSortedOrderRegardlessOfCreationOrder(@TempDir final Path baseDir) throws IOException {
        final List<String> creationOrder = List.of("nifi-standard-nar", "nifi-aws-nar", "nifi-kafka-nar", "nifi-azure-nar");
        for (final String bundleArtifactId : creationOrder) {
            createBundleManifest(baseDir, bundleArtifactId);
        }

        final DirectoryExtensionManifestProvider provider = new DirectoryExtensionManifestProvider(baseDir.toFile(), artifactIdParser);
        final List<ExtensionManifestContainer> containers = provider.getExtensionManifests();

        final List<String> returnedArtifactIds = new ArrayList<>();
        for (final ExtensionManifestContainer container : containers) {
            returnedArtifactIds.add(container.getManifest().getArtifactId());
        }

        final List<String> expectedArtifactIds = List.of("nifi-aws-nar", "nifi-azure-nar", "nifi-kafka-nar", "nifi-standard-nar");
        assertEquals(expectedArtifactIds, returnedArtifactIds);
    }

    @Test
    void testAdditionalDetailsReturnedInSortedOrderRegardlessOfCreationOrder(@TempDir final Path baseDir) throws IOException {
        final Path bundleDir = createBundleManifest(baseDir, "nifi-standard-nar");

        final List<String> creationOrder = List.of("org.apache.nifi.ZProcessor", "org.apache.nifi.AProcessor", "org.apache.nifi.MProcessor");
        for (final String extensionType : creationOrder) {
            final Path additionalDetailsTypeDir = bundleDir.resolve(ADDITIONAL_DETAILS_RELATIVE_PATH).resolve(extensionType);
            Files.createDirectories(additionalDetailsTypeDir);
            Files.writeString(additionalDetailsTypeDir.resolve("additionalDetails.md"), extensionType);
        }

        final DirectoryExtensionManifestProvider provider = new DirectoryExtensionManifestProvider(baseDir.toFile(), artifactIdParser);
        final List<ExtensionManifestContainer> containers = provider.getExtensionManifests();

        assertEquals(1, containers.size());

        final List<String> returnedTypes = new ArrayList<>(containers.getFirst().getAdditionalDetails().keySet());
        final List<String> expectedTypes = List.of("org.apache.nifi.AProcessor", "org.apache.nifi.MProcessor", "org.apache.nifi.ZProcessor");
        assertEquals(expectedTypes, returnedTypes);
    }

    private Path createBundleManifest(final Path baseDir, final String bundleArtifactId) throws IOException {
        final Path bundleDir = baseDir.resolve(bundleArtifactId);
        final Path manifestFile = bundleDir.resolve(MANIFEST_RELATIVE_PATH);
        Files.createDirectories(manifestFile.getParent());
        Files.writeString(manifestFile, bundleArtifactId);
        return bundleDir;
    }

    /**
     * Parser that reads the entire manifest content as the artifact identifier, allowing the test to correlate a
     * returned container with the directory that produced it without depending on the full manifest XML schema.
     */
    private static class ArtifactIdExtensionManifestParser implements ExtensionManifestParser {
        @Override
        public ExtensionManifest parse(final InputStream inputStream) {
            try {
                final String artifactId = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                final ExtensionManifest extensionManifest = new ExtensionManifest();
                extensionManifest.setGroupId("org.apache.nifi");
                extensionManifest.setArtifactId(artifactId);
                extensionManifest.setVersion("2.12.0");
                return extensionManifest;
            } catch (final IOException e) {
                throw new RuntimeException("Unable to read manifest content", e);
            }
        }
    }
}
