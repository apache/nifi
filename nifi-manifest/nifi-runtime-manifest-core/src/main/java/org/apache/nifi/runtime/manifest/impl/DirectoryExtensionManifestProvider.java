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
import org.apache.nifi.runtime.manifest.ExtensionManifestProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ExtensionManifestProvider that loads extension manifests from a directory where the nifi-assembly-manifests
 * artifact was unpacked.
 */
public class DirectoryExtensionManifestProvider implements ExtensionManifestProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryExtensionManifestProvider.class);

    private final File baseDir;
    private final ExtensionManifestParser extensionManifestParser;

    public DirectoryExtensionManifestProvider(final File baseDir, final ExtensionManifestParser extensionManifestParser) {
        this.baseDir = baseDir;
        this.extensionManifestParser = extensionManifestParser;
    }

    @Override
    public List<ExtensionManifestContainer> getExtensionManifests() {
        if (!baseDir.exists()) {
            throw new IllegalArgumentException("The specified manifest directory does not exist");
        }
        if (!baseDir.isDirectory()) {
            throw new IllegalArgumentException("The specified manifest location is not a directory");
        }

        LOGGER.info("Loading extension manifests from: {}", baseDir.getAbsolutePath());

        final List<ExtensionManifestContainer> extensionManifests = new ArrayList<>();
        for (final File manifestDir : baseDir.listFiles()) {
            if (!manifestDir.isDirectory()) {
                LOGGER.debug("Skipping [{}], not a directory...", manifestDir.getAbsolutePath());
                continue;
            }

            final File manifestFile = new File(manifestDir, "META-INF/docs/extension-manifest.xml");
            LOGGER.debug("Loading extension manifest file [{}]", manifestFile.getAbsolutePath());

            final ExtensionManifest extensionManifest = loadExtensionManifest(manifestFile);
            final Map<String, String> additionalDetails = loadAdditionalDetails(manifestDir);

            final ExtensionManifestContainer container = new ExtensionManifestContainer(extensionManifest, additionalDetails);
            extensionManifests.add(container);

            LOGGER.debug("Successfully loaded extension manifest for [{}-{}-{}]",
                    extensionManifest.getGroupId(), extensionManifest.getArtifactId(), extensionManifest.getVersion());
        }

        LOGGER.info("Loaded {} extension manifests", extensionManifests.size());
        return extensionManifests;
    }

    private ExtensionManifest loadExtensionManifest(final File manifestFile) {
        try (final InputStream inputStream = new FileInputStream(manifestFile)) {
            return extensionManifestParser.parse(inputStream);
        } catch (final IOException ioException) {
            throw new RuntimeException("Unable to load extension manifest: " + manifestFile.getAbsolutePath(), ioException);
        }
    }

    private Map<String, String> loadAdditionalDetails(final File manifestDir) {
        final Map<String, String> additionalDetailsMap = new LinkedHashMap<>();

        final File additionalDetailsDir = new File(manifestDir, "META-INF/docs/additional-details");
        if (!additionalDetailsDir.exists()) {
            LOGGER.debug("No additional-details directory found under [{}]", manifestDir.getAbsolutePath());
            return additionalDetailsMap;
        }

        for (final File additionalDetailsTypeDir : additionalDetailsDir.listFiles()) {
            if (!additionalDetailsTypeDir.isDirectory()) {
                LOGGER.debug("Skipping [{}], not a directory...", additionalDetailsTypeDir.getAbsolutePath());
                continue;
            }

            final File additionalDetailsFile = new File(additionalDetailsTypeDir, "additionalDetails.md");
            if (!additionalDetailsFile.exists()) {
                LOGGER.debug("No additionalDetails.md found under [{}]", additionalDetailsTypeDir.getAbsolutePath());
                continue;
            }

            try {
                final String typeName = additionalDetailsTypeDir.getName();
                final byte[] additionalDetailsBytes = Files.readAllBytes(additionalDetailsFile.toPath());
                LOGGER.debug("Added additionalDetails for {} from {}", typeName, additionalDetailsFile.getAbsolutePath());
                additionalDetailsMap.put(typeName, new String(additionalDetailsBytes, StandardCharsets.UTF_8));
            } catch (final IOException e) {
                throw new RuntimeException("Unable to load additional details content for "
                        + additionalDetailsFile.getAbsolutePath() + " due to: " + e.getMessage(), e);
            }
        }

        return additionalDetailsMap;
    }

}
