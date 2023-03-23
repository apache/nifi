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
package org.apache.nifi.manifest;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.c2.protocol.component.api.BuildInfo;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.extension.manifest.ExtensionManifest;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestParser;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.runtime.manifest.ExtensionManifestContainer;
import org.apache.nifi.runtime.manifest.RuntimeManifestBuilder;
import org.apache.nifi.runtime.manifest.impl.SchedulingDefaultsFactory;
import org.apache.nifi.runtime.manifest.impl.StandardRuntimeManifestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StandardRuntimeManifestService implements RuntimeManifestService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardRuntimeManifestService.class);

    private static final String RUNTIME_MANIFEST_IDENTIFIER = "nifi";
    private static final String RUNTIME_TYPE = "nifi";

    private final ExtensionManager extensionManager;
    private final ExtensionManifestParser extensionManifestParser;
    private final String runtimeManifestIdentifier;
    private final String runtimeType;

    public StandardRuntimeManifestService(final ExtensionManager extensionManager, final ExtensionManifestParser extensionManifestParser,
                                          final String runtimeManifestIdentifier, final String runtimeType) {
        this.extensionManager = extensionManager;
        this.extensionManifestParser = extensionManifestParser;
        this.runtimeManifestIdentifier = runtimeManifestIdentifier;
        this.runtimeType = runtimeType;
    }

    public StandardRuntimeManifestService(final ExtensionManager extensionManager, final ExtensionManifestParser extensionManifestParser) {
        this(extensionManager, extensionManifestParser, RUNTIME_MANIFEST_IDENTIFIER, RUNTIME_TYPE);
    }

    @Override
    public RuntimeManifest getManifest() {
        final Set<Bundle> allBundles = extensionManager.getAllBundles();

        final Bundle frameworkBundle = getFrameworkBundle();
        final BundleDetails frameworkDetails = frameworkBundle.getBundleDetails();
        final Date frameworkBuildDate = frameworkDetails.getBuildTimestampDate();

        final BuildInfo buildInfo = new BuildInfo();
        buildInfo.setVersion(frameworkDetails.getCoordinate().getVersion());
        buildInfo.setRevision(frameworkDetails.getBuildRevision());
        buildInfo.setCompiler(frameworkDetails.getBuildJdk());
        buildInfo.setTimestamp(frameworkBuildDate == null ? null : frameworkBuildDate.getTime());

        final RuntimeManifestBuilder manifestBuilder = new StandardRuntimeManifestBuilder()
                .identifier(runtimeManifestIdentifier)
                .runtimeType(runtimeType)
                .version(buildInfo.getVersion())
                .schedulingDefaults(SchedulingDefaultsFactory.getNifiSchedulingDefaults())
                .buildInfo(buildInfo);

        for (final Bundle bundle : allBundles) {
            getExtensionManifest(bundle).ifPresent(manifestBuilder::addBundle);
        }

        return manifestBuilder.build();
    }

    private Optional<ExtensionManifestContainer> getExtensionManifest(final Bundle bundle) {
        final BundleDetails bundleDetails = bundle.getBundleDetails();
        try {
            final ExtensionManifest extensionManifest = loadExtensionManifest(bundleDetails);
            final Map<String, String> additionalDetails = loadAdditionalDetails(bundleDetails);

            final ExtensionManifestContainer container = new ExtensionManifestContainer(extensionManifest, additionalDetails);
            return Optional.of(container);
        } catch (final IOException e) {
            LOGGER.error("Unable to load extension manifest for bundle [{}]", bundleDetails.getCoordinate(), e);
            return Optional.empty();
        }
    }

    private ExtensionManifest loadExtensionManifest(final BundleDetails bundleDetails) throws IOException {
        final File manifestFile = new File(bundleDetails.getWorkingDirectory(), "META-INF/docs/extension-manifest.xml");
        if (!manifestFile.exists()) {
            throw new FileNotFoundException("Extension manifest files does not exist for "
                    + bundleDetails.getCoordinate() + " at " + manifestFile.getAbsolutePath());
        }

        try (final InputStream inputStream = new FileInputStream(manifestFile)) {
            final ExtensionManifest extensionManifest = extensionManifestParser.parse(inputStream);
            // Newer NARs will have these fields populated in extension-manifest.xml, but older NARs will not, so we can
            // set the values from the BundleCoordinate which already has the group, artifact id, and version
            extensionManifest.setGroupId(bundleDetails.getCoordinate().getGroup());
            extensionManifest.setArtifactId(bundleDetails.getCoordinate().getId());
            extensionManifest.setVersion(bundleDetails.getCoordinate().getVersion());
            return extensionManifest;
        }
    }

    private Map<String, String> loadAdditionalDetails(final BundleDetails bundleDetails) {
        final Map<String, String> additionalDetailsMap = new LinkedHashMap<>();

        final File additionalDetailsDir = new File(bundleDetails.getWorkingDirectory(), "META-INF/docs/additional-details");
        if (!additionalDetailsDir.exists()) {
            LOGGER.debug("No additional-details directory found under [{}]", bundleDetails.getWorkingDirectory().getAbsolutePath());
            return additionalDetailsMap;
        }

        for (final File additionalDetailsTypeDir : additionalDetailsDir.listFiles()) {
            if (!additionalDetailsTypeDir.isDirectory()) {
                LOGGER.debug("Skipping [{}], not a directory...", additionalDetailsTypeDir.getAbsolutePath());
                continue;
            }

            final File additionalDetailsFile = new File(additionalDetailsTypeDir, "additionalDetails.html");
            if (!additionalDetailsFile.exists()) {
                LOGGER.debug("No additionalDetails.html found under [{}]", additionalDetailsTypeDir.getAbsolutePath());
                continue;
            }

            try (final Stream<String> additionalDetailsLines = Files.lines(additionalDetailsFile.toPath())) {
                final String typeName = additionalDetailsTypeDir.getName();
                final String additionalDetailsContent = additionalDetailsLines.collect(Collectors.joining());
                LOGGER.debug("Added additionalDetails for {} from {}", typeName, additionalDetailsFile.getAbsolutePath());
                additionalDetailsMap.put(typeName, additionalDetailsContent);
            } catch (final IOException e) {
                throw new RuntimeException("Unable to load additional details content for "
                        + additionalDetailsFile.getAbsolutePath() + " due to: " + e.getMessage(), e);
            }
        }

        return additionalDetailsMap;
    }

    // Visible for overriding from tests
    Bundle getFrameworkBundle() {
        return NarClassLoadersHolder.getInstance().getFrameworkBundle();
    }

}
