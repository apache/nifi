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
import org.apache.nifi.runtime.manifest.RuntimeManifestBuilder;
import org.apache.nifi.runtime.manifest.impl.SchedulingDefaultsFactory;
import org.apache.nifi.runtime.manifest.impl.StandardRuntimeManifestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Optional;
import java.util.Set;

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

    private Optional<ExtensionManifest> getExtensionManifest(final Bundle bundle) {
        final BundleDetails bundleDetails = bundle.getBundleDetails();
        final File manifestFile = new File(bundleDetails.getWorkingDirectory(), "META-INF/docs/extension-manifest.xml");
        if (!manifestFile.exists()) {
            LOGGER.warn("Unable to find extension manifest for [{}] at [{}]...", bundleDetails.getCoordinate(), manifestFile.getAbsolutePath());
            return Optional.empty();
        }

        try (final InputStream inputStream = new FileInputStream(manifestFile)) {
            final ExtensionManifest extensionManifest = extensionManifestParser.parse(inputStream);
            // Newer NARs will have these fields populated in extension-manifest.xml, but older NARs will not, so we can
            // set the values from the BundleCoordinate which already has the group, artifact id, and version
            extensionManifest.setGroupId(bundleDetails.getCoordinate().getGroup());
            extensionManifest.setArtifactId(bundleDetails.getCoordinate().getId());
            extensionManifest.setVersion(bundleDetails.getCoordinate().getVersion());
            return Optional.of(extensionManifest);
        } catch (final IOException e) {
            LOGGER.error("Unable to load extension manifest for bundle [{}]", bundleDetails.getCoordinate(), e);
            return Optional.empty();
        }
    }

    // Visible for overriding from tests
    Bundle getFrameworkBundle() {
        return NarClassLoadersHolder.getInstance().getFrameworkBundle();
    }

}
