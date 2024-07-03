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
package org.apache.nifi.nar;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.documentation.DocGenerator;
import org.apache.nifi.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Loads a set of NARs from the file system into the running application.
 *
 * NOTE: Initially this will only be used from the NarAutoLoader which is watching a directory for new files, but eventually
 * this may also be used for loading a NAR that was downloaded from the extension registry, and thus the load method
 * is synchronized to ensure only one set of NARs can be in process of loading at a given time.
 */
public class StandardNarLoader implements NarLoader {

    private static Logger LOGGER = LoggerFactory.getLogger(StandardNarLoader.class);

    private final File extensionsWorkingDir;
    private final File docsWorkingDir;
    private final NarClassLoaders narClassLoaders;
    private final ExtensionDiscoveringManager extensionManager;
    private final ExtensionMapping extensionMapping;
    private final ExtensionUiLoader extensionUiLoader;
    private final NarUnpackMode narUnpackMode;

    private Set<BundleDetails> previouslySkippedBundles;

    public StandardNarLoader(final File extensionsWorkingDir,
                             final File docsWorkingDir,
                             final NarClassLoaders narClassLoaders,
                             final ExtensionDiscoveringManager extensionManager,
                             final ExtensionMapping extensionMapping,
                             final ExtensionUiLoader extensionUiLoader,
                             final NarUnpackMode narUnpackMode) {
        this.extensionsWorkingDir = extensionsWorkingDir;
        this.docsWorkingDir = docsWorkingDir;
        this.narClassLoaders = narClassLoaders;
        this.extensionManager = extensionManager;
        this.extensionMapping = extensionMapping;
        this.extensionUiLoader = extensionUiLoader;
        this.narUnpackMode = narUnpackMode;
    }

    @Override
    public NarLoadResult load(final Collection<File> narFiles) {
        return load(narFiles, null);
    }

    @Override
    public synchronized NarLoadResult load(final Collection<File> narFiles, final Set<Class<?>> extensionTypes) {
        LOGGER.info("Starting load process for {} NARs...", narFiles.size());

        final List<File> unpackedNars = new ArrayList<>();

        for (final File narFile : narFiles) {
            LOGGER.debug("Unpacking {}...", narFile.getName());
            final File unpackedNar = unpack(narFile);
            if (unpackedNar != null) {
                LOGGER.debug("Completed unpacking {}", narFile.getName());
                unpackedNars.add(unpackedNar);
            }
        }

        if (previouslySkippedBundles != null && !previouslySkippedBundles.isEmpty()) {
            LOGGER.info("Including {} previously skipped bundle(s)", previouslySkippedBundles.size());
            previouslySkippedBundles.forEach(b -> unpackedNars.add(b.getWorkingDirectory()));
        }

        if (unpackedNars.isEmpty()) {
            LOGGER.info("No NARs were unpacked, nothing to do");
            return new NarLoadResult(Collections.emptySet(), Collections.emptySet());
        }

        LOGGER.info("Creating class loaders for {} NARs...", unpackedNars.size());

        final NarLoadResult narLoadResult = narClassLoaders.loadAdditionalNars(unpackedNars);
        final Set<Bundle> loadedBundles = narLoadResult.getLoadedBundles();
        final Set<BundleDetails> skippedBundles = narLoadResult.getSkippedBundles();

        LOGGER.info("Successfully created class loaders for {} NARs, {} were skipped", loadedBundles.size(), skippedBundles.size());

        // Store skipped bundles for next iteration
        previouslySkippedBundles = new HashSet<>(skippedBundles);

        if (!loadedBundles.isEmpty()) {
            LOGGER.debug("Discovering extensions...");
            if (extensionTypes == null) {
                extensionManager.discoverExtensions(loadedBundles);
            } else {
                extensionManager.discoverExtensions(loadedBundles, extensionTypes, true);
            }

            // Call the DocGenerator for the classes that were loaded from each Bundle
            for (final Bundle bundle : loadedBundles) {
                final BundleCoordinate bundleCoordinate = bundle.getBundleDetails().getCoordinate();
                final Set<ExtensionDefinition> extensionDefinitions = extensionManager.getTypes(bundleCoordinate);
                if (extensionDefinitions.isEmpty()) {
                    LOGGER.debug("No documentation to generate for {} because no extensions were found", bundleCoordinate);
                } else {
                    LOGGER.debug("Generating documentation for {} extensions in {}", extensionDefinitions.size(), bundleCoordinate);
                    DocGenerator.documentConfigurableComponent(extensionDefinitions, docsWorkingDir, extensionManager);
                }
            }

            LOGGER.debug("Loading custom UIs for extensions...");
            if (extensionUiLoader != null) {
                extensionUiLoader.loadExtensionUis(loadedBundles);
            }
        }

        LOGGER.info("Finished NAR loading process!");
        return narLoadResult;
    }

    @Override
    public synchronized void unload(final Set<Bundle> bundles) {
        if (extensionUiLoader != null) {
            extensionUiLoader.unloadExtensionUis(bundles);
        }
        bundles.forEach(this::unload);
    }

    private void unload(final Bundle bundle) {
        final BundleCoordinate bundleCoordinate = bundle.getBundleDetails().getCoordinate();
        LOGGER.info("Unloading bundle [{}]", bundleCoordinate);

        final Bundle removedBundle = extensionManager.removeBundle(bundleCoordinate);
        if (removedBundle == null) {
            LOGGER.warn("The extension manager does not have a bundle registered with the coordinate [{}]", bundleCoordinate);
            return;
        }

        narClassLoaders.removeBundle(removedBundle);

        final File workingDirectory = removedBundle.getBundleDetails().getWorkingDirectory();
        if (workingDirectory.exists()) {
            LOGGER.info("Removing NAR working directory [{}]", workingDirectory.getAbsolutePath());
            try {
                FileUtils.deleteFile(workingDirectory, true);
            } catch (final IOException e) {
                LOGGER.warn("Failed to delete bundle working directory [{}]", workingDirectory.getAbsolutePath());
            }
        } else {
            LOGGER.info("NAR working directory does not exist at [{}]", workingDirectory.getAbsolutePath());
        }

        DocGenerator.removeBundleDocumentation(docsWorkingDir, bundleCoordinate);
    }

    private File unpack(final File narFile) {
        try (final JarFile nar = new JarFile(narFile)) {
            final Manifest manifest = nar.getManifest();

            final Attributes attributes = manifest.getMainAttributes();
            final String groupId = attributes.getValue(NarManifestEntry.NAR_GROUP.getEntryName());
            final String narId = attributes.getValue(NarManifestEntry.NAR_ID.getEntryName());
            final String version = attributes.getValue(NarManifestEntry.NAR_VERSION.getEntryName());

            if (NarClassLoaders.FRAMEWORK_NAR_ID.equals(narId)) {
                LOGGER.error("Found a framework NAR, will not auto-load {}", narFile.getAbsolutePath());
                return null;
            }

            if (NarClassLoaders.JETTY_NAR_ID.equals(narId)) {
                LOGGER.error("Found a Jetty NAR, will not auto-load {}", narFile.getAbsolutePath());
                return null;
            }

            final BundleCoordinate coordinate = new BundleCoordinate(groupId, narId, version);

            final Bundle bundle = extensionManager.getBundle(coordinate);
            if (bundle != null) {
                LOGGER.warn("Found existing bundle with coordinate {}, will not load {}", coordinate, narFile.getAbsolutePath());
                return null;
            }

            final File unpackedExtension = NarUnpacker.unpackNar(narFile, extensionsWorkingDir, true, narUnpackMode);
            NarUnpacker.mapExtension(unpackedExtension, coordinate, docsWorkingDir, extensionMapping);
            return unpackedExtension;

        } catch (Exception e) {
            LOGGER.error("Error unpacking {}", narFile.getAbsolutePath(), e);
            return null;
        }
    }

}
