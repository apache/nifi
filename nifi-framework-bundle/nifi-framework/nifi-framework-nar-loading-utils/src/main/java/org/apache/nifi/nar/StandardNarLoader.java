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
 * NOTE: Initially this will only be used from the NarAutoLoader which is watching a directory for new files, but eventually
 * this may also be used for loading a NAR that was downloaded from the extension registry, and thus the load method
 * is synchronized to ensure only one set of NARs can be in process of loading at a given time.
 */
public class StandardNarLoader implements NarLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardNarLoader.class);

    private final File extensionsWorkingDir;
    private final NarClassLoaders narClassLoaders;
    private final ExtensionDiscoveringManager extensionManager;
    private final ExtensionMapping extensionMapping;
    private final ExtensionUiLoader extensionUiLoader;
    private final NarUnpackMode narUnpackMode;

    private Set<BundleDetails> previouslySkippedBundles;

    public StandardNarLoader(final File extensionsWorkingDir,
                             final NarClassLoaders narClassLoaders,
                             final ExtensionDiscoveringManager extensionManager,
                             final ExtensionMapping extensionMapping,
                             final ExtensionUiLoader extensionUiLoader,
                             final NarUnpackMode narUnpackMode) {
        this.extensionsWorkingDir = extensionsWorkingDir;
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
        LOGGER.info("Loading NAR Files [{}]", narFiles.size());

        final List<File> unpackedNars = new ArrayList<>();

        for (final File narFile : narFiles) {
            LOGGER.debug("Unpacking NAR File [{}] started", narFile.getName());
            final File unpackedNar = unpack(narFile);
            if (unpackedNar != null) {
                LOGGER.debug("Unpacking NAR File [{}] completed", narFile.getName());
                unpackedNars.add(unpackedNar);
            }
        }

        if (previouslySkippedBundles != null && !previouslySkippedBundles.isEmpty()) {
            LOGGER.info("Including [{}] previously skipped bundles", previouslySkippedBundles.size());
            previouslySkippedBundles.forEach(b -> unpackedNars.add(b.getWorkingDirectory()));
        }

        if (unpackedNars.isEmpty()) {
            return new NarLoadResult(Collections.emptySet(), Collections.emptySet());
        }

        final NarLoadResult narLoadResult = narClassLoaders.loadAdditionalNars(unpackedNars);
        final Set<Bundle> loadedBundles = narLoadResult.getLoadedBundles();
        final Set<BundleDetails> skippedBundles = narLoadResult.getSkippedBundles();

        LOGGER.info("Created class loaders for [{}] NAR bundles with [{}] skipped", loadedBundles.size(), skippedBundles.size());

        // Store skipped bundles for next iteration
        previouslySkippedBundles = new HashSet<>(skippedBundles);

        if (!loadedBundles.isEmpty()) {
            if (extensionTypes == null) {
                extensionManager.discoverExtensions(loadedBundles);
                discoverPythonExtensions(loadedBundles);
            } else {
                extensionManager.discoverExtensions(loadedBundles, extensionTypes, true);
                discoverPythonExtensions(loadedBundles);
            }

            if (extensionUiLoader != null) {
                LOGGER.debug("Loading custom UI extensions");
                extensionUiLoader.loadExtensionUis(loadedBundles);
            }
        }

        return narLoadResult;
    }

    @Override
    public synchronized void unload(final Collection<Bundle> bundles) {
        if (extensionUiLoader != null) {
            extensionUiLoader.unloadExtensionUis(bundles);
        }

        final List<BundleCoordinate> bundleCoordinates = bundles.stream()
                .map(Bundle::getBundleDetails)
                .map(BundleDetails::getCoordinate)
                .toList();

        for (final BundleCoordinate bundleCoordinate : bundleCoordinates) {
            LOGGER.info("Unloading bundle [{}]", bundleCoordinate);
        }

        final Set<Bundle> removedBundles = extensionManager.removeBundles(bundleCoordinates);
        removedBundles.forEach(this::removeBundle);
    }

    private void removeBundle(final Bundle bundle) {
        narClassLoaders.removeBundle(bundle);

        final File workingDirectory = bundle.getBundleDetails().getWorkingDirectory();
        if (workingDirectory.exists()) {
            LOGGER.debug("Removing NAR working directory [{}]", workingDirectory.getAbsolutePath());
            try {
                FileUtils.deleteFile(workingDirectory, true);
            } catch (final IOException e) {
                LOGGER.warn("Failed to delete bundle working directory [{}]", workingDirectory.getAbsolutePath());
            }
        } else {
            LOGGER.debug("NAR working directory does not exist at [{}]", workingDirectory.getAbsolutePath());
        }
    }

    private void discoverPythonExtensions(final Set<Bundle> loadedBundles) {
        final Bundle pythonBundle = extensionManager.getBundle(PythonBundle.PYTHON_BUNDLE_COORDINATE);
        if (pythonBundle == null) {
            LOGGER.warn("Python Bundle does not exist in the ExtensionManager, will not discover new Python extensions");
        } else {
            extensionManager.discoverPythonExtensions(pythonBundle, loadedBundles);
        }
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
            NarUnpacker.mapExtension(unpackedExtension, coordinate, extensionMapping);
            return unpackedExtension;

        } catch (Exception e) {
            LOGGER.error("Error unpacking {}", narFile.getAbsolutePath(), e);
            return null;
        }
    }

}
