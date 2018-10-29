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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Used to initialize the extension and framework classloaders.
 *
 * The core framework should obtain a singleton reference from NarClassLoadersHolder.
 */
public final class NarClassLoaders {

    public static final String FRAMEWORK_NAR_ID = "nifi-framework-nar";
    public static final String JETTY_NAR_ID = "nifi-jetty-bundle";

    private volatile InitContext initContext;
    private static final Logger logger = LoggerFactory.getLogger(NarClassLoaders.class);

    private final static class InitContext {

        private final File frameworkWorkingDir;
        private final File extensionWorkingDir;
        private final Bundle frameworkBundle;
        private final Bundle jettyBundle;
        private final Map<String, Bundle> bundles;

        private InitContext(
                final File frameworkDir,
                final File extensionDir,
                final Bundle frameworkBundle,
                final Bundle jettyBundle,
                final Map<String, Bundle> bundles) {
            this.frameworkWorkingDir = frameworkDir;
            this.extensionWorkingDir = extensionDir;
            this.frameworkBundle = frameworkBundle;
            this.jettyBundle = jettyBundle;
            this.bundles = bundles;
        }
    }

    /**
     * Initializes and loads the NarClassLoaders. This method must be called
     * before the rest of the methods to access the classloaders are called and
     * it can be safely called any number of times provided the same framework
     * and extension working dirs are used.
     *
     * @param frameworkWorkingDir where to find framework artifacts
     * @param extensionsWorkingDir where to find extension artifacts
     * @throws java.io.IOException if any issue occurs while exploding nar working directories.
     * @throws java.lang.ClassNotFoundException if unable to load class definition
     * @throws IllegalStateException already initialized with a given pair of
     * directories cannot reinitialize or use a different pair of directories.
     */
    public void init(File frameworkWorkingDir, File extensionsWorkingDir) throws IOException, ClassNotFoundException {
        init(ClassLoader.getSystemClassLoader(), frameworkWorkingDir, extensionsWorkingDir);
    }

    /**
     * Initializes and loads the NarClassLoaders. This method must be called
     * before the rest of the methods to access the classloaders are called and
     * it can be safely called any number of times provided the same framework
     * and extension working dirs are used.
     *
     * @param rootClassloader the root classloader to use for booting Jetty
     * @param frameworkWorkingDir where to find framework artifacts
     * @param extensionsWorkingDir where to find extension artifacts
     * @throws java.io.IOException if any issue occurs while exploding nar working directories.
     * @throws java.lang.ClassNotFoundException if unable to load class definition
     * @throws IllegalStateException already initialized with a given pair of
     * directories cannot reinitialize or use a different pair of directories.
     */
    public void init(final ClassLoader rootClassloader,
                     final File frameworkWorkingDir, final File extensionsWorkingDir) throws IOException, ClassNotFoundException {
        if (frameworkWorkingDir == null || extensionsWorkingDir == null) {
            throw new NullPointerException("cannot have empty arguments");
        }
        InitContext ic = initContext;
        if (ic == null) {
            synchronized (this) {
                ic = initContext;
                if (ic == null) {
                    initContext = ic = load(rootClassloader, frameworkWorkingDir, extensionsWorkingDir);
                }
            }
        }
        boolean matching = initContext.extensionWorkingDir.equals(extensionsWorkingDir)
                && initContext.frameworkWorkingDir.equals(frameworkWorkingDir);
        if (!matching) {
            throw new IllegalStateException("Cannot reinitialize and extension/framework directories cannot change");
        }
    }

    /**
     * Should be called at most once.
     */
    private InitContext load(final ClassLoader rootClassloader,
                             final File frameworkWorkingDir, final File extensionsWorkingDir)
            throws IOException, ClassNotFoundException {

        // find all nar files and create class loaders for them.
        final Map<String, Bundle> narDirectoryBundleLookup = new LinkedHashMap<>();
        final Map<String, ClassLoader> narCoordinateClassLoaderLookup = new HashMap<>();
        final Map<String, Set<BundleCoordinate>> narIdBundleLookup = new HashMap<>();

        // make sure the nar directory is there and accessible
        FileUtils.ensureDirectoryExistAndCanReadAndWrite(frameworkWorkingDir);
        FileUtils.ensureDirectoryExistAndCanReadAndWrite(extensionsWorkingDir);

        final List<File> narWorkingDirContents = new ArrayList<>();
        final File[] frameworkWorkingDirContents = frameworkWorkingDir.listFiles();
        if (frameworkWorkingDirContents != null) {
            narWorkingDirContents.addAll(Arrays.asList(frameworkWorkingDirContents));
        }
        final File[] extensionsWorkingDirContents = extensionsWorkingDir.listFiles();
        if (extensionsWorkingDirContents != null) {
            narWorkingDirContents.addAll(Arrays.asList(extensionsWorkingDirContents));
        }

        if (!narWorkingDirContents.isEmpty()) {
            final List<BundleDetails> narDetails = new ArrayList<>();
            final Map<String,String> narCoordinatesToWorkingDir = new HashMap<>();

            // load the nar details which includes and nar dependencies
            for (final File unpackedNar : narWorkingDirContents) {
                BundleDetails narDetail = null;
                try {
                     narDetail = getNarDetails(unpackedNar);
                } catch (IllegalStateException e) {
                    logger.warn("Unable to load NAR {} due to {}, skipping...",
                            new Object[] {unpackedNar.getAbsolutePath(), e.getMessage()});
                    continue;
                }

                // prevent the application from starting when there are two NARs with same group, id, and version
                final String narCoordinate = narDetail.getCoordinate().getCoordinate();
                if (narCoordinatesToWorkingDir.containsKey(narCoordinate)) {
                    final String existingNarWorkingDir = narCoordinatesToWorkingDir.get(narCoordinate);
                    throw new IllegalStateException("Unable to load NAR with coordinates " + narCoordinate
                            + " and working directory " + narDetail.getWorkingDirectory()
                            + " because another NAR with the same coordinates already exists at " + existingNarWorkingDir);
                }

                narDetails.add(narDetail);
                narCoordinatesToWorkingDir.put(narCoordinate, narDetail.getWorkingDirectory().getCanonicalPath());
            }

            // attempt to locate the jetty nar
            ClassLoader jettyClassLoader = null;
            for (final Iterator<BundleDetails> narDetailsIter = narDetails.iterator(); narDetailsIter.hasNext();) {
                final BundleDetails narDetail = narDetailsIter.next();

                // look for the jetty nar
                if (JETTY_NAR_ID.equals(narDetail.getCoordinate().getId())) {
                    // create the jetty classloader
                    jettyClassLoader = createNarClassLoader(narDetail.getWorkingDirectory(), rootClassloader);

                    // remove the jetty nar since its already loaded
                    narDirectoryBundleLookup.put(narDetail.getWorkingDirectory().getCanonicalPath(), new Bundle(narDetail, jettyClassLoader));
                    narCoordinateClassLoaderLookup.put(narDetail.getCoordinate().getCoordinate(), jettyClassLoader);
                    narDetailsIter.remove();
                }

                // populate bundle lookup
                narIdBundleLookup.computeIfAbsent(narDetail.getCoordinate().getId(), id -> new HashSet<>()).add(narDetail.getCoordinate());
            }

            // ensure the jetty nar was found
            if (jettyClassLoader == null) {
                throw new IllegalStateException("Unable to locate Jetty bundle.");
            }

            int narCount;
            do {
                // record the number of nars to be loaded
                narCount = narDetails.size();

                // attempt to create each nar class loader
                for (final Iterator<BundleDetails> narDetailsIter = narDetails.iterator(); narDetailsIter.hasNext();) {
                    final BundleDetails narDetail = narDetailsIter.next();
                    final BundleCoordinate narDependencyCoordinate = narDetail.getDependencyCoordinate();

                    // see if this class loader is eligible for loading
                    ClassLoader narClassLoader = null;
                    if (narDependencyCoordinate == null) {
                        narClassLoader = createNarClassLoader(narDetail.getWorkingDirectory(), jettyClassLoader);
                    } else {
                        final String dependencyCoordinateStr = narDependencyCoordinate.getCoordinate();

                        // if the declared dependency has already been loaded
                        if (narCoordinateClassLoaderLookup.containsKey(dependencyCoordinateStr)) {
                            final ClassLoader narDependencyClassLoader = narCoordinateClassLoaderLookup.get(dependencyCoordinateStr);
                            narClassLoader = createNarClassLoader(narDetail.getWorkingDirectory(), narDependencyClassLoader);
                        } else {
                            // get all bundles that match the declared dependency id
                            final Set<BundleCoordinate> coordinates = narIdBundleLookup.get(narDependencyCoordinate.getId());

                            // ensure there are known bundles that match the declared dependency id
                            if (coordinates != null && !coordinates.contains(narDependencyCoordinate)) {
                                // ensure the declared dependency only has one possible bundle
                                if (coordinates.size() == 1) {
                                    // get the bundle with the matching id
                                    final BundleCoordinate coordinate = coordinates.stream().findFirst().get();

                                    // if that bundle is loaded, use it
                                    if (narCoordinateClassLoaderLookup.containsKey(coordinate.getCoordinate())) {
                                        logger.warn(String.format("While loading '%s' unable to locate exact NAR dependency '%s'. Only found one possible match '%s'. Continuing...",
                                                narDetail.getCoordinate().getCoordinate(), dependencyCoordinateStr, coordinate.getCoordinate()));

                                        final ClassLoader narDependencyClassLoader = narCoordinateClassLoaderLookup.get(coordinate.getCoordinate());
                                        narClassLoader = createNarClassLoader(narDetail.getWorkingDirectory(), narDependencyClassLoader);
                                    }
                                }
                            }
                        }
                    }

                    // if we were able to create the nar class loader, store it and remove the details
                    final ClassLoader bundleClassLoader = narClassLoader;
                    if (bundleClassLoader != null) {
                        narDirectoryBundleLookup.put(narDetail.getWorkingDirectory().getCanonicalPath(), new Bundle(narDetail, bundleClassLoader));
                        narCoordinateClassLoaderLookup.put(narDetail.getCoordinate().getCoordinate(), narClassLoader);
                        narDetailsIter.remove();
                    }
                }

                // attempt to load more if some were successfully loaded this iteration
            } while (narCount != narDetails.size());

            // see if any nars couldn't be loaded
            for (final BundleDetails narDetail : narDetails) {
                logger.warn(String.format("Unable to resolve required dependency '%s'. Skipping NAR '%s'",
                        narDetail.getDependencyCoordinate().getId(), narDetail.getWorkingDirectory().getAbsolutePath()));
            }
        }

        // find the framework bundle, NarUnpacker already checked that there was a framework NAR and that there was only one
        final Bundle frameworkBundle = narDirectoryBundleLookup.values().stream()
                .filter(b -> b.getBundleDetails().getCoordinate().getId().equals(FRAMEWORK_NAR_ID))
                .findFirst().orElse(null);

        // find the Jetty bundle
        final Bundle jettyBundle = narDirectoryBundleLookup.values().stream()
                .filter(b -> b.getBundleDetails().getCoordinate().getId().equals(JETTY_NAR_ID))
                .findFirst().orElse(null);

        if (jettyBundle == null) {
            throw new IllegalStateException("Unable to locate Jetty bundle.");
        }

        return new InitContext(frameworkWorkingDir, extensionsWorkingDir, frameworkBundle, jettyBundle, new LinkedHashMap<>(narDirectoryBundleLookup));
    }

    /**
     * Loads additional NARs after the application has been started.
     *
     * @param additionalUnpackedNars a list of files where each file represents a directory of an unpacked NAR to load
     * @return the result which includes the loaded bundles and details of skipped bundles
     */
    public synchronized NarLoadResult loadAdditionalNars(final List<File> additionalUnpackedNars) {
        if (initContext == null) {
            throw new IllegalStateException("Must call init before attempting to load additional NARs");
        }

        final Set<Bundle> loadedBundles = new LinkedHashSet<>();
        final List<BundleDetails> additionalBundleDetails = loadBundleDetails(additionalUnpackedNars);

        // Create a lookup from bundle id to set of coordinates with that id, needs to be across already loaded NARs + additional NARs currently being loaded
        final Map<String,Set<BundleCoordinate>> bundleIdToCoordinatesLookup = new HashMap<>();

        // Add the coordinates from the additional bundles
        for (final BundleDetails bundleDetail : additionalBundleDetails) {
            final String bundleId = bundleDetail.getCoordinate().getId();
            final Set<BundleCoordinate> coordinates = bundleIdToCoordinatesLookup.computeIfAbsent(bundleId, (id) -> new HashSet<>());
            coordinates.add(bundleDetail.getCoordinate());
        }

        // Add coordinates from the already loaded bundles
        for (final Bundle bundle : getBundles()) {
            final BundleDetails bundleDetail = bundle.getBundleDetails();
            final String bundleId = bundleDetail.getCoordinate().getId();
            final Set<BundleCoordinate> coordinates = bundleIdToCoordinatesLookup.computeIfAbsent(bundleId, (id) -> new HashSet<>());
            coordinates.add(bundleDetail.getCoordinate());
        }

        int bundleCount;
        do {
            // Record the number of bundles to be loaded
            bundleCount = additionalBundleDetails.size();

            // Attempt to create each bundle class loader
            for (final Iterator<BundleDetails> additionalBundleDetailsIter = additionalBundleDetails.iterator(); additionalBundleDetailsIter.hasNext();) {
                final BundleDetails bundleDetail = additionalBundleDetailsIter.next();
                try {
                    // If we were able to create the bundle class loader, store it and remove the details
                    final ClassLoader bundleClassLoader = createBundleClassLoader(bundleDetail, bundleIdToCoordinatesLookup);
                    if (bundleClassLoader != null) {
                        final Bundle bundle = new Bundle(bundleDetail, bundleClassLoader);
                        loadedBundles.add(bundle);
                        additionalBundleDetailsIter.remove();

                        // Need to add to overall bundles as we go so if other NARs depend on this one we can find it
                        initContext.bundles.put(bundleDetail.getWorkingDirectory().getCanonicalPath(), bundle);
                    }
                } catch (final Exception e) {
                    logger.error("Unable to load NAR {} due to {}, skipping...", new Object[]{bundleDetail.getWorkingDirectory(), e.getMessage()});
                }
            }

            // Attempt to load more if some were successfully loaded this iteration
        } while (bundleCount != additionalBundleDetails.size());

        // See if any bundles couldn't be loaded
        final Set<BundleDetails> skippedBundles = new HashSet<>();
        for (final BundleDetails bundleDetail : additionalBundleDetails) {
            logger.warn(String.format("Unable to resolve required dependency '%s'. Skipping NAR '%s'",
                    bundleDetail.getDependencyCoordinate().getId(), bundleDetail.getWorkingDirectory().getAbsolutePath()));
            skippedBundles.add(bundleDetail);
        }

        return new NarLoadResult(loadedBundles, skippedBundles);
    }

    private ClassLoader createBundleClassLoader(final BundleDetails bundleDetail, final Map<String,Set<BundleCoordinate>> bundleIdToCoordinatesLookup)
            throws IOException, ClassNotFoundException {

        ClassLoader bundleClassLoader = null;

        final BundleCoordinate bundleDependencyCoordinate = bundleDetail.getDependencyCoordinate();
        if (bundleDependencyCoordinate == null) {
            final ClassLoader jettyClassLoader = getJettyBundle().getClassLoader();
            bundleClassLoader = createNarClassLoader(bundleDetail.getWorkingDirectory(), jettyClassLoader);
        } else {
            final Optional<Bundle> dependencyBundle = getBundle(bundleDependencyCoordinate);

            // If the declared dependency has already been loaded then use it
            if (dependencyBundle.isPresent()) {
                final ClassLoader narDependencyClassLoader = dependencyBundle.get().getClassLoader();
                bundleClassLoader = createNarClassLoader(bundleDetail.getWorkingDirectory(), narDependencyClassLoader);
            } else {
                // Otherwise get all bundles that match the declared dependency id
                final Set<BundleCoordinate> coordinates = bundleIdToCoordinatesLookup.get(bundleDependencyCoordinate.getId());

                // Ensure there are known bundles that match the declared dependency id
                if (coordinates != null && !coordinates.contains(bundleDependencyCoordinate)) {
                    // Ensure the declared dependency only has one possible bundle
                    if (coordinates.size() == 1) {
                        // Get the bundle with the matching id
                        final BundleCoordinate coordinate = coordinates.stream().findFirst().get();

                        // If that bundle is loaded, use it
                        final Optional<Bundle> matchingDependencyIdBundle = getBundle(coordinate);
                        if (matchingDependencyIdBundle.isPresent()) {
                            final String dependencyCoordinateStr = bundleDependencyCoordinate.getCoordinate();
                            logger.warn(String.format("While loading '%s' unable to locate exact NAR dependency '%s'. Only found one possible match '%s'. Continuing...",
                                    bundleDetail.getCoordinate().getCoordinate(), dependencyCoordinateStr, coordinate.getCoordinate()));

                            final ClassLoader narDependencyClassLoader = matchingDependencyIdBundle.get().getClassLoader();
                            bundleClassLoader = createNarClassLoader(bundleDetail.getWorkingDirectory(), narDependencyClassLoader);
                        }
                    }
                }
            }
        }

        return bundleClassLoader;
    }

    private List<BundleDetails> loadBundleDetails(List<File> unpackedNars) {
        final List<BundleDetails> narDetails = new ArrayList<>();
        for (final File unpackedNar : unpackedNars) {
            try {
                final BundleDetails narDetail = getNarDetails(unpackedNar);
                final BundleCoordinate unpackedNarCoordinate = narDetail.getCoordinate();

                // Skip this NAR if there is another NAR with the same group, id, and version
                final Optional<Bundle> existingBundle = getBundle(unpackedNarCoordinate);
                if (existingBundle.isPresent()) {
                    final BundleDetails existingBundleDetails = existingBundle.get().getBundleDetails();
                    final String existingNarWorkingDir = existingBundleDetails.getWorkingDirectory().getCanonicalPath();
                    final String unpackedNarWorkingDir = narDetail.getWorkingDirectory().getCanonicalPath();

                    logger.error("Unable to load NAR with coordinates {} and working directory {} " +
                                    "because another NAR with the same coordinates already exists at {}",
                            new Object[]{unpackedNarCoordinate, unpackedNarWorkingDir, existingNarWorkingDir});
                } else {
                    narDetails.add(narDetail);
                }

            } catch (Exception e) {
                logger.error("Unable to load NAR {} due to {}, skipping...", new Object[]{unpackedNar.getAbsolutePath(), e.getMessage()});
            }
        }
        return narDetails;
    }

    /**
     * Creates a new NarClassLoader. The parentClassLoader may be null.
     *
     * @param narDirectory root directory of nar
     * @param parentClassLoader parent classloader of nar
     * @return the nar classloader
     * @throws IOException ioe
     * @throws ClassNotFoundException cfne
     */
    private static ClassLoader createNarClassLoader(final File narDirectory, final ClassLoader parentClassLoader) throws IOException, ClassNotFoundException {
        logger.debug("Loading NAR file: " + narDirectory.getAbsolutePath());
        final ClassLoader narClassLoader = new NarClassLoader(narDirectory, parentClassLoader);
        logger.info("Loaded NAR file: " + narDirectory.getAbsolutePath() + " as class loader " + narClassLoader);
        return narClassLoader;
    }

    /**
     * Loads the details for the specified NAR. The details will be extracted
     * from the manifest file.
     *
     * @param narDirectory the nar directory
     * @return details about the NAR
     * @throws IOException ioe
     */
    private static BundleDetails getNarDetails(final File narDirectory) throws IOException {
        return NarBundleUtil.fromNarDirectory(narDirectory);
    }

    /**
     * Gets the bundle with the given coordinate.
     *
     * @param bundleCoordinate the coordinate of the bundle to find
     * @return the bundle with the coordinate, or an empty optional
     */
    private Optional<Bundle> getBundle(final BundleCoordinate bundleCoordinate) {
        return initContext.bundles.values().stream()
                .filter(b -> b.getBundleDetails().getCoordinate().equals(bundleCoordinate))
                .findFirst();
    }

    /**
     * @return the framework class Bundle
     *
     * @throws IllegalStateException if the frame Bundle has not been loaded
     */
    public Bundle getFrameworkBundle() {
        if (initContext == null) {
            throw new IllegalStateException("Framework bundle has not been loaded.");
        }

        return initContext.frameworkBundle;
    }

    /**
     * @return the Jetty Bundle
     *
     * @throws IllegalStateException if the Jetty Bundle has not been loaded
     */
    public Bundle getJettyBundle() {
        if (initContext == null) {
            throw new IllegalStateException("Jetty bundle has not been loaded.");
        }

        return initContext.jettyBundle;
    }

    /**
     * @param extensionWorkingDirectory the directory
     * @return the bundle for the specified working directory. Returns
     * null when no bundle exists for the specified working directory
     * @throws IllegalStateException if the bundles have not been loaded
     */
    public Bundle getBundle(final File extensionWorkingDirectory) {
        if (initContext == null) {
            throw new IllegalStateException("Extensions class loaders have not been loaded.");
        }

        try {
           return initContext.bundles.get(extensionWorkingDirectory.getCanonicalPath());
        } catch (final IOException ioe) {
            if(logger.isDebugEnabled()){
                logger.debug("Unable to get extension classloader for working directory '{}'", extensionWorkingDirectory);
            }
            return null;
        }
    }

    /**
     * @return the extensions that have been loaded
     * @throws IllegalStateException if the extensions have not been loaded
     */
    public Set<Bundle> getBundles() {
        if (initContext == null) {
            throw new IllegalStateException("Bundles have not been loaded.");
        }

        return new LinkedHashSet<>(initContext.bundles.values());
    }

}
