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

import org.apache.nifi.NiFiServer;
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
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to initialize the extension and framework classloaders.
 * The core framework should obtain a singleton reference from NarClassLoadersHolder.
 */
public final class NarClassLoaders {

    public static final String FRAMEWORK_NAR_ID = "nifi-framework-nar";
    public static final String JETTY_NAR_ID = "nifi-jetty-nar";

    private volatile InitContext initContext;
    private static final Logger logger = LoggerFactory.getLogger(NarClassLoaders.class);

    @SuppressWarnings("PMD.UnusedPrivateField")
    private final static class InitContext {

        private final File frameworkWorkingDir;
        private final File extensionWorkingDir;
        private final Bundle frameworkBundle;
        private final Bundle jettyBundle;
        private final NiFiServer serverInstance;
        private final Map<String, Bundle> bundles;

        private InitContext(
                final File frameworkDir,
                final File extensionDir,
                final Bundle frameworkBundle,
                final Bundle jettyBundle,
                final NiFiServer serverInstance,
                final Map<String, Bundle> bundles) {
            this.frameworkWorkingDir = frameworkDir;
            this.extensionWorkingDir = extensionDir;
            this.frameworkBundle = frameworkBundle;
            this.jettyBundle = jettyBundle;
            this.serverInstance = serverInstance;
            this.bundles = bundles;

            // Set the nifi.framework.version system property to make the version available
            // via expression language in data flows
            if (frameworkBundle != null) {
                System.setProperty("nifi.framework.version", frameworkBundle.getBundleDetails().getCoordinate().getVersion());
            }
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
    public void init(File frameworkWorkingDir, File extensionsWorkingDir, final String frameworkNarId) throws IOException, ClassNotFoundException {
        init(ClassLoader.getSystemClassLoader(), frameworkWorkingDir, extensionsWorkingDir, frameworkNarId, true);
    }

    public void init(File frameworkWorkingDir, File extensionsWorkingDir) throws IOException, ClassNotFoundException {
        init(frameworkWorkingDir, extensionsWorkingDir, FRAMEWORK_NAR_ID);
    }

    // Default to NiFi's framework NAR ID
    public void init(final ClassLoader rootClassloader,
                     final File frameworkWorkingDir, final File extensionsWorkingDir, final boolean logDetails) throws IOException, ClassNotFoundException {
        init(rootClassloader, frameworkWorkingDir, extensionsWorkingDir, FRAMEWORK_NAR_ID, logDetails);
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
    public void init(final ClassLoader rootClassloader, final File frameworkWorkingDir, final File extensionsWorkingDir,
                     final String frameworkNarId, final boolean logDetails) throws IOException, ClassNotFoundException {
        if (extensionsWorkingDir == null) {
            throw new NullPointerException("cannot have empty arguments");
        }

        InitContext ic = initContext;
        if (ic == null) {
            synchronized (this) {
                ic = initContext;
                if (ic == null) {
                    initContext = ic = load(rootClassloader, frameworkWorkingDir, extensionsWorkingDir, frameworkNarId, logDetails);
                }
            }
        }
    }

    /**
     * Should be called at most once.
     */
    private InitContext load(final ClassLoader rootClassloader, final File frameworkWorkingDir, final File extensionsWorkingDir,
                             final String frameworkNarId, final boolean logDetails)
            throws IOException, ClassNotFoundException {

        // find all nar files and create class loaders for them.
        final Map<String, Bundle> narDirectoryBundleLookup = new LinkedHashMap<>();
        final Map<String, ClassLoader> narCoordinateClassLoaderLookup = new HashMap<>();
        final Map<String, Set<BundleCoordinate>> narIdBundleLookup = new HashMap<>();

        // make sure the nar directory is there and accessible
        final List<File> narWorkingDirContents = new ArrayList<>();

        if (frameworkWorkingDir != null) {
            FileUtils.ensureDirectoryExistAndCanReadAndWrite(frameworkWorkingDir);
            final File[] frameworkWorkingDirContents = frameworkWorkingDir.listFiles();
            if (frameworkWorkingDirContents != null) {
                narWorkingDirContents.addAll(Arrays.asList(frameworkWorkingDirContents));
            }
        }

        FileUtils.ensureDirectoryExistAndCanReadAndWrite(extensionsWorkingDir);
        final File[] extensionsWorkingDirContents = extensionsWorkingDir.listFiles();
        if (extensionsWorkingDirContents != null) {
            narWorkingDirContents.addAll(Arrays.asList(extensionsWorkingDirContents));
        }

        NiFiServer serverInstance = null;
        if (!narWorkingDirContents.isEmpty()) {
            final List<BundleDetails> narDetails = new ArrayList<>();
            final Map<String, String> narCoordinatesToWorkingDir = new HashMap<>();

            // load the nar details which includes and nar dependencies
            for (final File unpackedNar : narWorkingDirContents) {
                BundleDetails narDetail = null;
                try {
                     narDetail = getNarDetails(unpackedNar);
                } catch (IllegalStateException e) {
                    logger.warn("Unable to load NAR {} due to {}, skipping...", unpackedNar.getAbsolutePath(), e.getMessage());
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
                    jettyClassLoader = createNarClassLoader(narDetail.getWorkingDirectory(), rootClassloader, logDetails);

                    // remove the jetty nar since its already loaded
                    narDirectoryBundleLookup.put(narDetail.getWorkingDirectory().getCanonicalPath(), new Bundle(narDetail, jettyClassLoader));
                    narCoordinateClassLoaderLookup.put(narDetail.getCoordinate().getCoordinate(), jettyClassLoader);
                    narDetailsIter.remove();
                }

                // populate bundle lookup
                narIdBundleLookup.computeIfAbsent(narDetail.getCoordinate().getId(), id -> new HashSet<>()).add(narDetail.getCoordinate());
            }

            // Keep track of NiFiServer implementations
            Map<NiFiServer, String> niFiServers = new HashMap<>();
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
                        final ClassLoader parentClassLoader = jettyClassLoader == null ? ClassLoader.getSystemClassLoader() : jettyClassLoader;
                        narClassLoader = createNarClassLoader(narDetail.getWorkingDirectory(), parentClassLoader, logDetails);
                    } else {
                        final String dependencyCoordinateStr = narDependencyCoordinate.getCoordinate();

                        // if the declared dependency has already been loaded
                        if (narCoordinateClassLoaderLookup.containsKey(dependencyCoordinateStr)) {
                            final ClassLoader narDependencyClassLoader = narCoordinateClassLoaderLookup.get(dependencyCoordinateStr);
                            narClassLoader = createNarClassLoader(narDetail.getWorkingDirectory(), narDependencyClassLoader, logDetails);
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
                                        logger.warn("While loading '{}' unable to locate exact NAR dependency '{}'. Only found one possible match '{}'. Continuing...",
                                                narDetail.getCoordinate().getCoordinate(), dependencyCoordinateStr, coordinate.getCoordinate());

                                        final ClassLoader narDependencyClassLoader = narCoordinateClassLoaderLookup.get(coordinate.getCoordinate());
                                        narClassLoader = createNarClassLoader(narDetail.getWorkingDirectory(), narDependencyClassLoader, logDetails);
                                    }
                                }
                            }
                        }
                    }

                    // if we were able to create the nar class loader, store it and remove the details
                    final ClassLoader bundleClassLoader = narClassLoader;
                    if (bundleClassLoader != null) {
                        narDirectoryBundleLookup.put(narDetail.getWorkingDirectory().getCanonicalPath(), new Bundle(narDetail, bundleClassLoader));
                        String coordinate = narDetail.getCoordinate().getCoordinate();
                        narCoordinateClassLoaderLookup.put(coordinate, narClassLoader);
                        narDetailsIter.remove();
                        // Search for a NiFiServer implementation
                        ServiceLoader<NiFiServer> niFiServerServiceLoader = ServiceLoader.load(NiFiServer.class, narClassLoader);
                        for (NiFiServer server : niFiServerServiceLoader) {
                            niFiServers.put(server, coordinate);
                        }
                    }
                }
                // attempt to load more if some were successfully loaded this iteration
            } while (narCount != narDetails.size());

            // Ensure exactly one NiFiServer implementation, otherwise report none or multiples found
            if (niFiServers.isEmpty()) {
                serverInstance = null;
            } else if (niFiServers.size() > 1) {
                String sb = "Expected exactly one implementation of NiFiServer but found " + niFiServers.size() + ": " +
                        niFiServers.entrySet().stream().map((entry) -> entry.getKey().getClass().getName() + " from " + entry.getValue()).collect(Collectors.joining(", "));
                throw new IOException(sb);
            } else {
                Map.Entry<NiFiServer, String> nifiServer = niFiServers.entrySet().iterator().next();
                serverInstance = nifiServer.getKey();
                logger.info("Found NiFiServer implementation {} in {}", serverInstance.getClass().getName(), nifiServer.getValue());
            }

            // see if any nars couldn't be loaded
            for (final BundleDetails narDetail : narDetails) {
                logger.warn("Unable to resolve required dependency '{}'. Skipping NAR '{}'", narDetail.getDependencyCoordinate().getId(), narDetail.getWorkingDirectory().getAbsolutePath());
            }
        }

        // find the framework bundle, NarUnpacker already checked that there was a framework NAR and that there was only one
        final Bundle frameworkBundle = narDirectoryBundleLookup.values().stream()
                .filter(b -> b.getBundleDetails().getCoordinate().getId().equals(frameworkNarId))
                .findFirst().orElse(null);

        // find the Jetty bundle
        final Bundle jettyBundle = narDirectoryBundleLookup.values().stream()
                .filter(b -> b.getBundleDetails().getCoordinate().getId().equals(JETTY_NAR_ID))
                .findFirst().orElse(null);

        return new InitContext(frameworkWorkingDir, extensionsWorkingDir, frameworkBundle, jettyBundle, serverInstance, new LinkedHashMap<>(narDirectoryBundleLookup));
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
        final Map<String, Set<BundleCoordinate>> bundleIdToCoordinatesLookup = new HashMap<>();

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
                    final ClassLoader bundleClassLoader = createBundleClassLoader(bundleDetail, bundleIdToCoordinatesLookup, true);
                    if (bundleClassLoader != null) {
                        final Bundle bundle = new Bundle(bundleDetail, bundleClassLoader);
                        loadedBundles.add(bundle);
                        additionalBundleDetailsIter.remove();

                        // Need to add to overall bundles as we go so if other NARs depend on this one we can find it
                        initContext.bundles.put(bundleDetail.getWorkingDirectory().getCanonicalPath(), bundle);
                    }
                } catch (final Exception e) {
                    logger.error("Unable to load NAR {} due to {}, skipping...", bundleDetail.getWorkingDirectory(), e.getMessage());
                }
            }

            // Attempt to load more if some were successfully loaded this iteration
        } while (bundleCount != additionalBundleDetails.size());

        // See if any bundles couldn't be loaded
        final Set<BundleDetails> skippedBundles = new HashSet<>();
        for (final BundleDetails bundleDetail : additionalBundleDetails) {
            logger.warn("Unable to resolve required dependency '{}'. Skipping NAR '{}'", bundleDetail.getDependencyCoordinate().getId(), bundleDetail.getWorkingDirectory().getAbsolutePath());
            skippedBundles.add(bundleDetail);
        }

        return new NarLoadResult(loadedBundles, skippedBundles);
    }

    private ClassLoader createBundleClassLoader(final BundleDetails bundleDetail, final Map<String, Set<BundleCoordinate>> bundleIdToCoordinatesLookup, final boolean logDetails)
            throws IOException, ClassNotFoundException {

        ClassLoader bundleClassLoader = null;

        final BundleCoordinate bundleDependencyCoordinate = bundleDetail.getDependencyCoordinate();
        if (bundleDependencyCoordinate == null) {
            final ClassLoader parentClassLoader;
            Bundle jettyBundle = getJettyBundle();
            if (jettyBundle != null) {
                parentClassLoader = jettyBundle.getClassLoader();
            } else {
                // If there is no Jetty bundle, assume to be "headless"
                parentClassLoader = null;
            }
            bundleClassLoader = createNarClassLoader(bundleDetail.getWorkingDirectory(), parentClassLoader, logDetails);
        } else {
            final Optional<Bundle> dependencyBundle = getBundle(bundleDependencyCoordinate);

            // If the declared dependency has already been loaded then use it
            if (dependencyBundle.isPresent()) {
                final ClassLoader narDependencyClassLoader = dependencyBundle.get().getClassLoader();
                bundleClassLoader = createNarClassLoader(bundleDetail.getWorkingDirectory(), narDependencyClassLoader, logDetails);
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
                            logger.warn("While loading '{}' unable to locate exact NAR dependency '{}'. Only found one possible match '{}'. Continuing...",
                                    bundleDetail.getCoordinate().getCoordinate(), dependencyCoordinateStr, coordinate.getCoordinate());

                            final ClassLoader narDependencyClassLoader = matchingDependencyIdBundle.get().getClassLoader();
                            bundleClassLoader = createNarClassLoader(bundleDetail.getWorkingDirectory(), narDependencyClassLoader, logDetails);
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

                    logger.error("Unable to load NAR with coordinates {} and working directory {} because another NAR with the same coordinates already exists at {}",
                            unpackedNarCoordinate, unpackedNarWorkingDir, existingNarWorkingDir);
                } else {
                    narDetails.add(narDetail);
                }

            } catch (Exception e) {
                logger.error("Unable to load NAR {} due to {}, skipping...", unpackedNar.getAbsolutePath(), e.getMessage());
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
    private static ClassLoader createNarClassLoader(final File narDirectory, final ClassLoader parentClassLoader, final boolean log) throws IOException, ClassNotFoundException {
        logger.debug("Loading NAR file: {}", narDirectory.getAbsolutePath());
        final ClassLoader narClassLoader = new NarClassLoader(narDirectory, parentClassLoader);

        if (log) {
            logger.info("Loaded NAR file: {} as class loader {}", narDirectory.getAbsolutePath(), narClassLoader);
        } else {
            logger.debug("Loaded NAR file: {} as class loader {}", narDirectory.getAbsolutePath(), narClassLoader);
        }

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
     * Removes the given bundle from the init context.
     *
     * @param bundle the bundle to remove
     */
    public void removeBundle(final Bundle bundle) {
        try {
            initContext.bundles.remove(bundle.getBundleDetails().getWorkingDirectory().getCanonicalPath());
        } catch (final Exception e) {
            logger.warn("Failed to remove bundle [{}]", bundle.getBundleDetails().getCoordinate(), e);
        }
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
     * @return the Server class implementation (NiFi Web/UI or MiNiFi, e.g.)
     *
     * @throws IllegalStateException if the server Bundle has not been loaded
     */
    public NiFiServer getServer() {
        if (initContext == null) {
            throw new IllegalStateException("Server bundle has not been loaded.");
        }

        return initContext.serverInstance;
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
            if (logger.isDebugEnabled()) {
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
