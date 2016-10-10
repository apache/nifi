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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import org.apache.nifi.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A singleton class used to initialize the extension and framework
 * classloaders.
 */
public final class NarClassLoaders {

    public static final String FRAMEWORK_NAR_ID = "nifi-framework-nar";
    public static final String JETTY_NAR_ID = "nifi-jetty-bundle";

    private static volatile NarClassLoaders ncl;
    private volatile InitContext initContext;
    private static final Logger logger = LoggerFactory.getLogger(NarClassLoaders.class);

    private final static class InitContext {

        private final File frameworkWorkingDir;
        private final File extensionWorkingDir;
        private final ClassLoader frameworkClassLoader;
        private final Map<String, ClassLoader> extensionClassLoaders;

        private InitContext(
                final File frameworkDir,
                final File extensionDir,
                final ClassLoader frameworkClassloader,
                final Map<String, ClassLoader> extensionClassLoaders) {
            this.frameworkWorkingDir = frameworkDir;
            this.extensionWorkingDir = extensionDir;
            this.frameworkClassLoader = frameworkClassloader;
            this.extensionClassLoaders = extensionClassLoaders;
        }
    }

    private NarClassLoaders() {
    }

    /**
     * @return The singleton instance of the NarClassLoaders
     */
    public static NarClassLoaders getInstance() {
        NarClassLoaders result = ncl;
        if (result == null) {
            synchronized (NarClassLoaders.class) {
                result = ncl;
                if (result == null) {
                    ncl = result = new NarClassLoaders();
                }
            }
        }
        return result;
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
    public void init(final File frameworkWorkingDir, final File extensionsWorkingDir) throws IOException, ClassNotFoundException {
        if (frameworkWorkingDir == null || extensionsWorkingDir == null) {
            throw new NullPointerException("cannot have empty arguments");
        }
        InitContext ic = initContext;
        if (ic == null) {
            synchronized (this) {
                ic = initContext;
                if (ic == null) {
                    initContext = ic = load(frameworkWorkingDir, extensionsWorkingDir);
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
    private InitContext load(final File frameworkWorkingDir, final File extensionsWorkingDir) throws IOException, ClassNotFoundException {
        // get the system classloader
        final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();

        // find all nar files and create class loaders for them.
        final Map<String, ClassLoader> extensionDirectoryClassLoaderLookup = new LinkedHashMap<>();
        final Map<String, ClassLoader> narIdClassLoaderLookup = new HashMap<>();

        // make sure the nar directory is there and accessible
        FileUtils.ensureDirectoryExistAndCanAccess(frameworkWorkingDir);
        FileUtils.ensureDirectoryExistAndCanAccess(extensionsWorkingDir);

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
            final List<NarDetails> narDetails = new ArrayList<>();

            // load the nar details which includes and nar dependencies
            for (final File unpackedNar : narWorkingDirContents) {
                final NarDetails narDetail = getNarDetails(unpackedNar);

                // ensure the nar contained an identifier
                if (narDetail.getNarId() == null) {
                    logger.warn("No NAR Id found. Skipping: " + unpackedNar.getAbsolutePath());
                    continue;
                }

                // store the nar details
                narDetails.add(narDetail);
            }

            // attempt to locate the jetty nar
            ClassLoader jettyClassLoader = null;
            for (final Iterator<NarDetails> narDetailsIter = narDetails.iterator(); narDetailsIter.hasNext();) {
                final NarDetails narDetail = narDetailsIter.next();

                // look for the jetty nar
                if (JETTY_NAR_ID.equals(narDetail.getNarId())) {
                    // create the jetty classloader
                    jettyClassLoader = createNarClassLoader(narDetail.getNarWorkingDirectory(), systemClassLoader);

                    // remove the jetty nar since its already loaded
                    narIdClassLoaderLookup.put(narDetail.getNarId(), jettyClassLoader);
                    narDetailsIter.remove();
                    break;
                }
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
                for (final Iterator<NarDetails> narDetailsIter = narDetails.iterator(); narDetailsIter.hasNext();) {
                    final NarDetails narDetail = narDetailsIter.next();
                    final String narDependencies = narDetail.getNarDependencyId();

                    // see if this class loader is eligible for loading
                    ClassLoader narClassLoader = null;
                    if (narDependencies == null) {
                        narClassLoader = createNarClassLoader(narDetail.getNarWorkingDirectory(), jettyClassLoader);
                    } else if (narIdClassLoaderLookup.containsKey(narDetail.getNarDependencyId())) {
                        narClassLoader = createNarClassLoader(narDetail.getNarWorkingDirectory(), narIdClassLoaderLookup.get(narDetail.getNarDependencyId()));
                    }

                    // if we were able to create the nar class loader, store it and remove the details
                    if (narClassLoader != null) {
                        extensionDirectoryClassLoaderLookup.put(narDetail.getNarWorkingDirectory().getCanonicalPath(), narClassLoader);
                        narIdClassLoaderLookup.put(narDetail.getNarId(), narClassLoader);
                        narDetailsIter.remove();
                    }
                }

                // attempt to load more if some were successfully loaded this iteration
            } while (narCount != narDetails.size());

            // see if any nars couldn't be loaded
            for (final NarDetails narDetail : narDetails) {
                logger.warn(String.format("Unable to resolve required dependency '%s'. Skipping NAR %s", narDetail.getNarDependencyId(), narDetail.getNarWorkingDirectory().getAbsolutePath()));
            }
        }

        return new InitContext(frameworkWorkingDir, extensionsWorkingDir, narIdClassLoaderLookup.get(FRAMEWORK_NAR_ID), new LinkedHashMap<>(extensionDirectoryClassLoaderLookup));
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
    private static NarDetails getNarDetails(final File narDirectory) throws IOException {
        final NarDetails narDetails = new NarDetails();
        narDetails.setNarWorkingDirectory(narDirectory);

        final File manifestFile = new File(narDirectory, "META-INF/MANIFEST.MF");
        try (final FileInputStream fis = new FileInputStream(manifestFile)) {
            final Manifest manifest = new Manifest(fis);
            final Attributes attributes = manifest.getMainAttributes();

            // get the nar details
            narDetails.setNarId(attributes.getValue("Nar-Id"));
            narDetails.setNarDependencyId(attributes.getValue("Nar-Dependency-Id"));
        }

        return narDetails;
    }

    /**
     * @return the framework class loader
     *
     * @throws IllegalStateException if the frame class loader has not been
     * loaded
     */
    public ClassLoader getFrameworkClassLoader() {
        if (initContext == null) {
            throw new IllegalStateException("Framework class loader has not been loaded.");
        }

        return initContext.frameworkClassLoader;
    }

    /**
     * @param extensionWorkingDirectory the directory
     * @return the class loader for the specified working directory. Returns
     * null when no class loader exists for the specified working directory
     * @throws IllegalStateException if the class loaders have not been loaded
     */
    public ClassLoader getExtensionClassLoader(final File extensionWorkingDirectory) {
        if (initContext == null) {
            throw new IllegalStateException("Extensions class loaders have not been loaded.");
        }

        try {
            return initContext.extensionClassLoaders.get(extensionWorkingDirectory.getCanonicalPath());
        } catch (final IOException ioe) {
            if(logger.isDebugEnabled()){
                logger.debug("Unable to get extension classloader for working directory '{}'", extensionWorkingDirectory);
            }
            return null;
        }
    }

    /**
     * @return the extension class loaders
     * @throws IllegalStateException if the class loaders have not been loaded
     */
    public Set<ClassLoader> getExtensionClassLoaders() {
        if (initContext == null) {
            throw new IllegalStateException("Extensions class loaders have not been loaded.");
        }

        return new LinkedHashSet<>(initContext.extensionClassLoaders.values());
    }

    private static class NarDetails {

        private String narId;
        private String narDependencyId;
        private File narWorkingDirectory;

        public String getNarDependencyId() {
            return narDependencyId;
        }

        public void setNarDependencyId(String narDependencyId) {
            this.narDependencyId = narDependencyId;
        }

        public String getNarId() {
            return narId;
        }

        public void setNarId(String narId) {
            this.narId = narId;
        }

        public File getNarWorkingDirectory() {
            return narWorkingDirectory;
        }

        public void setNarWorkingDirectory(File narWorkingDirectory) {
            this.narWorkingDirectory = narWorkingDirectory;
        }
    }

}
