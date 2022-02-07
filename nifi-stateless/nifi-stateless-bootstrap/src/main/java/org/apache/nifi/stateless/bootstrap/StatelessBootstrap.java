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

package org.apache.nifi.stateless.bootstrap;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.nar.NarClassLoader;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.stateless.config.ParameterOverride;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.engine.NarUnpackLock;
import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.stateless.flow.DataflowDefinitionParser;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.StatelessDataflowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;

public class StatelessBootstrap {
    private static final Logger logger = LoggerFactory.getLogger(StatelessBootstrap.class);
    private static final Pattern STATELESS_NAR_PATTERN = Pattern.compile("nifi-stateless-nar-.*\\.nar-unpacked");
    private final ClassLoader engineClassLoader;
    private final ClassLoader extensionClassLoader;
    private final StatelessEngineConfiguration engineConfiguration;

    private StatelessBootstrap(final ClassLoader engineClassLoader, final ClassLoader extensionClassLoader, final StatelessEngineConfiguration engineConfiguration) {
        this.engineClassLoader = engineClassLoader;
        this.extensionClassLoader = extensionClassLoader;
        this.engineConfiguration = engineConfiguration;
    }

    public StatelessDataflow createDataflow(final DataflowDefinition dataflowDefinition)
                throws IOException, StatelessConfigurationException {
        final StatelessDataflowFactory dataflowFactory = getSingleInstance(engineClassLoader, StatelessDataflowFactory.class);
        final StatelessDataflow dataflow = dataflowFactory.createDataflow(engineConfiguration, dataflowDefinition, extensionClassLoader);
        return dataflow;
    }

    public DataflowDefinition parseDataflowDefinition(final File flowDefinitionFile, final List<ParameterOverride> parameterOverrides)
                throws StatelessConfigurationException, IOException {
        final DataflowDefinitionParser dataflowDefinitionParser = getSingleInstance(engineClassLoader, DataflowDefinitionParser.class);
        final DataflowDefinition dataflowDefinition = dataflowDefinitionParser.parseFlowDefinition(flowDefinitionFile, engineConfiguration, parameterOverrides);
        return dataflowDefinition;
    }

    public DataflowDefinition parseDataflowDefinition(final Map<String, String> flowDefinitionProperties, final List<ParameterOverride> parameterOverrides)
                throws StatelessConfigurationException, IOException {
        final DataflowDefinitionParser dataflowDefinitionParser = getSingleInstance(engineClassLoader, DataflowDefinitionParser.class);
        final DataflowDefinition dataflowDefinition = dataflowDefinitionParser.parseFlowDefinition(flowDefinitionProperties, engineConfiguration, parameterOverrides);
        return dataflowDefinition;
    }

    public static StatelessBootstrap bootstrap(final StatelessEngineConfiguration engineConfiguration) throws IOException {
        return bootstrap(engineConfiguration, ClassLoader.getSystemClassLoader());
    }

    public static StatelessBootstrap bootstrap(final StatelessEngineConfiguration engineConfiguration, final ClassLoader rootClassLoader) throws IOException {
        final File narDirectory = engineConfiguration.getNarDirectory();
        final File workingDirectory = engineConfiguration.getWorkingDirectory();
        final File narExpansionDirectory = new File(workingDirectory, "nar");

        // Ensure working directory exists, creating it if necessary
        if (!narExpansionDirectory.exists() && !narExpansionDirectory.mkdirs()) {
            throw new IOException("Working Directory " + narExpansionDirectory + " does not exist and could not be created");
        }

        final Bundle systemBundle = SystemBundle.create(narDirectory.getAbsolutePath(), ClassLoader.getSystemClassLoader());
        final File frameworkWorkingDir = new File(narExpansionDirectory, "framework");
        final File extensionsWorkingDir = new File(narExpansionDirectory, "extensions");
        final List<Path> narDirectories = Collections.singletonList(narDirectory.toPath());

        // Unpack NARs
        final long unpackStart = System.currentTimeMillis();
        final Predicate<BundleCoordinate> narFilter = coordinate -> true;
        NarUnpackLock.lock();
        try {
            NarUnpacker.unpackNars(systemBundle, frameworkWorkingDir, extensionsWorkingDir, null, narDirectories, false, NarClassLoaders.FRAMEWORK_NAR_ID, false, false, narFilter);
        } finally {
            NarUnpackLock.unlock();
        }
        final long unpackMillis = System.currentTimeMillis() - unpackStart;
        logger.info("Unpacked NAR files in {} millis", unpackMillis);

        final BlockListClassLoader statelessClassLoader = createExtensionRootClassLoader(narDirectory, rootClassLoader);

        final File statelessNarWorkingDir = locateStatelessNarWorkingDirectory(extensionsWorkingDir);
        final NarClassLoader engineClassLoader;
        try {
            engineClassLoader = new NarClassLoader(statelessNarWorkingDir, statelessClassLoader);
        } catch (final ClassNotFoundException e) {
            throw new IOException("Could not create NarClassLoader for Stateless NAR located at " + statelessNarWorkingDir.getAbsolutePath(), e);
        }

        Thread.currentThread().setContextClassLoader(engineClassLoader);
        return new StatelessBootstrap(engineClassLoader, statelessClassLoader, engineConfiguration);
    }

    /**
     * Creates a ClassLoader that is to be used as the 'root'/parent for all NiFi Extensions' ClassLoaders. The ClassLoader will inherit from its parent
     * any classes that exist in JAR files that can be found in the given NAR Directory. However, it will not allow any other classes to be loaded from the parent.
     * This approach is important because we need to ensure that the ClassLoader that is provided to extensions when run from NiFi Stateless is the same as the ClassLoader
     * that will be provided to it in traditional NiFi. Whereas in traditional NiFi, we have the ability to control the System ClassLoader, Stateless NiFi is designed to be
     * embedded, so we cannot control the System ClassLoader of the embedding application. This gives us a way to ensure that we control what is available to Extensions and
     * still provides us the ability to load the necessary classes from the System ClassLoader, which prevents ClassCastExceptions that might otherwise occur if we were to
     * load the same classes from another ClassLoader.
     *
     * @param narDirectory the NAR directory whose .jar files should be made available via the parent.
     * @param parent the parent class loader that the given BlockListClassLoader should delegate to for classes that it does not block
     * @return a BlockListClassLoader that allows only the appropriate classes to be loaded from the given parent
     */
    private static BlockListClassLoader createExtensionRootClassLoader(final File narDirectory, final ClassLoader parent) throws IOException {
        if (!(parent instanceof URLClassLoader)) {
            return new BlockListClassLoader(parent, Collections.emptySet());
        }

        final File[] narDirectoryFiles = narDirectory.listFiles();
        if (narDirectoryFiles == null) {
            throw new IOException("Could not get a listing of the NAR directory");
        }

        logger.debug("NAR directory used to find files to allow being loaded by Stateless Extension Classloaders from parent {}: {}", parent, narDirectory);

        final Set<URL> urls = new HashSet<>();
        findClassLoaderUrls(parent, urls);

        final Set<String> classesBlocked = new HashSet<>();
        final Set<String> filesBlocked = new HashSet<>();
        findClassNamesInJars(urls, classesBlocked, filesBlocked);

        final Set<String> classesAllowed = new HashSet<>();
        final Set<String> filesAllowed = new HashSet<>();
        for (final File file : narDirectoryFiles) {
            findClassNamesInJar(file, classesAllowed);
            filesAllowed.add(file.getName());
        }

        final File java11Directory = new File(narDirectory, "java11");
        final File[] java11DirectoryFiles = java11Directory.listFiles();
        if (java11DirectoryFiles != null) {
            for (final File file : java11DirectoryFiles) {
                findClassNamesInJar(file, classesAllowed);
                filesAllowed.add(file.getName());
            }
        }
        logger.debug("The following JAR files are proposed to be blocked from being loaded by Stateless Extensions ClassLoaders from parent {}: {}", parent, filesBlocked);
        logger.debug("Of the full list above, the following JAR files will be explicitly allowed to be loaded by Stateless Extensions ClassLoaders from parent {}: {}", parent, filesAllowed);

        classesBlocked.removeAll(classesAllowed);
        filesBlocked.removeAll(filesAllowed);
        logger.debug("The final list of JAR files blocked from being loaded by Stateless Extensions ClassLoaders from parent {}: {}", parent, filesBlocked);

        logger.debug("The final list of classes blocked from being loaded by Stateless Extension ClassLoaders from parent {}: {}", parent, classesBlocked);

        final BlockListClassLoader blockingClassLoader = new BlockListClassLoader(parent, classesBlocked);
        return blockingClassLoader;
    }

    private static void findClassNamesInJars(final Collection<URL> jarUrls, final Set<String> classesFound, final Set<String> jarFilesFound) throws IOException {
        for (final URL url : jarUrls) {
            final File file;
            try {
                file = new File(url.toURI());
            } catch (URISyntaxException e) {
                logger.warn("Could not find file for {} in classpath", url);
                continue;
            }

            findClassNamesInJar(file, classesFound);
            jarFilesFound.add(file.getName());
        }
    }

    private static void findClassLoaderUrls(final ClassLoader classLoader, final Set<URL> urls) {
        if (classLoader == null) {
            return;
        }

        if (classLoader instanceof URLClassLoader) {
            final URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            urls.addAll(Arrays.asList(urlClassLoader.getURLs()));
        }

        // If the classLoader is the system class loader, we are done. We don't want to process the parent of
        // the system class loader (which would be the Launcher$ExtClassLoader that contains the JDK/JRE classes, etc)
        if (classLoader != ClassLoader.getSystemClassLoader()) {
            findClassLoaderUrls(classLoader.getParent(), urls);
        }
    }

    private static void findClassNamesInJar(final File file, final Set<String> classNames) throws IOException {
        if (!file.getName().endsWith(".jar") || !file.isFile() || !file.exists()) {
            return;
        }

        final JarFile jarFile = new JarFile(file);
        final Enumeration<? extends ZipEntry> enumeration = jarFile.entries();
        while (enumeration.hasMoreElements()) {
            final ZipEntry zipEntry = enumeration.nextElement();
            final String entryName = zipEntry.getName();

            if (entryName.endsWith(".class")) {
                final int lastIndex = entryName.lastIndexOf(".class");
                final String className = entryName.substring(0, lastIndex).replace("/", ".");
                classNames.add(className);
            }
        }
    }

    private static File locateStatelessNarWorkingDirectory(final File workingDirectory) throws IOException {
        final File[] files = workingDirectory.listFiles();
        if (files == null) {
            throw new IOException("Could not read contents of working directory " + workingDirectory);
        }

        final List<File> matching = new ArrayList<>();
        for (final File file : files) {
            final String filename = file.getName();
            if (STATELESS_NAR_PATTERN.matcher(filename).matches()) {
                matching.add(file);
            }
        }

        if (matching.isEmpty()) {
            throw new IOException("Could not find NiFi Stateless NAR in working directory " + workingDirectory);
        }
        if (matching.size() > 1) {
            throw new IOException("Found multiple NiFi Stateless NARs in working directory " + workingDirectory + ": " + matching);
        }

        return matching.get(0);
    }

    private static <T> T getSingleInstance(final ClassLoader classLoader, final Class<T> type) {
        final ServiceLoader<T> serviceLoader = ServiceLoader.load(type, classLoader);

        T instance = null;
        for (final T object : serviceLoader) {
            if (instance == null) {
                instance = object;
            } else {
                throw new IllegalStateException("Found multiple implementations of " + type);
            }
        }

        if (instance == null) {
            throw new IllegalStateException("Could not find any implementations of " + type);
        }

        return instance;
    }
}
