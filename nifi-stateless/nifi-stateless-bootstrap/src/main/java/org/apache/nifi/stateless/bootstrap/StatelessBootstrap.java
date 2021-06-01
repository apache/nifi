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
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class StatelessBootstrap {
    private static final Logger logger = LoggerFactory.getLogger(StatelessBootstrap.class);
    private static final Pattern STATELESS_NAR_PATTERN = Pattern.compile("nifi-stateless-nar-.*\\.nar-unpacked");
    private static final String NIFI_GROUP = "org.apache.nifi";
    private static final String NIFI_STATELESS_ARTIFACT_ID = "nifi-stateless-nar";
    private static final String NIFI_JETTY_ARTIFACT_ID = "nifi-jetty-bundle";
    private final ClassLoader statelessClassLoader;
    private final StatelessEngineConfiguration engineConfiguration;

    private StatelessBootstrap(final ClassLoader statelessClassLoader, final StatelessEngineConfiguration engineConfiguration) {
        this.statelessClassLoader = statelessClassLoader;
        this.engineConfiguration = engineConfiguration;
    }

    public <T> StatelessDataflow createDataflow(final DataflowDefinition<T> dataflowDefinition)
                throws IOException, StatelessConfigurationException {
        final StatelessDataflowFactory<T> dataflowFactory = getSingleInstance(statelessClassLoader, StatelessDataflowFactory.class);
        final StatelessDataflow dataflow = dataflowFactory.createDataflow(engineConfiguration, dataflowDefinition);
        return dataflow;
    }

    public DataflowDefinition<?> parseDataflowDefinition(final File flowDefinitionFile, final List<ParameterOverride> parameterOverrides)
                throws StatelessConfigurationException, IOException {
        final DataflowDefinitionParser dataflowDefinitionParser = getSingleInstance(statelessClassLoader, DataflowDefinitionParser.class);
        final DataflowDefinition<?> dataflowDefinition = dataflowDefinitionParser.parseFlowDefinition(flowDefinitionFile, engineConfiguration, parameterOverrides);
        return dataflowDefinition;
    }

    public DataflowDefinition<?> parseDataflowDefinition(final Map<String, String> flowDefinitionProperties, final List<ParameterOverride> parameterOverrides)
                throws StatelessConfigurationException, IOException {
        final DataflowDefinitionParser dataflowDefinitionParser = getSingleInstance(statelessClassLoader, DataflowDefinitionParser.class);
        final DataflowDefinition<?> dataflowDefinition = dataflowDefinitionParser.parseFlowDefinition(flowDefinitionProperties, engineConfiguration, parameterOverrides);
        return dataflowDefinition;
    }

    public static StatelessBootstrap bootstrap(final StatelessEngineConfiguration engineConfiguration) throws IOException {
        return bootstrap(engineConfiguration, ClassLoader.getSystemClassLoader());
    }

    public static StatelessBootstrap bootstrap(final StatelessEngineConfiguration engineConfiguration, final ClassLoader rootClassLoader) throws IOException {
        final File narDirectory = engineConfiguration.getNarDirectory();
        final File workingDirectory = engineConfiguration.getWorkingDirectory();

        // Ensure working directory exists, creating it if necessary
        if (!workingDirectory.exists() && !workingDirectory.mkdirs()) {
            throw new IOException("Working Directory " + workingDirectory + " does not exist and could not be created");
        }

        final Bundle systemBundle = SystemBundle.create(narDirectory.getAbsolutePath(), ClassLoader.getSystemClassLoader());
        final File frameworkWorkingDir = new File(workingDirectory, "nifi-framework");
        final File extensionsWorkingDir = new File(workingDirectory, "extensions");
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

        final File statelessNarWorkingDir = locateStatelessNarWorkingDirectory(extensionsWorkingDir);
        final File statelessNarInf = new File(statelessNarWorkingDir, "NAR-INF");
        final File statelessNarDependencies = new File(statelessNarInf, "bundled-dependencies");
        final File[] statelessNarContents = statelessNarDependencies.listFiles();
        if (statelessNarContents == null || statelessNarContents.length == 0) {
            throw new IOException("Could not access contents of Stateless NAR dependencies at " + statelessNarDependencies);
        }

        final URL[] urls = new URL[statelessNarContents.length];
        for (int i=0; i < statelessNarContents.length; i++) {
            final File dependency = statelessNarContents[i];
            final URL url = dependency.toURI().toURL();
            urls[i] = url;
        }

        final URLClassLoader statelessClassLoader = new URLClassLoader(urls, rootClassLoader);
        Thread.currentThread().setContextClassLoader(statelessClassLoader);
        return new StatelessBootstrap(statelessClassLoader, engineConfiguration);
    }

    private static boolean isRequiredForBootstrap(final BundleCoordinate coordinate) {
        final String group = coordinate.getGroup();
        if (!NIFI_GROUP.equals(group)) {
            return false;
        }

        final String artifactId = coordinate.getId();
        return NIFI_JETTY_ARTIFACT_ID.equals(artifactId) || NIFI_STATELESS_ARTIFACT_ID.equals(artifactId);
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
