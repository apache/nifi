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

package org.apache.nifi.kafka.connect;

import org.apache.nifi.stateless.bootstrap.StatelessBootstrap;
import org.apache.nifi.stateless.config.ExtensionClientDefinition;
import org.apache.nifi.stateless.config.ParameterOverride;
import org.apache.nifi.stateless.config.SslContextDefinition;
import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

public class StatelessKafkaConnectorUtil {
    private static final String UNKNOWN_VERSION = "<Unable to determine Stateless NiFi Kafka Connector Version>";
    private static final Logger logger = LoggerFactory.getLogger(StatelessKafkaConnectorUtil.class);
    private static final Lock unpackNarLock = new ReentrantLock();
    protected static final Pattern STATELESS_BOOTSTRAP_FILE_PATTERN = Pattern.compile("nifi-stateless-bootstrap-(.*).jar");

    public static String getVersion() {
        final File bootstrapJar = detectBootstrapJar();
        if (bootstrapJar == null) {
            return UNKNOWN_VERSION;
        }

        try (final JarFile jarFile = new JarFile(bootstrapJar)) {
            final Manifest manifest = jarFile.getManifest();
            if (manifest != null) {
                return manifest.getMainAttributes().getValue("Implementation-Version");
            }
        } catch (IOException e) {
            logger.warn("Could not determine Version of NiFi Stateless Kafka Connector", e);
            return UNKNOWN_VERSION;
        }

        return UNKNOWN_VERSION;
    }

    public static StatelessDataflow createDataflow(final StatelessNiFiCommonConfig config) {
        final StatelessEngineConfiguration engineConfiguration = createEngineConfiguration(config);

        final List<ParameterOverride> parameterOverrides = config.getParameterOverrides();
        final String dataflowName = config.getDataflowName();

        final DataflowDefinition dataflowDefinition;
        final StatelessBootstrap bootstrap;
        try {
            final Map<String, String> dataflowDefinitionProperties = new HashMap<>();
            config.setFlowDefinition(dataflowDefinitionProperties);
            dataflowDefinitionProperties.put(StatelessNiFiCommonConfig.BOOTSTRAP_FLOW_NAME, dataflowName);
            MDC.setContextMap(Collections.singletonMap("dataflow", dataflowName));
            StatelessDataflow dataflow;

            // Use a Write Lock to ensure that only a single thread is calling StatelessBootstrap.bootstrap().
            // We do this because the bootstrap() method will expand all NAR files into the working directory.
            // If we have multiple Connector instances, or multiple tasks, we don't want several threads all
            // unpacking NARs at the same time, as it could potentially result in the working directory becoming corrupted.
            unpackNarLock.lock();
            try {
                WorkingDirectoryUtils.reconcileWorkingDirectory(engineConfiguration.getWorkingDirectory());

                bootstrap = StatelessBootstrap.bootstrap(engineConfiguration, StatelessNiFiSourceTask.class.getClassLoader());

                dataflowDefinition = bootstrap.parseDataflowDefinition(dataflowDefinitionProperties, parameterOverrides);
                dataflow = bootstrap.createDataflow(dataflowDefinition);
            } finally {
                unpackNarLock.unlock();
            }
            return dataflow;
        } catch (final Exception e) {
            throw new RuntimeException("Failed to bootstrap Stateless NiFi Engine", e);
        }
    }

    private static StatelessEngineConfiguration createEngineConfiguration(final StatelessNiFiCommonConfig config) {
        final File narDirectory;
        final String narDirectoryFilename = config.getNarDirectory();
        if (narDirectoryFilename == null) {
            narDirectory = detectNarDirectory();
        } else {
            narDirectory = new File(narDirectoryFilename);
        }

        final String dataflowName = config.getDataflowName();

        final File baseWorkingDirectory;
        final String workingDirectoryFilename = config.getWorkingDirectory();
        if (workingDirectoryFilename == null) {
            baseWorkingDirectory = StatelessNiFiCommonConfig.DEFAULT_WORKING_DIRECTORY;
        } else {
            baseWorkingDirectory = new File(workingDirectoryFilename);
        }
        final File workingDirectory = new File(baseWorkingDirectory, dataflowName);

        final File extensionsDirectory;
        final String extensionsDirectoryFilename = config.getExtensionsDirectory();
        if (extensionsDirectoryFilename == null) {
            extensionsDirectory = StatelessNiFiCommonConfig.DEFAULT_EXTENSIONS_DIRECTORY;
        } else {
            extensionsDirectory = new File(extensionsDirectoryFilename);
        }

        final SslContextDefinition sslContextDefinition = createSslContextDefinition(config);

        return new StatelessEngineConfiguration() {
            @Override
            public File getWorkingDirectory() {
                return workingDirectory;
            }

            @Override
            public File getNarDirectory() {
                return narDirectory;
            }

            @Override
            public File getExtensionsDirectory() {
                return extensionsDirectory;
            }

            @Override
            public Collection<File> getReadOnlyExtensionsDirectories() {
                return Collections.emptyList();
            }

            @Override
            public File getKrb5File() {
                return new File(config.getKrb5File());
            }

            @Override
            public Optional<File> getContentRepositoryDirectory() {
                return Optional.empty();
            }

            @Override
            public SslContextDefinition getSslContext() {
                return sslContextDefinition;
            }

            @Override
            public String getSensitivePropsKey() {
                return config.getSensitivePropsKey();
            }

            @Override
            public List<ExtensionClientDefinition> getExtensionClients() {
                final List<ExtensionClientDefinition> extensionClientDefinitions = new ArrayList<>();

                final String nexusBaseUrl = config.getNexusBaseUrl();
                if (nexusBaseUrl != null) {
                    final ExtensionClientDefinition definition = new ExtensionClientDefinition();
                    definition.setUseSslContext(false);
                    definition.setExtensionClientType("nexus");
                    definition.setCommsTimeout("30 secs");
                    definition.setBaseUrl(nexusBaseUrl);
                    extensionClientDefinitions.add(definition);
                }

                return extensionClientDefinitions;
            }

            @Override
            public String getStatusTaskInterval() {
                return "1 min";
            }
        };
    }

    private static SslContextDefinition createSslContextDefinition(final StatelessNiFiCommonConfig config) {
        final String truststoreFile = config.getTruststoreFile();
        if (truststoreFile == null || truststoreFile.trim().isEmpty()) {
            return null;
        }

        final SslContextDefinition sslContextDefinition;
        sslContextDefinition = new SslContextDefinition();
        sslContextDefinition.setTruststoreFile(truststoreFile);
        sslContextDefinition.setTruststorePass(config.getTruststorePassword());
        sslContextDefinition.setTruststoreType(config.getTruststoreType());

        final String keystoreFile = config.getKeystoreFile();
        if (keystoreFile != null && !keystoreFile.trim().isEmpty()) {
            sslContextDefinition.setKeystoreFile(keystoreFile);
            sslContextDefinition.setKeystoreType(config.getKeystoreType());

            final String keystorePass = config.getKeystorePassword();
            sslContextDefinition.setKeystorePass(keystorePass);

            final String explicitKeyPass = config.getKeystoreKeyPassword();
            final String keyPass = (explicitKeyPass == null || explicitKeyPass.trim().isEmpty()) ? keystorePass : explicitKeyPass;
            sslContextDefinition.setKeyPass(keyPass);
        }

        return sslContextDefinition;
    }

    private static URLClassLoader getConnectClassLoader() {
        final ClassLoader classLoader = StatelessKafkaConnectorUtil.class.getClassLoader();
        if (!(classLoader instanceof URLClassLoader)) {
            throw new IllegalStateException("No configuration value was set for the " +
                    StatelessNiFiCommonConfig.NAR_DIRECTORY +
                    " configuration property, and was unable to determine the NAR directory automatically");
        }

        return (URLClassLoader) classLoader;
    }

    private static File detectBootstrapJar() {
        final URLClassLoader urlClassLoader = getConnectClassLoader();
        for (final URL url : urlClassLoader.getURLs()) {
            final String artifactFilename = url.getFile();
            if (artifactFilename == null) {
                continue;
            }

            final File artifactFile = new File(artifactFilename);
            if (STATELESS_BOOTSTRAP_FILE_PATTERN.matcher(artifactFile.getName()).matches()) {
                return artifactFile;
            }
        }

        return null;
    }

    private static File detectNarDirectory() {
        final File bootstrapJar = detectBootstrapJar();
        if (bootstrapJar == null) {
            final URLClassLoader urlClassLoader = getConnectClassLoader();
            logger.error("ClassLoader that loaded Stateless Kafka Connector did not contain nifi-stateless-bootstrap." +
                    " URLs that were present: {}", Arrays.asList(urlClassLoader.getURLs()));
            throw new IllegalStateException("No configuration value was set for the " +
                    StatelessNiFiCommonConfig.NAR_DIRECTORY +
                    " configuration property, and was unable to determine the NAR directory automatically");
        }

        final File narDirectory = bootstrapJar.getParentFile();
        logger.info("Detected NAR Directory to be {}", narDirectory.getAbsolutePath());
        return narDirectory;
    }
}
