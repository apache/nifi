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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.nifi.kafka.connect.validators.ConnectDirectoryExistsValidator;
import org.apache.nifi.kafka.connect.validators.ConnectHttpUrlValidator;
import org.apache.nifi.kafka.connect.validators.FlowSnapshotValidator;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StatelessKafkaConnectorUtil {
    private static final String UNKNOWN_VERSION = "<Unable to determine Stateless NiFi Kafka Connector Version>";
    private static final Logger logger = LoggerFactory.getLogger(StatelessKafkaConnectorUtil.class);
    private static final Lock unpackNarLock = new ReentrantLock();

    static final String NAR_DIRECTORY = "nar.directory";
    static final String EXTENSIONS_DIRECTORY = "extensions.directory";
    static final String WORKING_DIRECTORY = "working.directory";
    static final String FLOW_SNAPSHOT = "flow.snapshot";
    static final String KRB5_FILE = "krb5.file";
    static final String NEXUS_BASE_URL = "nexus.url";
    static final String DATAFLOW_TIMEOUT = "dataflow.timeout";
    static final String DATAFLOW_NAME = "name";

    static final String TRUSTSTORE_FILE = "security.truststore";
    static final String TRUSTSTORE_TYPE = "security.truststoreType";
    static final String TRUSTSTORE_PASSWORD = "security.truststorePasswd";
    static final String KEYSTORE_FILE = "security.keystore";
    static final String KEYSTORE_TYPE = "security.keystoreType";
    static final String KEYSTORE_PASSWORD = "security.keystorePasswd";
    static final String KEY_PASSWORD = "security.keyPasswd";
    static final String SENSITIVE_PROPS_KEY = "sensitive.props.key";

    static final String BOOTSTRAP_SNAPSHOT_URL = "nifi.stateless.flow.snapshot.url";
    static final String BOOTSTRAP_SNAPSHOT_FILE = "nifi.stateless.flow.snapshot.file";
    static final String BOOTSTRAP_SNAPSHOT_CONTENTS = "nifi.stateless.flow.snapshot.contents";
    static final String BOOTSTRAP_FLOW_NAME = "nifi.stateless.flow.name";

    static final String DEFAULT_KRB5_FILE = "/etc/krb5.conf";
    static final String DEFAULT_DATAFLOW_TIMEOUT = "60 sec";
    static final File DEFAULT_WORKING_DIRECTORY = new File("/tmp/nifi-stateless-working");
    static final File DEFAULT_EXTENSIONS_DIRECTORY = new File("/tmp/nifi-stateless-extensions");
    static final String DEFAULT_SENSITIVE_PROPS_KEY = "nifi-stateless";

    private static final Pattern STATELESS_BOOTSTRAP_FILE_PATTERN = Pattern.compile("nifi-stateless-bootstrap-(.*).jar");
    private static final Pattern PARAMETER_WITH_CONTEXT_PATTERN = Pattern.compile("parameter\\.(.*?):(.*)");
    private static final Pattern PARAMETER_WITHOUT_CONTEXT_PATTERN = Pattern.compile("parameter\\.(.*)");

    public static void addCommonConfigElements(final ConfigDef configDef) {
        configDef.define(NAR_DIRECTORY, ConfigDef.Type.STRING, null, new ConnectDirectoryExistsValidator(), ConfigDef.Importance.HIGH,
            "Specifies the directory that stores the NiFi Archives (NARs)");
        configDef.define(EXTENSIONS_DIRECTORY, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "Specifies the directory that stores the extensions that will be downloaded (if any) from the configured Extension Client");
        configDef.define(WORKING_DIRECTORY, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "Specifies the temporary working directory for expanding NiFi Archives (NARs)");
        configDef.define(FLOW_SNAPSHOT, ConfigDef.Type.STRING, null, new FlowSnapshotValidator(), ConfigDef.Importance.HIGH,
            "Specifies the dataflow to run. This may be a file containing the dataflow, a URL that points to a dataflow, or a String containing the entire dataflow as an escaped JSON.");
        configDef.define(DATAFLOW_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "The name of the dataflow.");

        configDef.define(StatelessKafkaConnectorUtil.KRB5_FILE, ConfigDef.Type.STRING, StatelessKafkaConnectorUtil.DEFAULT_KRB5_FILE, ConfigDef.Importance.MEDIUM,
            "Specifies the krb5.conf file to use if connecting to Kerberos-enabled services");
        configDef.define(StatelessKafkaConnectorUtil.NEXUS_BASE_URL, ConfigDef.Type.STRING, null, new ConnectHttpUrlValidator(), ConfigDef.Importance.MEDIUM,
            "Specifies the Base URL of the Nexus instance to source extensions from");

        configDef.define(StatelessKafkaConnectorUtil.DATAFLOW_TIMEOUT, ConfigDef.Type.STRING, StatelessKafkaConnectorUtil.DEFAULT_DATAFLOW_TIMEOUT, ConfigDef.Importance.MEDIUM,
            "Specifies the amount of time to wait for the dataflow to finish processing input before considering the dataflow a failure");

        configDef.define(KEYSTORE_FILE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
            "Filename of the keystore that Stateless NiFi should use for connecting to NiFi Registry and for Site-to-Site communications.");
        configDef.define(KEYSTORE_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
            "The type of the Keystore file. Either JKS or PKCS12.");
        configDef.define(KEYSTORE_PASSWORD, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
            "The password for the keystore.");
        configDef.define(KEY_PASSWORD, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
            "The password for the key in the keystore. If not provided, the password is assumed to be the same as the keystore password.");
        configDef.define(TRUSTSTORE_FILE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
            "Filename of the truststore that Stateless NiFi should use for connecting to NiFi Registry and for Site-to-Site communications. If not specified, communications will occur only over " +
                "http, not https.");
        configDef.define(TRUSTSTORE_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
            "The type of the Truststore file. Either JKS or PKCS12.");
        configDef.define(TRUSTSTORE_PASSWORD, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
            "The password for the truststore.");
        configDef.define(SENSITIVE_PROPS_KEY, ConfigDef.Type.PASSWORD, DEFAULT_SENSITIVE_PROPS_KEY, ConfigDef.Importance.MEDIUM, "A key that components can use for encrypting and decrypting " +
            "sensitive values.");
    }

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

    public static StatelessDataflow createDataflow(final Map<String, String> properties) {
        final StatelessEngineConfiguration engineConfiguration = createEngineConfiguration(properties);
        final String configuredFlowSnapshot = properties.get(FLOW_SNAPSHOT);

        final List<ParameterOverride> parameterOverrides = parseParameterOverrides(properties);
        final String dataflowName = properties.get(DATAFLOW_NAME);

        final DataflowDefinition<?> dataflowDefinition;
        final StatelessBootstrap bootstrap;
        try {
            final Map<String, String> dataflowDefinitionProperties = new HashMap<>();

            if (configuredFlowSnapshot.startsWith("http://") || configuredFlowSnapshot.startsWith("https://")) {
                logger.debug("Configured Flow Snapshot appears to be a URL. Will use {} property to configured Stateless NiFi", BOOTSTRAP_SNAPSHOT_URL);
                dataflowDefinitionProperties.put(BOOTSTRAP_SNAPSHOT_URL, configuredFlowSnapshot);
            } else if (configuredFlowSnapshot.trim().startsWith("{")) {
                logger.debug("Configured Flow Snapshot appears to be JSON. Will use {} property to configured Stateless NiFi", BOOTSTRAP_SNAPSHOT_CONTENTS);
                dataflowDefinitionProperties.put(BOOTSTRAP_SNAPSHOT_CONTENTS, configuredFlowSnapshot);
            } else {
                logger.debug("Configured Flow Snapshot appears to be a File. Will use {} property to configured Stateless NiFi", BOOTSTRAP_SNAPSHOT_FILE);
                final File flowSnapshotFile = new File(configuredFlowSnapshot);
                dataflowDefinitionProperties.put(BOOTSTRAP_SNAPSHOT_FILE, flowSnapshotFile.getAbsolutePath());
            }

            dataflowDefinitionProperties.put(BOOTSTRAP_FLOW_NAME, dataflowName);

            MDC.setContextMap(Collections.singletonMap("dataflow", dataflowName));

            // Use a Write Lock to ensure that only a single thread is calling StatelessBootstrap.bootstrap().
            // We do this because the bootstrap() method will expand all NAR files into the working directory.
            // If we have multiple Connector instances, or multiple tasks, we don't want several threads all
            // unpacking NARs at the same time, as it could potentially result in the working directory becoming corrupted.
            unpackNarLock.lock();
            try {
                bootstrap = StatelessBootstrap.bootstrap(engineConfiguration, StatelessNiFiSourceTask.class.getClassLoader());
            } finally {
                unpackNarLock.unlock();
            }

            dataflowDefinition = bootstrap.parseDataflowDefinition(dataflowDefinitionProperties, parameterOverrides);
            return bootstrap.createDataflow(dataflowDefinition);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to bootstrap Stateless NiFi Engine", e);
        }
    }

    private static List<ParameterOverride> parseParameterOverrides(final Map<String, String> properties) {
        final List<ParameterOverride> parameterOverrides = new ArrayList<>();

        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            final String parameterValue = entry.getValue();

            ParameterOverride parameterOverride = null;
            final Matcher matcher = PARAMETER_WITH_CONTEXT_PATTERN.matcher(entry.getKey());
            if (matcher.matches()) {
                final String contextName = matcher.group(1);
                final String parameterName = matcher.group(2);
                parameterOverride = new ParameterOverride(contextName, parameterName, parameterValue);
            } else {
                final Matcher noContextMatcher = PARAMETER_WITHOUT_CONTEXT_PATTERN.matcher(entry.getKey());
                if (noContextMatcher.matches()) {
                    final String parameterName = noContextMatcher.group(1);
                    parameterOverride = new ParameterOverride(parameterName, parameterValue);
                }
            }

            if (parameterOverride != null) {
                parameterOverrides.add(parameterOverride);
            }
        }

        return parameterOverrides;
    }

    public static Map<String, String> getLoggableProperties(final Map<String, String> properties) {
        final Map<String, String> loggable = new HashMap<>(properties);
        loggable.keySet().removeIf(key -> key.startsWith("parameter."));
        return loggable;
    }

    private static StatelessEngineConfiguration createEngineConfiguration(final Map<String, String> properties) {
        final File narDirectory;
        final String narDirectoryFilename = properties.get(NAR_DIRECTORY);
        if (narDirectoryFilename == null) {
            narDirectory = detectNarDirectory();
        } else {
            narDirectory = new File(narDirectoryFilename);
        }

        final String dataflowName = properties.get(DATAFLOW_NAME);

        final File baseWorkingDirectory;
        final String workingDirectoryFilename = properties.get(WORKING_DIRECTORY);
        if (workingDirectoryFilename == null) {
            baseWorkingDirectory = DEFAULT_WORKING_DIRECTORY;
        } else {
            baseWorkingDirectory = new File(workingDirectoryFilename);
        }
        final File workingDirectory = new File(baseWorkingDirectory, dataflowName);

        final File extensionsDirectory;
        final String extensionsDirectoryFilename = properties.get(EXTENSIONS_DIRECTORY);
        if (extensionsDirectoryFilename == null) {
            extensionsDirectory = DEFAULT_EXTENSIONS_DIRECTORY;
        } else {
            extensionsDirectory = new File(extensionsDirectoryFilename);
        }

        final SslContextDefinition sslContextDefinition = createSslContextDefinition(properties);

        final StatelessEngineConfiguration engineConfiguration = new StatelessEngineConfiguration() {
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
            public File getKrb5File() {
                return new File(properties.getOrDefault(KRB5_FILE, DEFAULT_KRB5_FILE));
            }

            @Override
            public SslContextDefinition getSslContext() {
                return sslContextDefinition;
            }

            @Override
            public String getSensitivePropsKey() {
                return properties.getOrDefault(SENSITIVE_PROPS_KEY, DEFAULT_SENSITIVE_PROPS_KEY);
            }

            @Override
            public List<ExtensionClientDefinition> getExtensionClients() {
                final List<ExtensionClientDefinition> extensionClientDefinitions = new ArrayList<>();

                final String nexusBaseUrl = properties.get(NEXUS_BASE_URL);
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
        };

        return engineConfiguration;
    }

    private static SslContextDefinition createSslContextDefinition(final Map<String, String> properties) {
        final String truststoreFile = properties.get(TRUSTSTORE_FILE);
        if (truststoreFile == null || truststoreFile.trim().isEmpty()) {
            return null;
        }

        final SslContextDefinition sslContextDefinition;
        sslContextDefinition = new SslContextDefinition();
        sslContextDefinition.setTruststoreFile(truststoreFile);
        sslContextDefinition.setTruststorePass(properties.get(TRUSTSTORE_PASSWORD));
        sslContextDefinition.setTruststoreType(properties.get(TRUSTSTORE_TYPE));

        final String keystoreFile = properties.get(KEYSTORE_FILE);
        if (keystoreFile != null && !keystoreFile.trim().isEmpty()) {
            sslContextDefinition.setKeystoreFile(keystoreFile);
            sslContextDefinition.setKeystoreType(properties.get(KEYSTORE_TYPE));

            final String keystorePass = properties.get(KEYSTORE_PASSWORD);
            sslContextDefinition.setKeystorePass(keystorePass);

            final String explicitKeyPass = properties.get(KEY_PASSWORD);
            final String keyPass = (explicitKeyPass == null || explicitKeyPass.trim().isEmpty()) ? keystorePass : explicitKeyPass;
            sslContextDefinition.setKeyPass(keyPass);
        }

        return sslContextDefinition;
    }

    private static URLClassLoader getConnectClassLoader() {
        final ClassLoader classLoader = StatelessKafkaConnectorUtil.class.getClassLoader();
        if (!(classLoader instanceof URLClassLoader)) {
            throw new IllegalStateException("No configuration value was set for the " + NAR_DIRECTORY + " configuration property, and was unable to determine the NAR directory automatically");
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
            logger.error("ClassLoader that loaded Stateless Kafka Connector did not contain nifi-stateless-bootstrap. URLs that were present: {}", Arrays.asList(urlClassLoader.getURLs()));
            throw new IllegalStateException("No configuration value was set for the " + NAR_DIRECTORY + " configuration property, and was unable to determine the NAR directory automatically");
        }

        final File narDirectory = bootstrapJar.getParentFile();
        logger.info("Detected NAR Directory to be {}", narDirectory.getAbsolutePath());
        return narDirectory;
    }
}
