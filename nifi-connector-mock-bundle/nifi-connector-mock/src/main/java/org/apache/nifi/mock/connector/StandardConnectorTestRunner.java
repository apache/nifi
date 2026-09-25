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

package org.apache.nifi.mock.connector;

import org.apache.nifi.NiFiServer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StepConfiguration;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.mock.connector.server.ConnectorConfigVerificationResult;
import org.apache.nifi.mock.connector.server.ConnectorMockServer;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarUnpackMode;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.NiFiProperties;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class StandardConnectorTestRunner implements ConnectorTestRunner, Closeable {
    private final File narLibraryDirectory;
    private final int httpPort;
    private final Path instanceDirectory;

    private ConnectorMockServer mockServer;

    static Properties getInstanceProperties(final Path instanceDirectory) {
        final Properties properties = new Properties();
        if (instanceDirectory == null) {
            return properties;
        }

        properties.setProperty(NiFiProperties.FLOW_CONFIGURATION_FILE, instanceDirectory.resolve("conf/flow.json.gz").toString());
        properties.setProperty(NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_DIR, instanceDirectory.resolve("conf/archive").toString());
        properties.setProperty(NiFiProperties.STATE_MANAGEMENT_CONFIG_FILE, instanceDirectory.resolve("conf/state-management.xml").toString());
        properties.setProperty(NiFiProperties.REPOSITORY_DATABASE_DIRECTORY, instanceDirectory.resolve("database_repository").toString());
        properties.setProperty(NiFiProperties.FLOWFILE_REPOSITORY_DIRECTORY, instanceDirectory.resolve("flowfile_repository").toString());
        properties.setProperty(NiFiProperties.REPOSITORY_CONTENT_PREFIX + "default", instanceDirectory.resolve("content_repository").toString());
        properties.setProperty(NiFiProperties.NAR_PERSISTENCE_PROVIDER_PROPERTIES_PREFIX + "directory", instanceDirectory.resolve("nar_repository").toString());
        properties.setProperty(NiFiProperties.ASSET_MANAGER_PREFIX + "directory", instanceDirectory.resolve("assets").toString());
        properties.setProperty(NiFiProperties.CONNECTOR_ASSET_MANAGER_PREFIX + "directory", instanceDirectory.resolve("connector-assets").toString());
        properties.setProperty(NiFiProperties.NAR_WORKING_DIRECTORY, instanceDirectory.resolve("work").toString());
        properties.setProperty(NiFiProperties.NAR_LIBRARY_AUTOLOAD_DIRECTORY, instanceDirectory.resolve("autoload").toString());
        properties.setProperty(NiFiProperties.WEB_WORKING_DIR, instanceDirectory.resolve("work/jetty").toString());
        return properties;
    }

    private StandardConnectorTestRunner(final Builder builder) {
        this.narLibraryDirectory = builder.narLibraryDirectory;
        this.httpPort = builder.httpPort;
        this.instanceDirectory = builder.instanceDirectory;

        try {
            bootstrapInstance();
        } catch (final Exception e) {
            closeAfterFailure(e);
            throw new RuntimeException("Failed to bootstrap ConnectorTestRunner", e);
        }

        try {
            // It is important that we register the processor and controller service mocks before instantiating the connector.
            // Otherwise, the call to instantiateConnector will initialize the Connector, which may update the flow.
            // If the flow is updated before the mocks are registered, the components will be created without
            // using the mocks. Subsequent updates to the flow will not replace the components already created because
            // these are not recognized as updates to the flow, since the framework assumes that the type of a component
            // with a given ID does not change.
            builder.processorMocks.forEach(mockServer::mockProcessor);
            builder.controllerServiceMocks.forEach(mockServer::mockControllerService);

            mockServer.instantiateConnector(builder.connectorClassName);
        } catch (final RuntimeException e) {
            closeAfterFailure(e);
            throw e;
        }
    }

    private void closeAfterFailure(final Exception failure) {
        try {
            close();
        } catch (final RuntimeException e) {
            failure.addSuppressed(e);
        }
    }

    private void bootstrapInstance() throws IOException, ClassNotFoundException {
        final List<Path> libDirectoryPaths = List.of(narLibraryDirectory.toPath());
        final File extensionsWorkingDir;
        final File frameworkWorkingDir;
        if (instanceDirectory == null) {
            extensionsWorkingDir = new File("target/work/extensions");
            frameworkWorkingDir = new File("target/work/framework");
        } else {
            final File narWorkingDirectory = instanceDirectory.resolve("work").toFile();
            extensionsWorkingDir = new File(narWorkingDirectory, "extensions");
            frameworkWorkingDir = new File(narWorkingDirectory, "framework");
        }

        final Bundle systemBundle = SystemBundle.create(narLibraryDirectory.getAbsolutePath(), ClassLoader.getSystemClassLoader());

        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(systemBundle, frameworkWorkingDir, extensionsWorkingDir, libDirectoryPaths, true,
            NarClassLoaders.FRAMEWORK_NAR_ID, true, false, NarUnpackMode.UNPACK_INDIVIDUAL_JARS, bundleCoordinate -> true);

        final NarClassLoaders narClassLoaders = new NarClassLoaders();
        narClassLoaders.init(frameworkWorkingDir, extensionsWorkingDir);

        final NiFiServer nifiServer = narClassLoaders.getServer();
        if (nifiServer == null) {
            throw new RuntimeException("Could not find NiFiServer instance");
        }
        if (!(nifiServer instanceof ConnectorMockServer)) {
            throw new RuntimeException("Test ClassPath does not contain ConnectorMockServer. " +
                                       "Ensure that the appropriate module is packaged in the NAR library directory: " + narLibraryDirectory.getAbsolutePath());
        }

        // Set Application Server Class Loader for subsequent operations
        final ClassLoader applicationServerClassLoader = narClassLoaders.getServer().getClass().getClassLoader();
        Thread.currentThread().setContextClassLoader(applicationServerClassLoader);

        final Set<Bundle> narBundles = narClassLoaders.getBundles();

        final Properties additionalProperties = getInstanceProperties(instanceDirectory);
        if (httpPort >= 0) {
            additionalProperties.setProperty(NiFiProperties.WEB_HTTP_PORT, String.valueOf(httpPort));
        }

        final NiFiProperties properties;
        try (final InputStream propertiesIn = getClass().getClassLoader().getResourceAsStream("nifi.properties")) {
            properties = NiFiProperties.createBasicNiFiProperties(propertiesIn, additionalProperties);
        }

        mockServer = (ConnectorMockServer) nifiServer;
        mockServer.initialize(properties, systemBundle, narBundles, extensionMapping);
        mockServer.start();
        mockServer.registerMockBundle(getClass().getClassLoader(), new File(extensionsWorkingDir, "mock-implementations-bundle"));
    }

    @Override
    public void close() {
        if (mockServer != null) {
            mockServer.stop();
        }
    }

    @Override
    public void applyUpdate() throws FlowUpdateException {
        mockServer.applyUpdate();
    }

    @Override
    public void configure(final String stepName, final StepConfiguration configuration) throws FlowUpdateException {
        mockServer.configure(stepName, configuration);
    }

    @Override
    public void configure(final String stepName, final Map<String, String> propertyValues) throws FlowUpdateException {
        mockServer.configure(stepName, propertyValues);
    }

    @Override
    public void configure(final String stepName, final Map<String, String> propertyValues, final Map<String, ConnectorValueReference> propertyReferences) throws FlowUpdateException {
        mockServer.configure(stepName, propertyValues, propertyReferences);
    }

    @Override
    public SecretReference createSecretReference(final String secretName) {
        return mockServer.createSecretReference(secretName);
    }

    @Override
    public ConnectorConfigVerificationResult verifyConfiguration(final String stepName, final Map<String, String> propertyValueOverrides) {
        return mockServer.verifyConfiguration(stepName, propertyValueOverrides);
    }

    @Override
    public ConnectorConfigVerificationResult verifyConfiguration(final String stepName, final Map<String, String> propertyValueOverrides,
            final Map<String, ConnectorValueReference> referenceOverrides) {

        return mockServer.verifyConfiguration(stepName, propertyValueOverrides, referenceOverrides);
    }

    @Override
    public ConnectorConfigVerificationResult verifyConfiguration(final String stepName, final StepConfiguration configurationOverrides) {
        return mockServer.verifyConfiguration(stepName, configurationOverrides);
    }

    @Override
    public void addSecret(final String name, final String value) {
        mockServer.addSecret(name, value);
    }

    @Override
    public AssetReference addAsset(final File file) {
        return mockServer.addAsset(file);
    }

    @Override
    public AssetReference addAsset(final String assetName, final InputStream contents) {
        return mockServer.addAsset(assetName, contents);
    }

    @Override
    public void startConnector() {
        mockServer.startConnector();
    }

    @Override
    public void stopConnector() {
        mockServer.stopConnector();
    }

    @Override
    public void stopConnector(final Duration timeout) throws TimeoutException {
        mockServer.stopConnector(timeout);
    }

    @Override
    public void waitForDataIngested(final Duration maxWaitTime) {
        mockServer.waitForDataIngested(maxWaitTime);
    }

    @Override
    public void waitForIdle(final Duration maxWaitTime) {
        mockServer.waitForIdle(maxWaitTime);
    }

    @Override
    public void waitForIdle(final Duration minIdleTime, final Duration maxWaitTime) {
        mockServer.waitForIdle(minIdleTime, maxWaitTime);
    }

    @Override
    public List<ValidationResult> validate() {
        return mockServer.validate();
    }

    @Override
    public int getHttpPort() {
        return mockServer.getHttpPort();
    }

    @Override
    public List<DescribedValue> fetchAllowableValues(final String stepName, final String propertyName) {
        return mockServer.fetchAllowableValues(stepName, propertyName);
    }

    @Override
    public VersionedExternalFlow getActiveFlowSnapshot() {
        return mockServer.getActiveFlowSnapshot();
    }

    @Override
    public VersionedExternalFlow getWorkingFlowSnapshot() {
        return mockServer.getWorkingFlowSnapshot();
    }


    public static class Builder {
        private String connectorClassName;
        private File narLibraryDirectory;
        private int httpPort = -1;
        private Path instanceDirectory;
        private final Map<String, Class<? extends Processor>> processorMocks = new HashMap<>();
        private final Map<String, Class<? extends ControllerService>> controllerServiceMocks = new HashMap<>();

        public Builder connectorClassName(final String connectorClassName) {
            this.connectorClassName = connectorClassName;
            return this;
        }

        public Builder narLibraryDirectory(final File libDirectory) {
            this.narLibraryDirectory = libDirectory;
            return this;
        }

        public Builder httpPort(final int httpPort) {
            this.httpPort = httpPort;
            return this;
        }

        public Builder instanceDirectory(final Path instanceDirectory) {
            this.instanceDirectory = Objects.requireNonNull(instanceDirectory, "Instance Directory required")
                    .toAbsolutePath()
                    .normalize();
            return this;
        }

        public Builder mockProcessor(final String processorType, final Class<? extends Processor> mockProcessorClass) {
            processorMocks.put(processorType, mockProcessorClass);
            return this;
        }

        public Builder mockControllerService(final String controllerServiceType, final Class<? extends ControllerService> mockControllerServiceClass) {
            controllerServiceMocks.put(controllerServiceType, mockControllerServiceClass);
            return this;
        }

        public StandardConnectorTestRunner build() {
            if (!narLibraryDirectory.exists() || !narLibraryDirectory.isDirectory()) {
                throw new IllegalArgumentException("NAR file does not exist or is not a directory: " + narLibraryDirectory.getAbsolutePath());
            }

            if (instanceDirectory != null) {
                try {
                    Files.createDirectories(instanceDirectory);
                } catch (final IOException e) {
                    throw new UncheckedIOException("Failed to create instance directory: " + instanceDirectory, e);
                }
            }

            return new StandardConnectorTestRunner(this);
        }
    }
}
