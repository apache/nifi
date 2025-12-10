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
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.StepConfiguration;
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
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StandardConnectorTestRunner implements ConnectorTestRunner, Closeable {
    private final File narLibraryDirectory;

    private ConnectorMockServer mockServer;

    private StandardConnectorTestRunner(final Builder builder) {
        this.narLibraryDirectory = builder.narLibraryDirectory;

        try {
            bootstrapInstance();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to bootstrap ConnectorTestRunner", e);
        }

        // It is important that we register the processor mocks before instantiating the connector.
        // Otherwise, the call to instantiateConnector will initialize the Connector, which may update the flow.
        // If the flow is updated before the processor mocks are registered, the Processors will be created without
        // using the mocks. Subsequent updates to the flow will not replace the Processors already created because
        // these are not recognized as updates to the flow, since the framework assumes that the type of a Processor
        // with a given ID does not change.
        builder.processorMocks.forEach(mockServer::mockProcessor);

        mockServer.instantiateConnector(builder.connectorClassName);
    }

    private void bootstrapInstance() throws IOException, ClassNotFoundException {
        final List<Path> libDirectoryPaths = List.of(narLibraryDirectory.toPath());
        final File extensionsWorkingDir = new File("target/work/extensions");
        final File frameworkWorkingDir = new File("target/work/framework");

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

        final NiFiProperties properties;
        try (final InputStream propertiesIn = getClass().getClassLoader().getResourceAsStream("nifi.properties")) {
            properties = NiFiProperties.createBasicNiFiProperties(propertiesIn);
        }

        nifiServer.initialize(properties, systemBundle, narBundles, extensionMapping);
        nifiServer.start();

        mockServer = (ConnectorMockServer) nifiServer;
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
    public ConnectorConfigVerificationResult verifyConfiguration(final String stepName, final Map<String, String> propertyValueOverrides) {
        return mockServer.verifyConfiguration(stepName, propertyValueOverrides);
    }

    @Override
    public ConnectorConfigVerificationResult verifyConfiguration(final String stepName, final StepConfiguration configurationOverrides) {
        return mockServer.verifyConfiguration(stepName, configurationOverrides);
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


    public static class Builder {
        private String connectorClassName;
        private File narLibraryDirectory;
        private final Map<String, Class<? extends Processor>> processorMocks = new HashMap<>();

        public Builder connectorClassName(final String connectorClassName) {
            this.connectorClassName = connectorClassName;
            return this;
        }

        public Builder narLibraryDirectory(final File libDirectory) {
            this.narLibraryDirectory = libDirectory;
            return this;
        }

        public Builder mockProcessor(final String processorType, final Class<? extends Processor> mockProcessorClass) {
            processorMocks.put(processorType, mockProcessorClass);
            return this;
        }

        public StandardConnectorTestRunner build() {
            if (!narLibraryDirectory.exists() || !narLibraryDirectory.isDirectory()) {
                throw new IllegalArgumentException("NAR file does not exist or is not a directory: " + narLibraryDirectory.getAbsolutePath());
            }

            return new StandardConnectorTestRunner(this);
        }
    }
}
