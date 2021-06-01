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

package org.apache.nifi.stateless.flow;

import org.apache.nifi.components.state.StatelessStateManagerProvider;
import org.apache.nifi.controller.kerberos.KerberosConfig;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.CounterRepository;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.StandardCounterRepository;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.controller.scheduling.StatelessProcessScheduler;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.StandardControllerServiceProvider;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorFactory;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.extensions.ExtensionClient;
import org.apache.nifi.extensions.ExtensionRepository;
import org.apache.nifi.extensions.FileSystemExtensionRepository;
import org.apache.nifi.extensions.NexusExtensionClient;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.StandardParameterContextManager;
import org.apache.nifi.provenance.IdentifierLookup;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.InMemoryFlowRegistry;
import org.apache.nifi.registry.flow.StandardFlowRegistryClient;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.stateless.bootstrap.ExtensionDiscovery;
import org.apache.nifi.stateless.config.ExtensionClientDefinition;
import org.apache.nifi.stateless.config.SslConfigurationUtil;
import org.apache.nifi.stateless.config.SslContextDefinition;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.engine.CachingProcessContextFactory;
import org.apache.nifi.stateless.engine.ProcessContextFactory;
import org.apache.nifi.stateless.engine.StandardStatelessEngine;
import org.apache.nifi.stateless.engine.StatelessAuthorizer;
import org.apache.nifi.stateless.engine.StatelessEngine;
import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.apache.nifi.stateless.engine.StatelessEngineInitializationContext;
import org.apache.nifi.stateless.engine.StatelessFlowManager;
import org.apache.nifi.stateless.engine.StatelessProcessContextFactory;
import org.apache.nifi.stateless.engine.StatelessProvenanceAuthorizableFactory;
import org.apache.nifi.stateless.repository.ByteArrayContentRepository;
import org.apache.nifi.stateless.repository.RepositoryContextFactory;
import org.apache.nifi.stateless.repository.StatelessFlowFileRepository;
import org.apache.nifi.stateless.repository.StatelessProvenanceRepository;
import org.apache.nifi.stateless.repository.StatelessRepositoryContextFactory;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StandardStatelessDataflowFactory implements StatelessDataflowFactory<VersionedFlowSnapshot> {
    private static final Logger logger = LoggerFactory.getLogger(StandardStatelessDataflowFactory.class);
    private static final EncryptionMethod ENCRYPTION_METHOD = EncryptionMethod.MD5_256AES;

    @Override
    public StatelessDataflow createDataflow(final StatelessEngineConfiguration engineConfiguration, final DataflowDefinition<VersionedFlowSnapshot> dataflowDefinition)
                    throws IOException, StatelessConfigurationException {
        final long start = System.currentTimeMillis();

        final VersionedFlowSnapshot flowSnapshot = dataflowDefinition.getFlowSnapshot();

        ProvenanceRepository provenanceRepo = null;
        ContentRepository contentRepo = null;
        StatelessProcessScheduler processScheduler = null;
        FlowFileRepository flowFileRepo = null;
        FlowFileEventRepository flowFileEventRepo = null;

        try {
            final BulletinRepository bulletinRepository = new VolatileBulletinRepository();
            final File workingDir = engineConfiguration.getWorkingDirectory();
            if (!workingDir.exists() && !workingDir.mkdirs()) {
                throw new IOException("Working Directory " + workingDir + " does not exist and could not be created");
            }

            final InMemoryFlowRegistry flowRegistry = new InMemoryFlowRegistry();
            flowRegistry.addFlowSnapshot(flowSnapshot);
            final FlowRegistryClient flowRegistryClient = new StandardFlowRegistryClient();
            flowRegistryClient.addFlowRegistry(flowRegistry);

            final NarClassLoaders narClassLoaders = new NarClassLoaders();
            final File extensionsWorkingDir = new File(workingDir, "extensions");
            final ClassLoader systemClassLoader = createSystemClassLoader(engineConfiguration.getNarDirectory());
            final ExtensionDiscoveringManager extensionManager = ExtensionDiscovery.discover(extensionsWorkingDir, systemClassLoader, narClassLoaders);

            flowFileEventRepo = new RingBufferEventRepository(5);

            final StatelessStateManagerProvider stateManagerProvider = new StatelessStateManagerProvider();

            final ParameterContextManager parameterContextManager = new StandardParameterContextManager();
            processScheduler = new StatelessProcessScheduler(extensionManager);
            provenanceRepo = new StatelessProvenanceRepository(1_000);
            provenanceRepo.initialize(EventReporter.NO_OP, new StatelessAuthorizer(), new StatelessProvenanceAuthorizableFactory(), IdentifierLookup.EMPTY);

            final SSLContext sslContext;
            try {
                sslContext = SslConfigurationUtil.createSslContext(engineConfiguration.getSslContext());
            } catch (StatelessConfigurationException e) {
                throw new StatelessConfigurationException("Could not create SSLContext", e);
            }

            // Build Extension Repository
            final List<ExtensionClient> extensionClients = new ArrayList<>();
            for (final ExtensionClientDefinition extensionClientDefinition : engineConfiguration.getExtensionClients()) {
                final ExtensionClient extensionClient = createExtensionClient(extensionClientDefinition, engineConfiguration.getSslContext());
                extensionClients.add(extensionClient);
            }

            final ExtensionRepository extensionRepository = new FileSystemExtensionRepository(extensionManager, engineConfiguration.getExtensionsDirectory(), engineConfiguration.getWorkingDirectory(),
                narClassLoaders, extensionClients);

            final VariableRegistry variableRegistry = VariableRegistry.EMPTY_REGISTRY;
            final PropertyEncryptor lazyInitializedEncryptor = new PropertyEncryptor() {
                private PropertyEncryptor created = null;

                @Override
                public String encrypt(final String property) {
                    return getEncryptor().encrypt(property);
                }

                @Override
                public String decrypt(final String encryptedProperty) {
                    return getEncryptor().decrypt(encryptedProperty);
                }

                private synchronized PropertyEncryptor getEncryptor() {
                    if (created != null) {
                        return created;
                    }

                    created = getPropertyEncryptor(engineConfiguration.getSensitivePropsKey());
                    return created;
                }
            };

            final CounterRepository counterRepo = new StandardCounterRepository();

            final File krb5File = engineConfiguration.getKrb5File();
            final KerberosConfig kerberosConfig = new KerberosConfig(null, null, krb5File);
            logger.info("Setting java.security.krb5.conf to {}", krb5File.getAbsolutePath());
            System.setProperty("java.security.krb5.conf", krb5File.getAbsolutePath());

            final StatelessEngine<VersionedFlowSnapshot> statelessEngine = new StandardStatelessEngine.Builder()
                .bulletinRepository(bulletinRepository)
                .encryptor(lazyInitializedEncryptor)
                .extensionManager(extensionManager)
                .flowRegistryClient(flowRegistryClient)
                .stateManagerProvider(stateManagerProvider)
                .variableRegistry(variableRegistry)
                .processScheduler(processScheduler)
                .kerberosConfiguration(kerberosConfig)
                .flowFileEventRepository(flowFileEventRepo)
                .provenanceRepository(provenanceRepo)
                .extensionRepository(extensionRepository)
                .counterRepository(counterRepo)
                .build();

            final StatelessFlowManager flowManager = new StatelessFlowManager(flowFileEventRepo, parameterContextManager, statelessEngine, () -> true, sslContext);
            final ControllerServiceProvider controllerServiceProvider = new StandardControllerServiceProvider(processScheduler, bulletinRepository, flowManager, extensionManager);

            final ProcessContextFactory rawProcessContextFactory = new StatelessProcessContextFactory(controllerServiceProvider, lazyInitializedEncryptor, stateManagerProvider);
            final ProcessContextFactory processContextFactory = new CachingProcessContextFactory(rawProcessContextFactory);
            contentRepo = new ByteArrayContentRepository();
            flowFileRepo = new StatelessFlowFileRepository();

            final RepositoryContextFactory repositoryContextFactory = new StatelessRepositoryContextFactory(contentRepo, flowFileRepo, flowFileEventRepo,
                counterRepo, provenanceRepo, stateManagerProvider);
            final StatelessEngineInitializationContext statelessEngineInitializationContext = new StatelessEngineInitializationContext(controllerServiceProvider, flowManager, processContextFactory,
                repositoryContextFactory);

            processScheduler.initialize(processContextFactory, dataflowDefinition);
            statelessEngine.initialize(statelessEngineInitializationContext);

            // Initialize components. This is generally needed because of the interdependencies between the components.
            // There are some circular dependencies that are resolved by passing objects via initialization rather than by providing to the constructors.
            final ResourceClaimManager resourceClaimManager = new StandardResourceClaimManager();
            contentRepo.initialize(resourceClaimManager);
            flowFileRepo.initialize(resourceClaimManager);
            flowManager.initialize(controllerServiceProvider);

            // Create flow
            final ProcessGroup rootGroup = flowManager.createProcessGroup("root");
            rootGroup.setName("root");
            flowManager.setRootGroup(rootGroup);

            final StatelessDataflow dataflow = statelessEngine.createFlow(dataflowDefinition);
            final long millis = System.currentTimeMillis() - start;
            logger.info("NiFi Stateless Engine and Dataflow created and initialized in {} millis", millis);

            return dataflow;
        } catch (final Exception e) {
            try {
                if (provenanceRepo != null) {
                    provenanceRepo.close();
                }
            } catch (final IOException ioe) {
                e.addSuppressed(ioe);
            }

            if (contentRepo != null) {
                contentRepo.shutdown();
            }

            if (processScheduler != null) {
                processScheduler.shutdown();
            }

            if (flowFileRepo != null) {
                try {
                    flowFileRepo.close();
                } catch (final IOException ioe) {
                    e.addSuppressed(ioe);
                }
            }

            if (flowFileEventRepo != null) {
                try {
                    flowFileEventRepo.close();
                } catch (final IOException ioe) {
                    e.addSuppressed(ioe);
                }
            }

            throw e;
        }
    }

    private ExtensionClient createExtensionClient(final ExtensionClientDefinition definition, final SslContextDefinition sslContextDefinition) {
        final String type = definition.getExtensionClientType();
        if (!isValidExtensionClientType(type)) {
            throw new IllegalArgumentException("Invalid Extension Client type: <" + definition.getExtensionClientType() +">. Currently, the only supported type is <nexus>");
        }

        final SslContextDefinition sslContext = (definition.isUseSslContext() && sslContextDefinition != null) ? sslContextDefinition : null;
        return new NexusExtensionClient(definition.getBaseUrl(), sslContext, definition.getCommsTimeout());
    }

    private boolean isValidExtensionClientType(final String type) {
        return "nexus".equalsIgnoreCase(type.trim());
    }

    private ClassLoader createSystemClassLoader(final File narDirectory) throws StatelessConfigurationException {
        final ClassLoader systemClassLoader = StatelessDataflowFactory.class.getClassLoader();
        final int javaMajorVersion = getJavaMajorVersion();
        if (javaMajorVersion >= 11) {
            // If running on Java 11 or greater, add the JAXB/activation/annotation libs to the classpath.
            // TODO: Once the minimum Java version requirement of NiFi is 11, this processing should be removed.
            // JAXB/activation/annotation will be added as an actual dependency via pom.xml.
            return createJava11OrLaterSystemClassLoader(javaMajorVersion, narDirectory, systemClassLoader);
        }

        return systemClassLoader;
    }

    private ClassLoader createJava11OrLaterSystemClassLoader(final int javaMajorVersion, final File narDirectory, final ClassLoader parentClassLoader) throws StatelessConfigurationException {
        final List<URL> java11JarFileUrls = new ArrayList<>();

        final File java11Dir = new File(narDirectory, "java11");
        if (!java11Dir.exists()) {
            throw new StatelessConfigurationException("Could not create System-level ClassLoader because Java version is " + javaMajorVersion + " but could not find the requisite Java 11 libraries " +
                "at " + java11Dir.getAbsolutePath());
        }

        final File[] java11JarFiles = java11Dir.listFiles(filename -> filename.getName().toLowerCase().endsWith(".jar"));
        if (java11JarFiles == null || java11JarFiles.length == 0) {
            throw new StatelessConfigurationException("Could not create System-level ClassLoader because Java version is " + javaMajorVersion + " but could not find the requisite Java 11 libraries " +
                "at " + java11Dir.getAbsolutePath());
        }

        try {
            for (final File file : java11JarFiles) {
                java11JarFileUrls.add(file.toURI().toURL());
            }
        } catch (final Exception e) {
            throw new StatelessConfigurationException("Could not create System-level ClassLoader", e);
        }

        final ClassLoader classLoader = new URLClassLoader(java11JarFileUrls.toArray(new URL[0]), parentClassLoader);
        return classLoader;
    }

    private int getJavaMajorVersion() {
        final String javaVersion = System.getProperty("java.version");
        logger.debug("Java Version is {}", javaVersion);

        if (javaVersion.startsWith("1.")) {
            return Integer.parseInt(javaVersion.substring(2, 3));
        }

        final int dotIndex = javaVersion.indexOf(".");
        if (dotIndex < 0) {
            return Integer.parseInt(javaVersion);
        }

        return Integer.parseInt(javaVersion.substring(0, dotIndex));
    }

    private PropertyEncryptor getPropertyEncryptor(final String sensitivePropertiesKey) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(NiFiProperties.SENSITIVE_PROPS_ALGORITHM, ENCRYPTION_METHOD.getAlgorithm());
        properties.put(NiFiProperties.SENSITIVE_PROPS_PROVIDER, ENCRYPTION_METHOD.getProvider());
        properties.put(NiFiProperties.SENSITIVE_PROPS_KEY, sensitivePropertiesKey);
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);
        return PropertyEncryptorFactory.getPropertyEncryptor(niFiProperties);
    }
}
