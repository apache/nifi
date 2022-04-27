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
package org.apache.nifi.c2;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.PersistentUuidGenerator;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.api.FlowUpdateInfo;
import org.apache.nifi.c2.client.http.C2HttpClient;
import org.apache.nifi.c2.protocol.api.AgentInfo;
import org.apache.nifi.c2.protocol.api.AgentRepositories;
import org.apache.nifi.c2.protocol.api.AgentRepositoryStatus;
import org.apache.nifi.c2.protocol.api.AgentStatus;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.DeviceInfo;
import org.apache.nifi.c2.protocol.api.FlowInfo;
import org.apache.nifi.c2.protocol.api.FlowQueueStatus;
import org.apache.nifi.c2.protocol.api.NetworkInfo;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.apache.nifi.c2.protocol.api.SystemInfo;
import org.apache.nifi.c2.serializer.C2JacksonSerializer;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.diagnostics.StorageUsage;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestParser;
import org.apache.nifi.extension.manifest.parser.jaxb.JAXBExtensionManifestParser;
import org.apache.nifi.manifest.RuntimeManifestService;
import org.apache.nifi.manifest.StandardRuntimeManifestService;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class C2ClientService {

    private static final Logger logger = LoggerFactory.getLogger(C2ClientService.class);
    private static final String RUNTIME_MANIFEST_IDENTIFIER = "minifi";
    private static final String RUNTIME_TYPE = "minifi-java";
    private static final String CONF_DIR_KEY = "conf.dir";
    private static final String AGENT_IDENTIFIER_FILENAME = "agent-identifier";
    private static final String DEVICE_IDENTIFIER_FILENAME = "device-identifier";
    private static final String LOCATION = "location";

    private final C2Client c2Client;
    protected final AtomicReference<FlowUpdateInfo> updateInfo = new AtomicReference<>();
    private final FlowController flowController;
    private final AtomicReference<String> agentIdentifierRef = new AtomicReference<>();
    private final AtomicReference<String> deviceIdentifierRef = new AtomicReference<>();
    private final ScheduledThreadPoolExecutor scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
    private final ExtensionManifestParser extensionManifestParser = new JAXBExtensionManifestParser();
    private final C2ClientConfig clientConfig;

    final RuntimeManifestService runtimeManifestService =
            new StandardRuntimeManifestService(ExtensionManagerHolder.getExtensionManager(), extensionManifestParser, RUNTIME_MANIFEST_IDENTIFIER, RUNTIME_TYPE);
    private volatile long initialDelay = 0;
    private volatile long period = 1000L;
    private volatile int termination_wait = 5000;

    public C2ClientService(final NiFiProperties niFiProperties, final long clientHeartbeatPeriod, final FlowController flowController) {
        this.clientConfig = generateClientConfig(niFiProperties);
        this.c2Client = new C2HttpClient(clientConfig, new C2JacksonSerializer());
        this.period = clientHeartbeatPeriod;
        this.flowController = flowController;
    }

    private C2ClientConfig generateClientConfig(NiFiProperties properties) {
        return new C2ClientConfig.Builder()
                .agentClass(properties.getProperty(C2NiFiProperties.C2_AGENT_CLASS_KEY, ""))
                .agentIdentifier(properties.getProperty(C2NiFiProperties.C2_AGENT_HEARTBEAT_PERIOD_KEY, String.valueOf(C2NiFiProperties.C2_AGENT_DEFAULT_HEARTBEAT_PERIOD)))
                .c2Url(properties.getProperty(C2NiFiProperties.C2_REST_URL_KEY, ""))
                .c2AckUrl(properties.getProperty(C2NiFiProperties.C2_REST_URL_ACK_KEY, ""))
                .truststoreFilename(properties.getProperty(C2NiFiProperties.TRUSTSTORE_LOCATION_KEY, ""))
                .truststorePassword(properties.getProperty(C2NiFiProperties.TRUSTSTORE_PASSWORD_KEY, ""))
                .truststoreType(KeystoreType.valueOf(properties.getProperty(C2NiFiProperties.TRUSTSTORE_TYPE_KEY, "JKS")))
                .keystoreFilename(properties.getProperty(C2NiFiProperties.KEYSTORE_LOCATION_KEY, ""))
                .keystorePassword(properties.getProperty(C2NiFiProperties.KEYSTORE_PASSWORD_KEY, ""))
                .keystoreType(KeystoreType.valueOf(properties.getProperty(C2NiFiProperties.KEYSTORE_TYPE_KEY, "JKS")))
                .build();
    }

    public void start() {
        try {
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                if (c2Client != null) {
                    try {
                        // TODO exception handling for all the C2 Client interactions (IOExceptions, logger.error vs logger.warn, etc.)
                        C2Heartbeat c2Heartbeat = generateHeartbeat();
                        FlowUpdateInfo flowUpdateInfo = updateInfo.get();
                        if (flowUpdateInfo != null) {
                            final String flowId = flowUpdateInfo.getFlowId();
                            logger.trace("Determined that current flow id is {}.", flowId);
                            c2Heartbeat.getFlowInfo().setFlowId(flowId);
                        }
                        c2Client.publishHeartbeat(c2Heartbeat)
                            .ifPresent(this::processResponse);
                    } catch (IOException ioe) {
                        // TODO
                        logger.error("C2 Error", ioe);
                    }
                }
            }, initialDelay, period, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Could not start status reporter", e);
            throw new RuntimeException(e);
        }
    }

    private void processResponse(C2HeartbeatResponse response) {
        List<C2Operation> requestedOperations = response.getRequestedOperations();
        if (CollectionUtils.isNotEmpty(requestedOperations)) {
            logger.info("Received {} operations from the C2 server", requestedOperations.size());
            handleRequestedOperations(requestedOperations);
        } else {
            logger.trace("No operations received from the C2 server in the server. Nothing to do.");
        }
    }

    private void handleRequestedOperations(List<C2Operation> requestedOperations) {
        for (C2Operation requestedOperation : requestedOperations) {
            C2OperationAck operationAck = new C2OperationAck();
            if (requestedOperation.getOperation().equals(OperationType.UPDATE) && requestedOperation.getOperand().equals(OperandType.CONFIGURATION)) {
                String opIdentifier = Optional.ofNullable(requestedOperation.getIdentifier())
                    .orElse(EMPTY);
                String updateLocation = Optional.ofNullable(requestedOperation.getArgs())
                    .map(map -> map.get(LOCATION))
                    .orElse(EMPTY);

                final FlowUpdateInfo fui = new FlowUpdateInfo(updateLocation, opIdentifier);
                final FlowUpdateInfo currentFui = updateInfo.get();
                if (currentFui == null || !currentFui.getFlowId().equals(fui.getFlowId())) {
                    logger.info("Will perform flow update from {} for command #{}.  Previous flow id was {} with new id {}", updateLocation, opIdentifier,
                            currentFui == null ? "not set" : currentFui.getFlowId(), fui.getFlowId());
                    updateInfo.set(fui);
                } else {
                    logger.info("Flow is current...");
                }
                updateInfo.set(fui);
                ByteBuffer updateContent = c2Client.retrieveUpdateContent(fui);
                if (updateContent != null) {
                    // TODO processUpdateContent(ByteBuffer updateContentByteBuffer);
                }

                operationAck.setOperationId(fui.getRequestId());
            } // else other operations
            c2Client.acknowledgeOperation(operationAck);
        }
    }

    /**
     * Stops the associated client
     */
    public void stop() {
        try {
            scheduledExecutorService.shutdown();
            scheduledExecutorService.awaitTermination(termination_wait, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {
            // Shutting down anyway
        }
    }

    private C2Heartbeat generateHeartbeat() throws IOException {
        final C2Heartbeat heartbeat = new C2Heartbeat();
        // Agent Info
        final AgentInfo agentInfo = generateAgentInfo();
        final DeviceInfo deviceInfo = generateDeviceInfo();
        final FlowInfo flowInfo = generateFlowInfo();

        heartbeat.setCreated(new Date().getTime());
        // Populate heartbeat
        heartbeat.setAgentInfo(agentInfo);
        heartbeat.setDeviceInfo(deviceInfo);
        // Update flow information
        heartbeat.setFlowInfo(flowInfo);
        heartbeat.setCreated(new Date().getTime());

        return heartbeat;
    }

    private FlowInfo generateFlowInfo() {
        // Populate FlowInfo
        final FlowInfo flowInfo = new FlowInfo();
        final ProcessGroupStatus rootProcessGroupStatus = flowController.getEventAccess().getGroupStatus("root");

        if (rootProcessGroupStatus == null) {
            return flowInfo;
        }

        // Handle populating queues
        final Collection<ConnectionStatus> connectionStatuses = rootProcessGroupStatus.getConnectionStatus();

        final Map<String, FlowQueueStatus> processGroupStatus = new HashMap<>();
        for (ConnectionStatus connectionStatus : connectionStatuses) {
            final FlowQueueStatus flowQueueStatus = new FlowQueueStatus();

            flowQueueStatus.setSize((long) connectionStatus.getQueuedCount());
            flowQueueStatus.setSizeMax(connectionStatus.getBackPressureObjectThreshold());

            flowQueueStatus.setDataSize(connectionStatus.getQueuedBytes());
            flowQueueStatus.setDataSizeMax(connectionStatus.getBackPressureBytesThreshold());

            processGroupStatus.put(connectionStatus.getId(), flowQueueStatus);
        }
        flowInfo.setQueues(processGroupStatus);

        return flowInfo;
    }

    static SystemInfo generateSystemInfo() {
        final SystemInfo systemInfo = new SystemInfo();
        systemInfo.setMachineArch(System.getProperty("os.arch"));
        systemInfo.setPhysicalMem(Runtime.getRuntime().maxMemory());
        systemInfo.setvCores(Runtime.getRuntime().availableProcessors());

        return systemInfo;
    }

    private DeviceInfo generateDeviceInfo() {
        // Populate DeviceInfo
        final DeviceInfo deviceInfo = new DeviceInfo();
        deviceInfo.setNetworkInfo(generateNetworkInfo());
        deviceInfo.setIdentifier(getDeviceIdentifier(deviceInfo.getNetworkInfo()));
        deviceInfo.setSystemInfo(generateSystemInfo());
        return deviceInfo;
    }

    String getDeviceIdentifier(NetworkInfo networkInfo) {

        if (deviceIdentifierRef.get() == null) {
            if (networkInfo.getDeviceId() != null) {
                try {
                    final NetworkInterface netInterface = NetworkInterface.getByName(networkInfo.getDeviceId());
                    byte[] hardwareAddress = netInterface.getHardwareAddress();
                    final StringBuilder macBuilder = new StringBuilder();
                    if (hardwareAddress != null) {
                        for (int i = 0; i < hardwareAddress.length; i++) {
                            macBuilder.append(String.format("%02X", hardwareAddress[i]));
                        }
                    }
                    deviceIdentifierRef.set(macBuilder.toString());
                    return deviceIdentifierRef.get();
                } catch (Exception e) {
                    logger.warn("Could not determine device identifier.  Generating a unique ID", e);
                }
            }

            final File idFile = new File(getConfDirectory(), DEVICE_IDENTIFIER_FILENAME);
            synchronized (this) {
                deviceIdentifierRef.set(new PersistentUuidGenerator(idFile).generate());
            }
        }
        return deviceIdentifierRef.get();
    }

    static NetworkInfo generateNetworkInfo() {
        final NetworkInfo networkInfo = new NetworkInfo();
        try {
            // Determine all interfaces
            final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

            final Set<NetworkInterface> operationIfaces = new HashSet<>();

            // Determine eligible interfaces
            while (networkInterfaces.hasMoreElements()) {
                final NetworkInterface networkInterface = networkInterfaces.nextElement();
                if (!networkInterface.isLoopback() && networkInterface.isUp()) {
                    operationIfaces.add(networkInterface);
                }
            }
            logger.trace("Have {} interfaces with names {}", operationIfaces.size(),
                    operationIfaces.stream()
                            .map(NetworkInterface::getName)
                            .collect(Collectors.toSet())
            );

            if (!operationIfaces.isEmpty()) {
                if (operationIfaces.size() > 1) {
                    logger.debug("Instance has multiple interfaces.  Generated information may be non-deterministic.");
                }

                NetworkInterface iface = operationIfaces.iterator().next();
                final StringBuilder macSb = new StringBuilder();
                byte[] hardwareAddress = iface.getHardwareAddress();
                if (hardwareAddress != null) {
                    for (int i = 0; i < hardwareAddress.length; i++) {
                        macSb.append(String.format("%02X", hardwareAddress[i]));
                    }
                }
                final String macString = macSb.toString();
                Enumeration<InetAddress> inetAddresses = iface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    String hostAddress = inetAddress.getHostAddress();
                    String hostName = inetAddress.getHostName();
                    byte[] address = inetAddress.getAddress();
                    String canonicalHostName = inetAddress.getCanonicalHostName();

                    networkInfo.setDeviceId(iface.getName());
                    networkInfo.setHostname(hostName);
                    networkInfo.setIpAddress(hostAddress);
                }
            }
        } catch (
                Exception e) {
            logger.error("Had execption determining network information", e);
        }
        return networkInfo;
    }

    AgentInfo generateAgentInfo() {
        final AgentInfo agentInfo = new AgentInfo();

        // Populate AgentInfo
        agentInfo.setAgentClass(clientConfig.getAgentClass());
        if (agentIdentifierRef.get() == null) {
            final String rawAgentIdentifer = clientConfig.getAgentIdentifier();
            if (isNotBlank(rawAgentIdentifer)) {
                agentIdentifierRef.set(rawAgentIdentifer.trim());
            } else {
                final File idFile = new File(getConfDirectory(), AGENT_IDENTIFIER_FILENAME);
                synchronized (this) {
                    agentIdentifierRef.set(new PersistentUuidGenerator(idFile).generate());
                }
            }
        }
        agentInfo.setIdentifier(agentIdentifierRef.get());

        final AgentStatus agentStatus = new AgentStatus();
        agentStatus.setUptime(System.currentTimeMillis());

        if (flowController == null) {
            return agentInfo;
        }
        final SystemDiagnostics systemDiagnostics = flowController.getSystemDiagnostics();

        // Handle repositories
        final AgentRepositories repos = new AgentRepositories();
        final AgentRepositoryStatus flowFileRepoStatus = new AgentRepositoryStatus();
        final StorageUsage ffRepoStorageUsage = systemDiagnostics.getFlowFileRepositoryStorageUsage();
        flowFileRepoStatus.setDataSize(ffRepoStorageUsage.getUsedSpace());
        flowFileRepoStatus.setDataSizeMax(ffRepoStorageUsage.getTotalSpace());
        repos.setFlowFile(flowFileRepoStatus);

        final AgentRepositoryStatus provRepoStatus = new AgentRepositoryStatus();
        final Iterator<Map.Entry<String, StorageUsage>> provRepoStorageUsages = systemDiagnostics.getProvenanceRepositoryStorageUsage().entrySet().iterator();
        if (provRepoStorageUsages.hasNext()) {
            final StorageUsage provRepoStorageUsage = provRepoStorageUsages.next().getValue();
            provRepoStatus.setDataSize(provRepoStorageUsage.getUsedSpace());
            provRepoStatus.setDataSizeMax(provRepoStorageUsage.getTotalSpace());
        }
        repos.setProvenance(provRepoStatus);
        agentStatus.setRepositories(repos);

        agentInfo.setStatus(agentStatus);

        agentInfo.setAgentManifest(runtimeManifestService.getManifest());

        return agentInfo;
    }

    protected Collection<Processor> getComponents(final String type, final String identifier,
                                                  final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls) throws ProcessorInstantiationException {
        Set<ExtensionDefinition> processorExtensions = ExtensionManagerHolder.getExtensionManager().getExtensions(Processor.class);
        for (final ExtensionDefinition extensionDefinition : processorExtensions) {
            Class<?> extensionClass = extensionDefinition.getExtensionType();
            if (ConfigurableComponent.class.isAssignableFrom(extensionClass)) {
                final String extensionClassName = extensionClass.getCanonicalName();

                final org.apache.nifi.bundle.Bundle bundle = ExtensionManagerHolder.getExtensionManager().getBundle(extensionClass.getClassLoader());
                if (bundle == null) {
                    logger.warn("No coordinate found for {}, skipping...", extensionClassName);
                    continue;
                }
                final BundleCoordinate coordinate = bundle.getBundleDetails().getCoordinate();

                final String path = coordinate.getGroup() + "/" + coordinate.getId() + "/" + coordinate.getVersion() + "/" + extensionClassName;

                final Class<? extends ConfigurableComponent> componentClass = extensionClass.asSubclass(ConfigurableComponent.class);
                try {
                    logger.error("Documenting: " + componentClass);

                    // use temp components from ExtensionManager which should always be populated before doc generation
                    final String classType = componentClass.getCanonicalName();
                    logger.error("Getting temp component: {}", classType);

                    final ConfigurableComponent component = ExtensionManagerHolder.getExtensionManager().getTempComponent(classType, coordinate);
                    logger.error("found component: {}", component);

                    final List<org.apache.nifi.components.PropertyDescriptor> properties = component.getPropertyDescriptors();

                    for (org.apache.nifi.components.PropertyDescriptor descriptor : properties) {
                        logger.error("Found descriptor: {} -> {}", descriptor.getName(), descriptor.getDisplayName());
                    }

                } catch (Exception e) {
                    logger.warn("Unable to document: " + componentClass, e);
                }
            }
        }

        final org.apache.nifi.bundle.Bundle processorBundle = ExtensionManagerHolder.getExtensionManager().getBundle(bundleCoordinate);
        if (processorBundle == null) {
            throw new ProcessorInstantiationException("Unable to find bundle for coordinate " + bundleCoordinate.getCoordinate());
        }

        final ClassLoader ctxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final ClassLoader detectedClassLoaderForInstance = ExtensionManagerHolder.getExtensionManager().createInstanceClassLoader(type, identifier, processorBundle, additionalUrls);
            logger.error("Detected class loader for instance={}", detectedClassLoaderForInstance == null);
            final Class<?> rawClass = Class.forName(type, true, detectedClassLoaderForInstance);
            logger.error("Raw class {}", rawClass.getName());
            Thread.currentThread().setContextClassLoader(detectedClassLoaderForInstance);

            final Class<? extends Processor> processorClass = rawClass.asSubclass(Processor.class);
            logger.error("processorClass class {}", processorClass.getName());
            final Processor processor = processorClass.newInstance();
            Set<Processor> processors = new HashSet<>();
            processors.add(processor);
            return processors;
        } catch (final Throwable t) {
            throw new ProcessorInstantiationException(type, t);
        } finally {
            if (ctxClassLoader != null) {
                Thread.currentThread().setContextClassLoader(ctxClassLoader);
            }
        }
    }

    public static final String DEFAULT_CONFIG_FILE = "./conf/bootstrap.conf";

    private Properties getBootstrapProperties() {
        final Properties bootstrapProperties = new Properties();

        try (final FileInputStream fis = new FileInputStream(getBootstrapConfFile())) {
            bootstrapProperties.load(fis);
        } catch (Exception e) {
            logger.error("Could not locate bootstrap.conf file specified as {}", getBootstrapConfFile());
        }
        return bootstrapProperties;
    }

    public static File getBootstrapConfFile() {
        String configFilename = System.getProperty("org.apache.nifi.minifi.bootstrap.config.file");

        if (configFilename == null) {
            final String nifiHome = System.getenv("MINIFI_HOME");
            if (nifiHome != null) {
                final File nifiHomeFile = new File(nifiHome.trim());
                final File configFile = new File(nifiHomeFile, DEFAULT_CONFIG_FILE);
                configFilename = configFile.getAbsolutePath();
            }
        }

        if (configFilename == null) {
            configFilename = DEFAULT_CONFIG_FILE;
        }
        return new File(configFilename);
    }

    private File getConfDirectory() {
        final Properties bootstrapProperties = getBootstrapProperties();
        final String confDirectoryName = defaultIfBlank(bootstrapProperties.getProperty(CONF_DIR_KEY), "./conf");
        final File confDirectory = new File(confDirectoryName);
        if (!confDirectory.exists() || !confDirectory.isDirectory()) {
            throw new IllegalStateException("Specified conf directory " + confDirectoryName + " does not exist or is not a directory.");
        }
        return confDirectory;
    }
}
