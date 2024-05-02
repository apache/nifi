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

package org.apache.nifi.minifi.c2;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_AGENT_CLASS;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_AGENT_HEARTBEAT_PERIOD;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_AGENT_IDENTIFIER;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_ASSET_DIRECTORY;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_BOOTSTRAP_ACKNOWLEDGE_TIMEOUT;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_CONFIG_DIRECTORY;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_FULL_HEARTBEAT;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_KEEP_ALIVE_DURATION;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_MAX_IDLE_CONNECTIONS;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_REQUEST_COMPRESSION;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_REST_CALL_TIMEOUT;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_REST_CONNECTION_TIMEOUT;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_REST_HTTP_HEADERS;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_REST_PATH_ACKNOWLEDGE;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_REST_PATH_BASE;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_REST_PATH_HEARTBEAT;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_REST_READ_TIMEOUT;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_REST_URL;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_REST_URL_ACK;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_RUNTIME_MANIFEST_IDENTIFIER;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_RUNTIME_TYPE;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_SECURITY_KEYSTORE_LOCATION;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_SECURITY_KEYSTORE_PASSWORD;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_SECURITY_KEYSTORE_TYPE;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_SECURITY_TRUSTSTORE_LOCATION;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_SECURITY_TRUSTSTORE_PASSWORD;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_SECURITY_TRUSTSTORE_TYPE;
import static org.apache.nifi.util.FormatUtils.getPreciseTimeDuration;
import static org.apache.nifi.util.NiFiProperties.FLOW_CONFIGURATION_FILE;
import static org.apache.nifi.util.NiFiProperties.SENSITIVE_PROPS_ALGORITHM;
import static org.apache.nifi.util.NiFiProperties.SENSITIVE_PROPS_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.bootstrap.BootstrapCommunicator;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.http.C2HttpClient;
import org.apache.nifi.c2.client.service.C2HeartbeatFactory;
import org.apache.nifi.c2.client.service.C2HeartbeatManager;
import org.apache.nifi.c2.client.service.C2OperationManager;
import org.apache.nifi.c2.client.service.FlowIdHolder;
import org.apache.nifi.c2.client.service.ManifestHashProvider;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.client.service.operation.C2OperationHandlerProvider;
import org.apache.nifi.c2.client.service.operation.DescribeManifestOperationHandler;
import org.apache.nifi.c2.client.service.operation.EmptyOperandPropertiesProvider;
import org.apache.nifi.c2.client.service.operation.OperandPropertiesProvider;
import org.apache.nifi.c2.client.service.operation.OperationQueueDAO;
import org.apache.nifi.c2.client.service.operation.SupportedOperationsProvider;
import org.apache.nifi.c2.client.service.operation.TransferDebugOperationHandler;
import org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler;
import org.apache.nifi.c2.client.service.operation.UpdateConfigurationOperationHandler;
import org.apache.nifi.c2.client.service.operation.UpdateConfigurationStrategy;
import org.apache.nifi.c2.client.service.operation.UpdatePropertiesOperationHandler;
import org.apache.nifi.c2.protocol.api.AgentManifest;
import org.apache.nifi.c2.protocol.api.AgentRepositories;
import org.apache.nifi.c2.protocol.api.AgentRepositoryStatus;
import org.apache.nifi.c2.protocol.api.FlowQueueStatus;
import org.apache.nifi.c2.serializer.C2JacksonSerializer;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestParser;
import org.apache.nifi.extension.manifest.parser.jaxb.JAXBExtensionManifestParser;
import org.apache.nifi.manifest.RuntimeManifestService;
import org.apache.nifi.manifest.StandardRuntimeManifestService;
import org.apache.nifi.minifi.c2.command.DefaultUpdateConfigurationStrategy;
import org.apache.nifi.minifi.c2.command.PropertiesPersister;
import org.apache.nifi.minifi.c2.command.TransferDebugCommandHelper;
import org.apache.nifi.minifi.c2.command.UpdateAssetCommandHelper;
import org.apache.nifi.minifi.c2.command.UpdatePropertiesPropertyProvider;
import org.apache.nifi.minifi.commons.api.MiNiFiProperties;
import org.apache.nifi.minifi.commons.service.FlowPropertyEncryptor;
import org.apache.nifi.minifi.commons.service.StandardFlowEnrichService;
import org.apache.nifi.minifi.commons.service.StandardFlowPropertyEncryptor;
import org.apache.nifi.minifi.commons.service.StandardFlowSerDeService;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C2NifiClientService {

    private static final Logger LOGGER = LoggerFactory.getLogger(C2NifiClientService.class);
    private static final String ROOT_GROUP_ID = "root";
    private static final Integer TERMINATION_WAIT = 5000;
    private static final Long INITIAL_HEARTBEAT_DELAY_MS = 10000L;

    private final ScheduledExecutorService heartbeatManagerExecutorService;
    private final ExecutorService operationManagerExecutorService;

    private final FlowController flowController;
    private final ExtensionManifestParser extensionManifestParser;
    private final RuntimeManifestService runtimeManifestService;
    private final SupportedOperationsProvider supportedOperationsProvider;
    private final C2HeartbeatManager c2HeartbeatManager;
    private final C2OperationManager c2OperationManager;

    private final long heartbeatPeriod;

    public C2NifiClientService(NiFiProperties niFiProperties, FlowController flowController, BootstrapCommunicator bootstrapCommunicator, FlowService flowService) {
        this.heartbeatManagerExecutorService = newScheduledThreadPool(1);
        this.operationManagerExecutorService = newSingleThreadExecutor();

        this.extensionManifestParser = new JAXBExtensionManifestParser();

        C2ClientConfig clientConfig = generateClientConfig(niFiProperties);

        this.runtimeManifestService = new StandardRuntimeManifestService(
            ExtensionManagerHolder.getExtensionManager(),
            extensionManifestParser,
            clientConfig.getRuntimeManifestIdentifier(),
            clientConfig.getRuntimeType()
        );
        this.heartbeatPeriod = clientConfig.getHeartbeatPeriod();
        this.flowController = flowController;

        C2HttpClient client = C2HttpClient.create(clientConfig, new C2JacksonSerializer());
        FlowIdHolder flowIdHolder = new FlowIdHolder(clientConfig.getConfDirectory());
        C2HeartbeatFactory heartbeatFactory = new C2HeartbeatFactory(clientConfig, flowIdHolder, new ManifestHashProvider());
        String bootstrapConfigFileLocation = niFiProperties.getProperty("nifi.minifi.bootstrap.file");
        C2OperationHandlerProvider c2OperationHandlerProvider = c2OperationHandlerProvider(niFiProperties, flowController, flowService, flowIdHolder,
            client, heartbeatFactory, bootstrapConfigFileLocation, clientConfig.getC2AssetDirectory());

        this.supportedOperationsProvider = new SupportedOperationsProvider(c2OperationHandlerProvider.getHandlers());

        OperationQueueDAO operationQueueDAO =
            new FileBasedOperationQueueDAO(niFiProperties.getProperty("org.apache.nifi.minifi.bootstrap.config.pid.dir", "bin"), new ObjectMapper());
        ReentrantLock heartbeatLock = new ReentrantLock();
        BootstrapC2OperationRestartHandler c2OperationRestartHandler = new BootstrapC2OperationRestartHandler(bootstrapCommunicator, clientConfig.getBootstrapAcknowledgeTimeout());

        this.c2OperationManager = new C2OperationManager(
            client, c2OperationHandlerProvider, heartbeatLock, operationQueueDAO, c2OperationRestartHandler);
        this.c2HeartbeatManager = new C2HeartbeatManager(
            client, heartbeatFactory, heartbeatLock, generateRuntimeInfo(), c2OperationManager);
    }

    private C2ClientConfig generateClientConfig(NiFiProperties properties) {
        return new C2ClientConfig.Builder()
            .agentClass(properties.getProperty(C2_AGENT_CLASS.getKey(), C2_AGENT_CLASS.getDefaultValue()))
            .agentIdentifier(properties.getProperty(C2_AGENT_IDENTIFIER.getKey()))
            .fullHeartbeat(parseBoolean(properties.getProperty(C2_FULL_HEARTBEAT.getKey(), C2_FULL_HEARTBEAT.getDefaultValue())))
            .heartbeatPeriod(parseLong(properties.getProperty(C2_AGENT_HEARTBEAT_PERIOD.getKey(), C2_AGENT_HEARTBEAT_PERIOD.getDefaultValue())))
            .connectTimeout(durationPropertyInMilliSecs(properties, C2_REST_CONNECTION_TIMEOUT))
            .readTimeout(durationPropertyInMilliSecs(properties, C2_REST_READ_TIMEOUT))
            .callTimeout(durationPropertyInMilliSecs(properties, C2_REST_CALL_TIMEOUT))
            .maxIdleConnections(parseInt(properties.getProperty(C2_MAX_IDLE_CONNECTIONS.getKey(), C2_MAX_IDLE_CONNECTIONS.getDefaultValue())))
            .keepAliveDuration(durationPropertyInMilliSecs(properties, C2_KEEP_ALIVE_DURATION))
            .httpHeaders(properties.getProperty(C2_REST_HTTP_HEADERS.getKey(), C2_REST_HTTP_HEADERS.getDefaultValue()))
            .c2RequestCompression(properties.getProperty(C2_REQUEST_COMPRESSION.getKey(), C2_REQUEST_COMPRESSION.getDefaultValue()))
            .c2AssetDirectory(properties.getProperty(C2_ASSET_DIRECTORY.getKey(), C2_ASSET_DIRECTORY.getDefaultValue()))
            .confDirectory(properties.getProperty(C2_CONFIG_DIRECTORY.getKey(), C2_CONFIG_DIRECTORY.getDefaultValue()))
            .runtimeManifestIdentifier(properties.getProperty(C2_RUNTIME_MANIFEST_IDENTIFIER.getKey(), C2_RUNTIME_MANIFEST_IDENTIFIER.getDefaultValue()))
            .runtimeType(properties.getProperty(C2_RUNTIME_TYPE.getKey(), C2_RUNTIME_TYPE.getDefaultValue()))
            .truststoreFilename(properties.getProperty(C2_SECURITY_TRUSTSTORE_LOCATION.getKey(), C2_SECURITY_TRUSTSTORE_LOCATION.getDefaultValue()))
            .truststorePassword(properties.getProperty(C2_SECURITY_TRUSTSTORE_PASSWORD.getKey(), C2_SECURITY_TRUSTSTORE_PASSWORD.getDefaultValue()))
            .truststoreType(properties.getProperty(C2_SECURITY_TRUSTSTORE_TYPE.getKey(), C2_SECURITY_TRUSTSTORE_TYPE.getDefaultValue()))
            .keystoreFilename(properties.getProperty(C2_SECURITY_KEYSTORE_LOCATION.getKey(), C2_SECURITY_KEYSTORE_LOCATION.getDefaultValue()))
            .keystorePassword(properties.getProperty(C2_SECURITY_KEYSTORE_PASSWORD.getKey(), C2_SECURITY_KEYSTORE_PASSWORD.getDefaultValue()))
            .keystoreType(properties.getProperty(C2_SECURITY_KEYSTORE_TYPE.getKey(), C2_SECURITY_KEYSTORE_TYPE.getDefaultValue()))
            .c2Url(properties.getProperty(C2_REST_URL.getKey(), C2_REST_URL.getDefaultValue()))
            .c2AckUrl(properties.getProperty(C2_REST_URL_ACK.getKey(), C2_REST_URL_ACK.getDefaultValue()))
            .c2RestPathBase(properties.getProperty(C2_REST_PATH_BASE.getKey(), C2_REST_PATH_BASE.getDefaultValue()))
            .c2RestPathHeartbeat(properties.getProperty(C2_REST_PATH_HEARTBEAT.getKey(), C2_REST_PATH_HEARTBEAT.getDefaultValue()))
            .c2RestPathAcknowledge(properties.getProperty(C2_REST_PATH_ACKNOWLEDGE.getKey(), C2_REST_PATH_ACKNOWLEDGE.getDefaultValue()))
            .bootstrapAcknowledgeTimeout(durationPropertyInMilliSecs(properties, C2_BOOTSTRAP_ACKNOWLEDGE_TIMEOUT))
            .build();
    }

    private long durationPropertyInMilliSecs(NiFiProperties properties, MiNiFiProperties property) {
        return (long) getPreciseTimeDuration(properties.getProperty(property.getKey(), property.getDefaultValue()), MILLISECONDS);
    }

    private C2OperationHandlerProvider c2OperationHandlerProvider(NiFiProperties niFiProperties, FlowController flowController, FlowService flowService,
                                                                  FlowIdHolder flowIdHolder, C2HttpClient client, C2HeartbeatFactory heartbeatFactory,
                                                                  String bootstrapConfigFileLocation, String c2AssetDirectory) {
        OperandPropertiesProvider emptyOperandPropertiesProvider = new EmptyOperandPropertiesProvider();
        TransferDebugCommandHelper transferDebugCommandHelper = new TransferDebugCommandHelper(niFiProperties);
        UpdateAssetCommandHelper updateAssetCommandHelper = new UpdateAssetCommandHelper(c2AssetDirectory);
        updateAssetCommandHelper.createAssetDirectory();
        UpdatePropertiesPropertyProvider updatePropertiesPropertyProvider = new UpdatePropertiesPropertyProvider(bootstrapConfigFileLocation);
        PropertiesPersister propertiesPersister = new PropertiesPersister(updatePropertiesPropertyProvider, bootstrapConfigFileLocation);

        FlowPropertyEncryptor flowPropertyEncryptor = new StandardFlowPropertyEncryptor(
            new PropertyEncryptorBuilder(niFiProperties.getProperty(SENSITIVE_PROPS_KEY))
                .setAlgorithm(niFiProperties.getProperty(SENSITIVE_PROPS_ALGORITHM)).build(),
            runtimeManifestService.getManifest());
        UpdateConfigurationStrategy updateConfigurationStrategy = new DefaultUpdateConfigurationStrategy(flowController, flowService,
            new StandardFlowEnrichService(niFiProperties), flowPropertyEncryptor,
            StandardFlowSerDeService.defaultInstance(), niFiProperties.getProperty(FLOW_CONFIGURATION_FILE));

        return new C2OperationHandlerProvider(List.of(
            new UpdateConfigurationOperationHandler(client, flowIdHolder, updateConfigurationStrategy, emptyOperandPropertiesProvider),
            new DescribeManifestOperationHandler(heartbeatFactory, this::generateRuntimeInfo, emptyOperandPropertiesProvider),
            TransferDebugOperationHandler.create(client, emptyOperandPropertiesProvider,
                transferDebugCommandHelper.debugBundleFiles(), transferDebugCommandHelper::excludeSensitiveText),
            UpdateAssetOperationHandler.create(client, emptyOperandPropertiesProvider,
                updateAssetCommandHelper::assetUpdatePrecondition, updateAssetCommandHelper::assetPersistFunction),
            new UpdatePropertiesOperationHandler(updatePropertiesPropertyProvider, propertiesPersister::persistProperties)
        ));
    }

    public void start() {
        operationManagerExecutorService.execute(c2OperationManager);
        LOGGER.debug("Scheduling heartbeats with {} ms periodicity", heartbeatPeriod);
        heartbeatManagerExecutorService.scheduleAtFixedRate(c2HeartbeatManager, INITIAL_HEARTBEAT_DELAY_MS, heartbeatPeriod, MILLISECONDS);
    }

    public void stop() {
        heartbeatManagerExecutorService.shutdown();
        try {
            if (!heartbeatManagerExecutorService.awaitTermination(TERMINATION_WAIT, MILLISECONDS)) {
                heartbeatManagerExecutorService.shutdownNow();
            }
        } catch (InterruptedException ignore) {
            LOGGER.info("Stopping C2 heartbeat executor service was interrupted, forcing shutdown");
            heartbeatManagerExecutorService.shutdownNow();
        }
        operationManagerExecutorService.shutdown();
        try {
            if (!operationManagerExecutorService.awaitTermination(TERMINATION_WAIT, MILLISECONDS)) {
                operationManagerExecutorService.shutdownNow();
            }
        } catch (InterruptedException ignore) {
            LOGGER.info("Stopping C2 operation executor service was interrupted, forcing shutdown");
            operationManagerExecutorService.shutdownNow();
        }
    }

    private RuntimeInfoWrapper generateRuntimeInfo() {
        AgentManifest agentManifest = new AgentManifest(runtimeManifestService.getManifest());
        agentManifest.setSupportedOperations(supportedOperationsProvider.getSupportedOperations());
        return new RuntimeInfoWrapper(getAgentRepositories(), agentManifest, getQueueStatus());
    }

    private AgentRepositories getAgentRepositories() {
        SystemDiagnostics systemDiagnostics = flowController.getSystemDiagnostics();

        AgentRepositoryStatus agentFlowRepositoryStatus = ofNullable(systemDiagnostics.getFlowFileRepositoryStorageUsage())
            .map(flowFileRepositoryStorageUsage -> {
                AgentRepositoryStatus flowRepositoryStatus = new AgentRepositoryStatus();
                flowRepositoryStatus.setDataSize(flowFileRepositoryStorageUsage.getUsedSpace());
                flowRepositoryStatus.setDataSizeMax(flowFileRepositoryStorageUsage.getTotalSpace());
                return flowRepositoryStatus;
            })
            .orElseGet(AgentRepositoryStatus::new);

        AgentRepositoryStatus agentProvenanceRepositoryStatus = systemDiagnostics.getProvenanceRepositoryStorageUsage().entrySet().stream()
            .findFirst()
            .map(Map.Entry::getValue)
            .map(provRepoStorageUsage -> {
                AgentRepositoryStatus provenanceRepositoryStatus = new AgentRepositoryStatus();
                provenanceRepositoryStatus.setDataSize(provRepoStorageUsage.getUsedSpace());
                provenanceRepositoryStatus.setDataSizeMax(provRepoStorageUsage.getTotalSpace());
                return provenanceRepositoryStatus;
            })
            .orElseGet(AgentRepositoryStatus::new);

        AgentRepositories agentRepositories = new AgentRepositories();
        agentRepositories.setFlowFile(agentFlowRepositoryStatus);
        agentRepositories.setProvenance(agentProvenanceRepositoryStatus);
        return agentRepositories;
    }

    private Map<String, FlowQueueStatus> getQueueStatus() {
        return flowController.getEventAccess()
            .getGroupStatus(ROOT_GROUP_ID)
            .getConnectionStatus()
            .stream()
            .map(connectionStatus -> {
                FlowQueueStatus flowQueueStatus = new FlowQueueStatus();
                flowQueueStatus.setSize((long) connectionStatus.getQueuedCount());
                flowQueueStatus.setSizeMax(connectionStatus.getBackPressureObjectThreshold());
                flowQueueStatus.setDataSize(connectionStatus.getQueuedBytes());
                flowQueueStatus.setDataSizeMax(connectionStatus.getBackPressureBytesThreshold());
                return Pair.of(connectionStatus.getId(), flowQueueStatus);
            })
            .collect(toMap(Pair::getKey, Pair::getValue));
    }
}