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

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_AGENT_CLASS;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_AGENT_HEARTBEAT_PERIOD;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_AGENT_IDENTIFIER;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.C2_ASSET_DIRECTORY;
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
import static org.apache.nifi.util.NiFiProperties.FLOW_CONFIGURATION_FILE;
import static org.apache.nifi.util.NiFiProperties.SENSITIVE_PROPS_ALGORITHM;
import static org.apache.nifi.util.NiFiProperties.SENSITIVE_PROPS_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.bootstrap.BootstrapCommunicator;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.http.C2HttpClient;
import org.apache.nifi.c2.client.service.C2ClientService;
import org.apache.nifi.c2.client.service.C2HeartbeatFactory;
import org.apache.nifi.c2.client.service.FlowIdHolder;
import org.apache.nifi.c2.client.service.ManifestHashProvider;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.client.service.operation.C2OperationHandlerProvider;
import org.apache.nifi.c2.client.service.operation.DescribeManifestOperationHandler;
import org.apache.nifi.c2.client.service.operation.EmptyOperandPropertiesProvider;
import org.apache.nifi.c2.client.service.operation.OperandPropertiesProvider;
import org.apache.nifi.c2.client.service.operation.OperationQueue;
import org.apache.nifi.c2.client.service.operation.RequestedOperationDAO;
import org.apache.nifi.c2.client.service.operation.SupportedOperationsProvider;
import org.apache.nifi.c2.client.service.operation.TransferDebugOperationHandler;
import org.apache.nifi.c2.client.service.operation.UpdateAssetOperationHandler;
import org.apache.nifi.c2.client.service.operation.UpdateConfigurationOperationHandler;
import org.apache.nifi.c2.client.service.operation.UpdateConfigurationStrategy;
import org.apache.nifi.c2.client.service.operation.UpdatePropertiesOperationHandler;
import org.apache.nifi.c2.protocol.api.AgentManifest;
import org.apache.nifi.c2.protocol.api.AgentRepositories;
import org.apache.nifi.c2.protocol.api.AgentRepositoryStatus;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
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
import org.apache.nifi.minifi.commons.api.MiNiFiCommandState;
import org.apache.nifi.minifi.commons.service.FlowPropertyEncryptor;
import org.apache.nifi.minifi.commons.service.StandardFlowEnrichService;
import org.apache.nifi.minifi.commons.service.StandardFlowPropertyEncryptor;
import org.apache.nifi.minifi.commons.service.StandardFlowSerDeService;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C2NifiClientService {

    private static final Logger LOGGER = LoggerFactory.getLogger(C2NifiClientService.class);
    private static final String ROOT_GROUP_ID = "root";
    private static final Long INITIAL_DELAY = 10000L;
    private static final Integer TERMINATION_WAIT = 5000;
    private static final int MINIFI_RESTART_TIMEOUT_SECONDS = 60;
    private static final String ACKNOWLEDGE_OPERATION = "ACKNOWLEDGE_OPERATION";
    private static final int IS_ACK_RECEIVED_POLL_INTERVAL = 1000;
    private static final int MAX_WAIT_FOR_BOOTSTRAP_ACK_MS = 20000;

    private static final Map<MiNiFiCommandState, OperationState> OPERATION_STATE_MAP = Map.of(
        MiNiFiCommandState.FULLY_APPLIED, OperationState.FULLY_APPLIED,
        MiNiFiCommandState.NO_OPERATION, OperationState.NO_OPERATION,
        MiNiFiCommandState.NOT_APPLIED_WITH_RESTART, OperationState.NOT_APPLIED,
        MiNiFiCommandState.NOT_APPLIED_WITHOUT_RESTART, OperationState.NOT_APPLIED);

    private final C2ClientService c2ClientService;
    private final FlowController flowController;
    private final ScheduledThreadPoolExecutor heartbeatExecutorService;
    private final ScheduledThreadPoolExecutor bootstrapAcknowledgeExecutorService;
    private final ExtensionManifestParser extensionManifestParser;
    private final RuntimeManifestService runtimeManifestService;
    private final SupportedOperationsProvider supportedOperationsProvider;
    private final RequestedOperationDAO requestedOperationDAO;
    private final BootstrapCommunicator bootstrapCommunicator;
    private final long heartbeatPeriod;

    private volatile boolean ackReceived = false;

    public C2NifiClientService(NiFiProperties niFiProperties, FlowController flowController, BootstrapCommunicator bootstrapCommunicator, FlowService flowService) {
        this.heartbeatExecutorService = new ScheduledThreadPoolExecutor(1);
        this.bootstrapAcknowledgeExecutorService = new ScheduledThreadPoolExecutor(1);
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

        this.requestedOperationDAO = new FileBasedRequestedOperationDAO(niFiProperties.getProperty("org.apache.nifi.minifi.bootstrap.config.pid.dir", "bin"), new ObjectMapper());
        String bootstrapConfigFileLocation = niFiProperties.getProperty("nifi.minifi.bootstrap.file");

        C2OperationHandlerProvider c2OperationHandlerProvider = c2OperationHandlerProvider(niFiProperties, flowController, flowService, flowIdHolder,
            client, heartbeatFactory, bootstrapConfigFileLocation, clientConfig.getC2AssetDirectory());

        this.c2ClientService = new C2ClientService(client, heartbeatFactory, c2OperationHandlerProvider, requestedOperationDAO, this::registerOperation);
        this.supportedOperationsProvider = new SupportedOperationsProvider(c2OperationHandlerProvider.getHandlers());

        this.bootstrapCommunicator = bootstrapCommunicator;
        this.bootstrapCommunicator.registerMessageHandler(ACKNOWLEDGE_OPERATION, (params, output) -> acknowledgeHandler(params));
    }

    private C2ClientConfig generateClientConfig(NiFiProperties properties) {
        return new C2ClientConfig.Builder()
            .agentClass(properties.getProperty(C2_AGENT_CLASS.getKey(), C2_AGENT_CLASS.getDefaultValue()))
            .agentIdentifier(properties.getProperty(C2_AGENT_IDENTIFIER.getKey()))
            .fullHeartbeat(Boolean.parseBoolean(properties.getProperty(C2_FULL_HEARTBEAT.getKey(), C2_FULL_HEARTBEAT.getDefaultValue())))
            .heartbeatPeriod(Long.parseLong(properties.getProperty(C2_AGENT_HEARTBEAT_PERIOD.getKey(),
                C2_AGENT_HEARTBEAT_PERIOD.getDefaultValue())))
            .connectTimeout((long) FormatUtils.getPreciseTimeDuration(properties.getProperty(C2_REST_CONNECTION_TIMEOUT.getKey(),
                C2_REST_CONNECTION_TIMEOUT.getDefaultValue()), TimeUnit.MILLISECONDS))
            .readTimeout((long) FormatUtils.getPreciseTimeDuration(properties.getProperty(C2_REST_READ_TIMEOUT.getKey(),
                C2_REST_READ_TIMEOUT.getDefaultValue()), TimeUnit.MILLISECONDS))
            .callTimeout((long) FormatUtils.getPreciseTimeDuration(properties.getProperty(C2_REST_CALL_TIMEOUT.getKey(),
                C2_REST_CALL_TIMEOUT.getDefaultValue()), TimeUnit.MILLISECONDS))
            .maxIdleConnections(Integer.parseInt(properties.getProperty(C2_MAX_IDLE_CONNECTIONS.getKey(), C2_MAX_IDLE_CONNECTIONS.getDefaultValue())))
            .keepAliveDuration((long) FormatUtils.getPreciseTimeDuration(properties.getProperty(C2_KEEP_ALIVE_DURATION.getKey(),
                C2_KEEP_ALIVE_DURATION.getDefaultValue()), TimeUnit.MILLISECONDS))
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
            .build();
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
        handleOngoingOperations();
        heartbeatExecutorService.scheduleAtFixedRate(() -> c2ClientService.sendHeartbeat(generateRuntimeInfo()), INITIAL_DELAY, heartbeatPeriod, TimeUnit.MILLISECONDS);
    }

    // need to be synchronized to prevent parallel run coming from acknowledgeHandler/ackTimeoutTask
    private synchronized void handleOngoingOperations() {
        Optional<OperationQueue> operationQueue = requestedOperationDAO.load();
        LOGGER.info("Handling ongoing operations: {}", operationQueue);
        if (operationQueue.isPresent()) {
            try {
                waitForAcknowledgeFromBootstrap();
                c2ClientService.handleRequestedOperations(operationQueue.get().getRemainingOperations());
            } catch (Exception e) {
                LOGGER.error("Failed to process c2 operations queue", e);
                c2ClientService.enableHeartbeat();
            }
        } else {
            c2ClientService.enableHeartbeat();
        }
    }

    private void waitForAcknowledgeFromBootstrap() {
        LOGGER.info("Waiting for ACK signal from Bootstrap");
        int currentWaitTime = 0;
        while (!ackReceived) {
            try {
                Thread.sleep(IS_ACK_RECEIVED_POLL_INTERVAL);
            } catch (InterruptedException e) {
                LOGGER.warn("Thread interrupted while waiting for Acknowledge");
            }
            currentWaitTime += IS_ACK_RECEIVED_POLL_INTERVAL;
            if (MAX_WAIT_FOR_BOOTSTRAP_ACK_MS <= currentWaitTime) {
                LOGGER.warn("Max wait time ({}) exceeded for waiting ack from bootstrap, skipping", MAX_WAIT_FOR_BOOTSTRAP_ACK_MS);
                break;
            }
        }
    }

    private void registerOperation(C2Operation c2Operation) {
        try {
            ackReceived = false;
            registerAcknowledgeTimeoutTask(c2Operation);
            String command = ofNullable(c2Operation.getOperand())
                .map(operand -> c2Operation.getOperation().name() + "_" + operand.name())
                .orElse(c2Operation.getOperation().name());
            bootstrapCommunicator.sendCommand(command);
        } catch (IOException e) {
            LOGGER.error("Failed to send operation to bootstrap", e);
            throw new UncheckedIOException(e);
        }
    }

    private void registerAcknowledgeTimeoutTask(C2Operation c2Operation) {
        bootstrapAcknowledgeExecutorService.schedule(() -> {
            if (!ackReceived) {
                LOGGER.info("Operation requiring restart is failed, and no restart/acknowledge is happened after {} seconds for {}. Handling remaining operations.",
                    MINIFI_RESTART_TIMEOUT_SECONDS, c2Operation);
                handleOngoingOperations();
            }
        }, MINIFI_RESTART_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void acknowledgeHandler(String[] params) {
        LOGGER.info("Received acknowledge message from bootstrap process");
        if (params.length < 1) {
            LOGGER.error("Invalid arguments coming from bootstrap, skipping acknowledging latest operation");
            return;
        }

        Optional<OperationQueue> optionalOperationQueue = requestedOperationDAO.load();
        ackReceived = true;
        if (optionalOperationQueue.isPresent()) {
            OperationQueue operationQueue = optionalOperationQueue.get();
            C2Operation c2Operation = operationQueue.getCurrentOperation();
            C2OperationAck c2OperationAck = new C2OperationAck();
            c2OperationAck.setOperationId(c2Operation.getIdentifier());
            C2OperationState c2OperationState = new C2OperationState();
            MiNiFiCommandState miNiFiCommandState = MiNiFiCommandState.valueOf(params[0]);
            OperationState state = OPERATION_STATE_MAP.get(miNiFiCommandState);
            c2OperationState.setState(state);
            c2OperationAck.setOperationState(c2OperationState);
            c2ClientService.sendAcknowledge(c2OperationAck);
            if (MiNiFiCommandState.NO_OPERATION == miNiFiCommandState || MiNiFiCommandState.NOT_APPLIED_WITHOUT_RESTART == miNiFiCommandState) {
                LOGGER.debug("No restart happened because of an error / the app was already in the desired state");
                handleOngoingOperations();
            }
        } else {
            LOGGER.error("Can not send acknowledge due to empty Operation Queue");
        }
    }

    public void stop() {
        bootstrapAcknowledgeExecutorService.shutdownNow();
        heartbeatExecutorService.shutdown();
        try {
            if (!heartbeatExecutorService.awaitTermination(TERMINATION_WAIT, TimeUnit.MILLISECONDS)) {
                heartbeatExecutorService.shutdownNow();
            }
        } catch (InterruptedException ignore) {
            LOGGER.info("Stopping C2 Client's thread was interrupted but shutting down anyway the C2NifiClientService");
            heartbeatExecutorService.shutdownNow();
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