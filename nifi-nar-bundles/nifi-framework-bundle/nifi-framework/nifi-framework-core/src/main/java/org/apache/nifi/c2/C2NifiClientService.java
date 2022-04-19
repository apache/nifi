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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.http.C2HttpClient;
import org.apache.nifi.c2.client.service.C2ClientService;
import org.apache.nifi.c2.client.service.C2HeartbeatFactory;
import org.apache.nifi.c2.client.service.FlowIdHolder;
import org.apache.nifi.c2.client.service.model.RuntimeInfoWrapper;
import org.apache.nifi.c2.protocol.api.AgentRepositories;
import org.apache.nifi.c2.protocol.api.AgentRepositoryStatus;
import org.apache.nifi.c2.protocol.api.FlowQueueStatus;
import org.apache.nifi.c2.serializer.C2JacksonSerializer;
import org.apache.nifi.controller.FlowController;;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.diagnostics.StorageUsage;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.extension.manifest.parser.ExtensionManifestParser;
import org.apache.nifi.extension.manifest.parser.jaxb.JAXBExtensionManifestParser;
import org.apache.nifi.manifest.RuntimeManifestService;
import org.apache.nifi.manifest.StandardRuntimeManifestService;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class C2NifiClientService {

    private static final Logger logger = LoggerFactory.getLogger(C2NifiClientService.class);
    private static final String DEFAULT_CONF_DIR = "./conf";
    private static final String TARGET_CONFIG_FILE = "/config-new.yml";
    private static final String ROOT_GROUP_ID = "root";
    private static final Long INITIAL_DELAY = 10000L;
    private static final Integer TERMINATION_WAIT = 5000;

    private final C2ClientService c2ClientService;

    private final FlowService flowService;
    private final FlowController flowController;
    private final String propertiesDir;
    private final ScheduledThreadPoolExecutor scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
    private final ExtensionManifestParser extensionManifestParser = new JAXBExtensionManifestParser();

    private final RuntimeManifestService runtimeManifestService;
    private final long heartbeatPeriod;

    public C2NifiClientService(final NiFiProperties niFiProperties, final FlowService flowService, final FlowController flowController) {
        C2ClientConfig clientConfig = generateClientConfig(niFiProperties);
        FlowIdHolder flowIdHolder = new FlowIdHolder(clientConfig.getConfDirectory());
        this.propertiesDir = niFiProperties.getProperty(NiFiProperties.PROPERTIES_FILE_PATH, null);
        this.runtimeManifestService = new StandardRuntimeManifestService(
            ExtensionManagerHolder.getExtensionManager(),
            extensionManifestParser,
            clientConfig.getRuntimeManifestIdentifier(),
            clientConfig.getRuntimeType()
        );
        this.heartbeatPeriod = clientConfig.getHeartbeatPeriod();
        this.flowService = flowService;
        this.flowController = flowController;
        this.c2ClientService = new C2ClientService(
            new C2HttpClient(clientConfig, new C2JacksonSerializer()),
            new C2HeartbeatFactory(clientConfig, flowIdHolder),
            flowIdHolder,
            this::updateFlowContent
        );
    }

    private C2ClientConfig generateClientConfig(NiFiProperties properties) {
        return new C2ClientConfig.Builder()
                .agentClass(properties.getProperty(C2NiFiProperties.C2_AGENT_CLASS_KEY, ""))
                .agentIdentifier(properties.getProperty(C2NiFiProperties.C2_AGENT_IDENTIFIER_KEY))
                .heartbeatPeriod(Long.parseLong(properties.getProperty(C2NiFiProperties.C2_AGENT_HEARTBEAT_PERIOD_KEY,
                    String.valueOf(C2NiFiProperties.C2_AGENT_DEFAULT_HEARTBEAT_PERIOD))))
                .connectTimeout((long) FormatUtils.getPreciseTimeDuration(properties.getProperty(C2NiFiProperties.C2_CONNECTION_TIMEOUT,
                    C2NiFiProperties.C2_DEFAULT_CONNECTION_TIMEOUT), TimeUnit.MILLISECONDS))
                .readTimeout((long) FormatUtils.getPreciseTimeDuration(properties.getProperty(C2NiFiProperties.C2_READ_TIMEOUT,
                    C2NiFiProperties.C2_DEFAULT_READ_TIMEOUT), TimeUnit.MILLISECONDS))
                .callTimeout((long) FormatUtils.getPreciseTimeDuration(properties.getProperty(C2NiFiProperties.C2_CALL_TIMEOUT,
                    C2NiFiProperties.C2_DEFAULT_CALL_TIMEOUT), TimeUnit.MILLISECONDS))
                .c2Url(properties.getProperty(C2NiFiProperties.C2_REST_URL_KEY, ""))
                .confDirectory(properties.getProperty(C2NiFiProperties.C2_CONFIG_DIRECTORY_KEY, DEFAULT_CONF_DIR))
                .runtimeManifestIdentifier(properties.getProperty(C2NiFiProperties.C2_RUNTIME_MANIFEST_IDENTIFIER_KEY, ""))
                .runtimeType(properties.getProperty(C2NiFiProperties.C2_RUNTIME_TYPE_KEY, ""))
                .c2AckUrl(properties.getProperty(C2NiFiProperties.C2_REST_URL_ACK_KEY, ""))
                .truststoreFilename(properties.getProperty(C2NiFiProperties.TRUSTSTORE_LOCATION_KEY, ""))
                .truststorePassword(properties.getProperty(C2NiFiProperties.TRUSTSTORE_PASSWORD_KEY, ""))
                .truststoreType(properties.getProperty(C2NiFiProperties.TRUSTSTORE_TYPE_KEY, "JKS"))
                .keystoreFilename(properties.getProperty(C2NiFiProperties.KEYSTORE_LOCATION_KEY, ""))
                .keystorePassword(properties.getProperty(C2NiFiProperties.KEYSTORE_PASSWORD_KEY, ""))
                .keystoreType(properties.getProperty(C2NiFiProperties.KEYSTORE_TYPE_KEY, "JKS"))
                .build();
    }

    public void start() {
        try {
            scheduledExecutorService.scheduleAtFixedRate(() -> c2ClientService.sendHeartbeat(generateRuntimeInfo()), INITIAL_DELAY, heartbeatPeriod, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Could not start C2 Client Heartbeat Reporting", e);
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            scheduledExecutorService.shutdown();
            scheduledExecutorService.awaitTermination(TERMINATION_WAIT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {
            logger.info("Stopping C2 Client's thread was interrupted but shutting down anyway the C2NifiClientService");
        }
    }

    private RuntimeInfoWrapper generateRuntimeInfo() {
        return new RuntimeInfoWrapper(getAgentRepositories(), runtimeManifestService.getManifest(), getQueueStatus());
    }

    private AgentRepositories getAgentRepositories() {
        final SystemDiagnostics systemDiagnostics = flowController.getSystemDiagnostics();

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

        return repos;
    }

    private Map<String, FlowQueueStatus> getQueueStatus() {
        ProcessGroupStatus rootProcessGroupStatus = flowController.getEventAccess().getGroupStatus(ROOT_GROUP_ID);

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

        return processGroupStatus;
    }

    private boolean updateFlowContent(byte[] updateContent) {
        logger.debug("Update content: \n{}", new String(updateContent, StandardCharsets.UTF_8));
        Path path = getTargetConfigFile().toPath();
        try {
            Files.write(getTargetConfigFile().toPath(), updateContent);
            logger.info("Updated configuration was written to: {}", path);
            return true;
        } catch (IOException e) {
            logger.error("Configuration update failed. File creation was not successful targeting: {}", path, e);
            return false;
        }
    }

    private File getTargetConfigFile() {
        return Optional.ofNullable(propertiesDir)
            .map(File::new)
            .map(File::getParent)
            .map(parentDir -> new File(parentDir + TARGET_CONFIG_FILE))
            .orElse( new File(DEFAULT_CONF_DIR + TARGET_CONFIG_FILE));
    }
}
