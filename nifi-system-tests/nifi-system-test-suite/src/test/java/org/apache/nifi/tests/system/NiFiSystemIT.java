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
package org.apache.nifi.tests.system;

import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientConfig;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.toolkit.client.impl.JerseyNiFiClient;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ClusterSummaryEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusSnapshotEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ExtendWith(TroubleshootingTestWatcher.class)
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public abstract class NiFiSystemIT implements NiFiInstanceProvider {
    // Frequently used Processor class names / constants for convenience
    public static final String GENERATE_FLOWFILE = "GenerateFlowFile";
    public static final String TERMINATE_FLOWFILE = "TerminateFlowFile";
    public static final String REVERSE_CONTENTS = "ReverseContents";
    public static final String SUCCESS = "success";

    private static final Logger logger = LoggerFactory.getLogger(NiFiSystemIT.class);
    private final ConcurrentMap<String, Long> lastLogTimestamps = new ConcurrentHashMap<>();

    private static final String QUEUE_SIZE_LOGGING_KEY = "Queue Sizes";
    // Group ID | Source Name | Dest Name | Conn Name | Queue Size |
    private static final String QUEUE_SIZES_FORMAT = "| %1$-36.36s | %2$-30.30s | %3$-30.30s | %4$-30.30s | %5$-30.30s |";

    public static final RequestConfig DO_NOT_REPLICATE = () -> Collections.singletonMap("request-replicated", Boolean.TRUE.toString());

    public static final int CLUSTERED_CLIENT_API_BASE_PORT = 5671;
    public static final int STANDALONE_CLIENT_API_BASE_PORT = 5670;
    public static final String NIFI_GROUP_ID = "org.apache.nifi";
    public static final String TEST_EXTENSIONS_ARTIFACT_ID = "nifi-system-test-extensions-nar";
    public static final String TEST_EXTENSIONS_SERVICES_ARTIFACT_ID = "nifi-system-test-extensions-services-nar";
    public static final String TEST_PYTHON_EXTENSIONS_ARTIFACT_ID = "python-extensions";
    public static final String TEST_PARAM_PROVIDERS_PACKAGE = "org.apache.nifi.parameter.tests.system";
    public static final String TEST_PROCESSORS_PACKAGE = "org.apache.nifi.processors.tests.system";
    public static final String TEST_CS_PACKAGE = "org.apache.nifi.cs.tests.system";
    public static final String TEST_REPORTING_TASK_PACKAGE = "org.apache.nifi.reporting";
    public static final String TEST_FLOW_ANALYSIS_RULE_PACKAGE = "org.apache.nifi.flowanalysis";

    private static final Pattern FRAMEWORK_NAR_PATTERN = Pattern.compile("nifi-framework-nar-(.*?)\\.nar");
    private static final File LIB_DIR = new File("target/nifi-lib-assembly/lib");
    private static volatile String nifiFrameworkVersion = null;

    private NiFiClient nifiClient;
    private NiFiClientUtil clientUtil;
    private static final AtomicReference<NiFiInstance> nifiRef = new AtomicReference<>();
    private static final NiFiInstanceCache instanceCache = new NiFiInstanceCache();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(instanceCache::shutdown));
    }

    private TestInfo testInfo;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws IOException {
        this.testInfo = testInfo;

        final String testClassName = testInfo.getTestClass().map(Class::getSimpleName).orElse("<Unknown Test Class>");
        final String friendlyTestName = testClassName + ":" + testInfo.getDisplayName();
        logger.info("Beginning Test {}", friendlyTestName);

        Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());

        final NiFiInstanceFactory instanceFactory = getInstanceFactory();
        final NiFiInstance instance = instanceCache.createInstance(instanceFactory, friendlyTestName, isAllowFactoryReuse());
        nifiRef.set(instance);

        instance.createEnvironment();
        instance.start();

        setupClient();

        Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());

        if (instance.isClustered()) {
            waitForAllNodesConnected();
        }
    }

    protected boolean isAllowFactoryReuse() {
        return true;
    }

    @AfterAll
    public static void cleanup() {
        logger.info("Beginning cleanup");

        final NiFiInstance nifi = nifiRef.get();
        nifiRef.set(null);
        if (nifi != null) {
            instanceCache.stopOrRecycle(nifi);
        }

        logger.info("Finished cleanup");
    }

    @AfterEach
    public void teardown() throws Exception {
        logger.info("Beginning teardown");

        try {
            // In some cases a test can pass, but still leave a clustered instance with one
            // of the nodes in a bad state, if the instance then gets reused
            // it will cause later tests to fail, so it is better to destroy the environment
            // if the cluster is in a bad state at the end of a test
            final NiFiInstance nifiInstance = nifiRef.get();
            if (nifiInstance != null && nifiInstance.isClustered() && (!isCoordinatorElected() || !allNodesConnected(nifiInstance.getNumberOfNodes()))) {
                logger.info("Clustered environment is in a bad state, will completely tear down the environments and start with a clean environment for the next test.");
                instanceCache.poison(nifiInstance);
                cleanup();
                return;
            }

            Exception destroyFlowFailure = null;
            if (isDestroyFlowAfterEachTest()) {
                try {
                    destroyFlow();
                } catch (final Exception e) {
                    logger.error("Failed to destroy flow", e);
                    destroyFlowFailure = e;
                }
            }

            if (isDestroyEnvironmentAfterEachTest()) {
                instanceCache.poison(nifiRef.get());
                cleanup();
            } else if (destroyFlowFailure != null) {
                // If unable to destroy the flow, we need to shutdown the instance and delete
                // the flow and completely recreate the environment.
                // Otherwise, we will be left in an unknown state for the next test, and that
                // can cause cascading failures that are very difficult
                // to understand and troubleshoot.
                logger.info("Because there was a failure when destroying the flow, will completely tear down the environments and start with a clean environment for the next test.");
                instanceCache.poison(nifiRef.get());
                cleanup();
            }

            if (destroyFlowFailure != null) {
                throw destroyFlowFailure;
            }
        } catch (final Exception e) {
            logger.error("Failure during test case teardown", e);
            throw e;
        } finally {
            if (nifiClient != null) {
                nifiClient.close();
            }

            logger.info("Finished teardown");
        }
    }

    @Override
    public NiFiInstance getNiFiInstance() {
        return nifiRef.get();
    }

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createStandaloneInstanceFactory();
    }

    public NiFiInstanceFactory createStandaloneInstanceFactory() {
        return new SpawnedStandaloneNiFiInstanceFactory(
                new InstanceConfiguration.Builder()
                        .bootstrapConfig("src/test/resources/conf/default/bootstrap.conf")
                        .instanceDirectory("target/standalone-instance")
                        .overrideNifiProperties(getNifiPropertiesOverrides())
                        .unpackPythonExtensions(false)
                        .build());
    }

    public NiFiInstanceFactory createTwoNodeInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
                "src/test/resources/conf/clustered/node1/bootstrap.conf",
                "src/test/resources/conf/clustered/node2/bootstrap.conf");
    }

    public NiFiInstanceFactory createPythonicInstanceFactory() {
        return new SpawnedStandaloneNiFiInstanceFactory(
                new InstanceConfiguration.Builder()
                        .bootstrapConfig("src/test/resources/conf/pythonic/bootstrap.conf")
                        .instanceDirectory("target/pythonic-instance")
                        .overrideNifiProperties(getNifiPropertiesOverrides())
                        .unpackPythonExtensions(true)
                        .build());
    }

    protected String getTestName() {
        return testInfo.getDisplayName();
    }

    protected boolean isDestroyEnvironmentAfterEachTest() {
        return false;
    }

    protected void destroyFlow() throws NiFiClientException, IOException, InterruptedException {
        logger.info("Starting destroyFlow");

        getClientUtil().stopProcessGroupComponents("root");
        getClientUtil().disableControllerServices("root", true);
        getClientUtil().stopReportingTasks();
        getClientUtil().disableControllerLevelServices();
        getClientUtil().disableFlowAnalysisRules();
        getClientUtil().stopTransmitting("root");
        getClientUtil().deleteAll("root");
        getClientUtil().deleteControllerLevelServices();
        getClientUtil().deleteReportingTasks();
        getClientUtil().deleteFlowAnalysisRules();
        getClientUtil().deleteParameterContexts();
        getClientUtil().deleteParameterProviders();

        logger.info("Finished destroyFlow");
    }

    protected void waitForAllNodesConnected() {
        waitForAllNodesConnected(getNumberOfNodes(true));
    }

    protected void waitForAllNodesConnected(final int expectedNumberOfNodes) {
        waitForAllNodesConnected(expectedNumberOfNodes, 500L);
    }

    protected void waitForAllNodesConnected(final int expectedNumberOfNodes, final long sleepMillis) {
        logger.info("Waiting for {} nodes to connect", expectedNumberOfNodes);

        final NiFiClient client = getNifiClient();

        int attemptedNodeIndex = 0;
        final long maxTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
        while (true) {
            int connectedNodeCount = -1;
            try {
                final ClusterSummaryEntity clusterSummary = client.getFlowClient().getClusterSummary();
                connectedNodeCount = clusterSummary.getClusterSummary().getConnectedNodeCount();
                if (connectedNodeCount == expectedNumberOfNodes) {
                    logger.info("Wait successful, {} nodes connected", expectedNumberOfNodes);
                    return;
                }

                logEverySecond("Waiting for {} nodes to connect but currently only {} nodes are connected", expectedNumberOfNodes, connectedNodeCount);
            } catch (final Exception e) {
                logger.error("Failed to determine how many nodes are currently connected", e);
                final int nodeIndexToAttempt = attemptedNodeIndex++ % expectedNumberOfNodes;
                setupClient(CLUSTERED_CLIENT_API_BASE_PORT + nodeIndexToAttempt);
            }

            if (System.currentTimeMillis() > maxTime) {
                throw new RuntimeException("Waited up to 60 seconds for all nodes to connect but only " + connectedNodeCount + " nodes connected");
            }

            try {
                Thread.sleep(sleepMillis);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    protected void switchClientToNode(final int nodeIndex) {
        setupClient(CLUSTERED_CLIENT_API_BASE_PORT + nodeIndex - 1);
    }

    protected void setupClient() {
        setupClient(getClientApiPort());
    }

    protected void setupClient(final int apiPort) {
        nifiClient = createClient(apiPort);
        clientUtil = new NiFiClientUtil(nifiClient, getNiFiVersion(), getTestName());
    }

    protected NiFiClientUtil getClientUtil() {
        return clientUtil;
    }

    protected NiFiClient createClient(final int port) {
        final NiFiClientConfig clientConfig = new NiFiClientConfig.Builder()
                .baseUrl("http://localhost:" + port)
                .connectTimeout(30000)
                .readTimeout(30000)
                .build();

        return new JerseyNiFiClient.Builder()
                .config(clientConfig)
                .build();
    }

    protected int getClientApiPort() {
        NiFiInstance nifiInstance = nifiRef.get();
        if (nifiInstance.getNumberOfNodes() > 1) {
            return CLUSTERED_CLIENT_API_BASE_PORT;
        }

        return STANDALONE_CLIENT_API_BASE_PORT;
    }

    protected NiFiClient getNifiClient() {
        Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
        return nifiClient;
    }

    protected static String getNiFiVersion() {
        final String knownVersion = nifiFrameworkVersion;
        if (knownVersion != null) {
            return knownVersion;
        }

        final File[] files = LIB_DIR.listFiles();
        for (final File file : files) {
            final String filename = file.getName();
            final Matcher matcher = FRAMEWORK_NAR_PATTERN.matcher(filename);
            if (matcher.matches()) {
                final String version = matcher.group(1);
                nifiFrameworkVersion = version;
                return version;
            }
        }

        throw new IllegalStateException("Could not determine version of NiFi");
    }

    protected int getNumberOfNodes() {
        return getNumberOfNodes(true);
    }

    protected int getNumberOfNodes(final boolean includeOnlyAutoStartInstances) {
        final NiFiInstance instance = nifiRef.get();
        if (instance == null) {
            return 1;
        }

        return instance.getNumberOfNodes(includeOnlyAutoStartInstances);
    }

    protected Map<String, String> getNifiPropertiesOverrides() {
        return Collections.emptyMap();
    }

    protected boolean isDestroyFlowAfterEachTest() {
        return true;
    }

    protected void waitFor(final ExceptionalBooleanSupplier condition) throws InterruptedException {
        waitFor(condition, 100L);
    }

    protected void waitFor(final ExceptionalBooleanSupplier condition, final long delayMillis) throws InterruptedException {
        boolean result = false;
        while (!result) {
            try {
                result = condition.getAsBoolean();
            } catch (final InterruptedException ie) {
                throw ie;
            } catch (final Exception ignored) {
            }

            Thread.sleep(delayMillis);
        }
    }

    protected void waitForNodeStatus(final NodeDTO nodeDto, final String status) throws InterruptedException {
        waitFor(() -> {
            try {
                final ClusterEntity clusterEntity = getNifiClient().getControllerClient().getNodes();
                final Collection<NodeDTO> nodes = clusterEntity.getCluster().getNodes();
                final NodeDTO nodeDtoMatch = nodes.stream()
                        .filter(n -> n.getApiPort().equals(nodeDto.getApiPort()))
                        .findFirst()
                        .get();
                return nodeDtoMatch.getStatus().equals(status);
            } catch (final Exception e) {
                logger.error("Failed to determine node status", e);
            }
            return false;
        });
    }

    protected void waitForQueueNotEmpty(final String connectionId) throws InterruptedException {
        logger.info("Waiting for Queue on Connection {} to not be empty", connectionId);

        waitForQueueCountToMatch(connectionId, size -> size > 0, "greater than 0");

        logger.info("Queue on Connection {} is not empty", connectionId);
    }

    protected void waitForMinQueueCount(final String connectionId, final int queueSize) throws InterruptedException {
        logger.info("Waiting for Queue Count of at least {} on Connection {}", queueSize, connectionId);

        waitForQueueCountToMatch(connectionId, size -> size >= queueSize, String.valueOf(queueSize));

        logger.info("Queue Count for Connection {} is now {}", connectionId, queueSize);
    }

    protected void waitForQueueCount(final ConnectionEntity entity, final int queueSize) throws InterruptedException {
        waitForQueueCount(entity.getId(), queueSize);
    }

    protected void waitForQueueCount(final String connectionId, final int queueSize) throws InterruptedException {
        logger.info("Waiting for Queue Count of {} on Connection {}", queueSize, connectionId);

        waitForQueueCountToMatch(connectionId, size -> size == queueSize, String.valueOf(queueSize));

        logger.info("Queue Count for Connection {} is now {}", connectionId, queueSize);
    }

    private void waitForQueueCountToMatch(final String connectionId, final Predicate<Integer> test, final String queueSizeDescription) throws InterruptedException {
        waitFor(() -> {
            final ConnectionStatusEntity statusEntity = getConnectionStatus(connectionId);
            final int currentSize = statusEntity.getConnectionStatus().getAggregateSnapshot().getFlowFilesQueued();
            final String sourceName = statusEntity.getConnectionStatus().getSourceName();
            final String destinationName = statusEntity.getConnectionStatus().getDestinationName();
            logEverySecond("Current Queue Size for Connection from {} to {} = {}, Waiting for {}", sourceName, destinationName, currentSize, queueSizeDescription);
            logQueueSizesEveryMinute();

            final boolean matched = test.test(currentSize);
            if (matched) {
                resetQueueSizeLogging();
            }

            return matched;
        });
    }

    private void resetQueueSizeLogging() {
        lastLogTimestamps.remove(QUEUE_SIZE_LOGGING_KEY);
    }

    private void logQueueSizesEveryMinute() {
        // If we haven't yet logged queue sizes, add entry
        final Long lastLogTime = lastLogTimestamps.get(QUEUE_SIZE_LOGGING_KEY);
        if (lastLogTime == null) {
            lastLogTimestamps.put(QUEUE_SIZE_LOGGING_KEY, System.currentTimeMillis());
            return;
        }

        // If it's not been at least 10 seconds, don't log again
        if (System.currentTimeMillis() < lastLogTime + TimeUnit.SECONDS.toMillis(10)) {
            return;
        }

        // Record the current time and log
        lastLogTimestamps.put(QUEUE_SIZE_LOGGING_KEY, System.currentTimeMillis());

        try {
            logQueueSizes();
        } catch (final Exception e) {
            logger.warn("Attempted to obtain queue sizes for logging purposes but failed to obtain queue sizes", e);
        }
    }

    protected void logQueueSizes() throws NiFiClientException, IOException {
        final ProcessGroupStatusEntity groupStatusEntity = getNifiClient().getFlowClient().getProcessGroupStatus("root", true);
        final ProcessGroupStatusSnapshotDTO groupStatusDto = groupStatusEntity.getProcessGroupStatus().getAggregateSnapshot();

        final List<ConnectionStatusSnapshotEntity> connectionStatuses = new ArrayList<>();
        gatherConnectionStatuses(groupStatusDto, connectionStatuses);

        logger.info("Dump of Queue Sizes:");
        final String headerLine = String.format(QUEUE_SIZES_FORMAT,
                "Group ID",
                "Source Name",
                "Destination Name",
                "Connection Name",
                "Queued");
        logger.info(headerLine);

        for (final ConnectionStatusSnapshotEntity connectionStatus : connectionStatuses) {
            final ConnectionStatusSnapshotDTO statusSnapshotDto = connectionStatus.getConnectionStatusSnapshot();
            if (statusSnapshotDto == null) {
                continue;
            }

            final String formatted = String.format(QUEUE_SIZES_FORMAT,
                    statusSnapshotDto.getGroupId(),
                    statusSnapshotDto.getSourceName(),
                    statusSnapshotDto.getDestinationName(),
                    statusSnapshotDto.getName(),
                    statusSnapshotDto.getQueued());
            logger.info(formatted);
        }

    }

    private void gatherConnectionStatuses(final ProcessGroupStatusSnapshotDTO groupStatusSnapshotDto, final List<ConnectionStatusSnapshotEntity> connectionStatuses) {
        if (groupStatusSnapshotDto == null) {
            return;
        }

        connectionStatuses.addAll(groupStatusSnapshotDto.getConnectionStatusSnapshots());

        for (final ProcessGroupStatusSnapshotEntity childStatusEntity : groupStatusSnapshotDto.getProcessGroupStatusSnapshots()) {
            gatherConnectionStatuses(childStatusEntity.getProcessGroupStatusSnapshot(), connectionStatuses);
        }
    }

    private void logEverySecond(final String message, final Object... args) {
        final Long lastLogTime = lastLogTimestamps.get(message);
        if (lastLogTime == null || lastLogTime < System.currentTimeMillis() - 1000L) {
            logger.info(message, args);
            lastLogTimestamps.put(message, System.currentTimeMillis());
        }
    }

    private ConnectionStatusEntity getConnectionStatus(final String connectionId) {
        try {
            return getNifiClient().getFlowClient().getConnectionStatus(connectionId, true);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to obtain connection status");
        }
    }

    protected int getConnectionQueueSize(final String connectionId) {
        final ConnectionStatusEntity statusEntity = getConnectionStatus(connectionId);
        return statusEntity.getConnectionStatus().getAggregateSnapshot().getFlowFilesQueued();
    }

    public NodeDTO getNodeDtoByNodeIndex(final int nodeIndex) throws NiFiClientException, IOException {
        return getNodeDtoByApiPort(5670 + nodeIndex);
    }

    public NodeDTO getNodeDtoByApiPort(final int apiPort) throws NiFiClientException, IOException {
        final ClusterEntity clusterEntity = getNifiClient().getControllerClient().getNodes();
        final NodeDTO node2Dto = clusterEntity.getCluster()
                .getNodes()
                .stream()
                .filter(nodeDto -> nodeDto.getApiPort() == apiPort)
                .findAny()
                .orElseThrow(() -> new RuntimeException("Could not locate Node 2"));

        return node2Dto;
    }

    /**
     * Disconnects a node from the cluster
     *
     * @param nodeIndex the 1-based index of the node
     */
    protected void disconnectNode(final int nodeIndex) throws NiFiClientException, IOException, InterruptedException {
        final NodeEntity nodeEntity = getNodeEntity(nodeIndex);
        nodeEntity.getNode().setStatus(NodeConnectionState.DISCONNECTING.name());
        getNifiClient().getControllerClient().disconnectNode(nodeEntity.getNode().getNodeId(), nodeEntity);

        waitForNodeState(nodeIndex, NodeConnectionState.DISCONNECTED);
        waitForCoordinatorElected();
    }

    protected void waitForCoordinatorElected() throws InterruptedException {
        waitFor(this::isCoordinatorElected);
    }

    protected boolean isCoordinatorElected() throws NiFiClientException, IOException {
        final ClusterEntity clusterEntity = getNifiClient().getControllerClient().getNodes();
        for (final NodeDTO nodeDto : clusterEntity.getCluster().getNodes()) {
            if (nodeDto.getRoles().contains(ClusterRoles.CLUSTER_COORDINATOR) && nodeDto.getStatus().equals("CONNECTED")) {
                return true;
            }
        }

        return false;
    }

    protected boolean allNodesConnected(int expectedNodeCount) throws NiFiClientException, IOException {
        final ClusterSummaryEntity clusterSummary = getNifiClient().getFlowClient().getClusterSummary();
        return expectedNodeCount == clusterSummary.getClusterSummary().getConnectedNodeCount();
    }

    protected void reconnectNode(final int nodeIndex) throws NiFiClientException, IOException {
        final NodeEntity nodeEntity = getNodeEntity(nodeIndex);
        nodeEntity.getNode().setStatus(NodeConnectionState.CONNECTING.name());
        getNifiClient().getControllerClient().connectNode(nodeEntity.getNode().getNodeId(), nodeEntity);
    }

    protected NodeEntity getNodeEntity(final int nodeIndex) throws NiFiClientException, IOException {
        final ClusterEntity clusterEntity = getNifiClient().getControllerClient().getNodes();
        final int expectedPort = getClientApiPort() + nodeIndex - 1;

        final List<Integer> nodePorts = new ArrayList<>();
        for (final NodeDTO nodeDto : clusterEntity.getCluster().getNodes()) {
            nodePorts.add(nodeDto.getApiPort());
            if (nodeDto.getApiPort() == expectedPort) {
                final NodeEntity nodeEntity = new NodeEntity();
                nodeEntity.setNode(nodeDto);
                return nodeEntity;
            }
        }

        throw new IllegalStateException("Could not find node with API Port of " + expectedPort + "; found nodes: " + nodePorts);
    }

    protected void waitForNodeState(final int nodeIndex, final NodeConnectionState... nodeStates) throws InterruptedException {
        waitFor(() -> {
            try {
                final NodeEntity nodeEntity = getNodeEntity(nodeIndex);
                final String status = nodeEntity.getNode().getStatus();
                for (final NodeConnectionState state : nodeStates) {
                    if (state.name().equals(status)) {
                        return true;
                    }
                }

                return false;
            } catch (final Exception e) {
                return false;
            }
        });
    }

    protected FlowRegistryClientEntity registerClient() throws NiFiClientException, IOException {
        final File storageDir = new File("target/flowRegistryStorage/" + getTestName().replace("\\(.*?\\)", ""));
        Files.createDirectories(storageDir.toPath());

        return registerClient(storageDir);
    }

    protected FlowRegistryClientEntity registerClient(final File storageDir) throws NiFiClientException, IOException {
        final String clientName = String.format("FileRegistry-%s", UUID.randomUUID());
        final FlowRegistryClientEntity clientEntity = getClientUtil().createFlowRegistryClient(clientName);
        getClientUtil().updateRegistryClientProperties(clientEntity, Collections.singletonMap("Directory", storageDir.getAbsolutePath()));

        return clientEntity;
    }

}
