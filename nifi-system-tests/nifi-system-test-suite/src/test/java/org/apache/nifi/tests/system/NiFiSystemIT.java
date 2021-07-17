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

import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientConfig;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.impl.JerseyNiFiClient;
import org.apache.nifi.web.api.entity.ClusteSummaryEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class NiFiSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(NiFiSystemIT.class);
    private final ConcurrentMap<String, Long> lastLogTimestamps = new ConcurrentHashMap<>();

    public static final int CLIENT_API_PORT = 5671;
    public static final String NIFI_GROUP_ID = "org.apache.nifi";
    public static final String TEST_EXTENSIONS_ARTIFACT_ID = "nifi-system-test-extensions-nar";
    public static final String TEST_PROCESSORS_PACKAGE = "org.apache.nifi.processors.tests.system";
    public static final String TEST_CS_PACKAGE = "org.apache.nifi.cs.tests.system";

    private static final Pattern FRAMEWORK_NAR_PATTERN = Pattern.compile("nifi-framework-nar-(.*?)\\.nar");
    private static final File LIB_DIR = new File("target/nifi-lib-assembly/lib");
    private static volatile String nifiFrameworkVersion = null;

    @Rule
    public TestName name = new TestName();
    @Rule
    public Timeout defaultTimeout = new Timeout(5, TimeUnit.MINUTES);

    private NiFiClient nifiClient;
    private NiFiClientUtil clientUtil;
    private static final AtomicReference<NiFiInstance> nifiRef = new AtomicReference<>();

    @Before
    public void setup() throws IOException {
        Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
        setupClient();

        if (nifiRef.get() == null) {
            final NiFiInstance instance = getInstanceFactory().createInstance();
            nifiRef.set(instance);
            instance.createEnvironment();
            instance.start();

            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());

            if (instance.isClustered()) {
                waitForAllNodesConnected();
            }
        }
    }

    @AfterClass
    public static void cleanup() {
        final NiFiInstance nifi = nifiRef.get();
        nifiRef.set(null);
        if (nifi != null) {
            nifi.stop();
        }
    }

    @After
    public void teardown() throws IOException, NiFiClientException {
        try {
            if (isDestroyFlowAfterEachTest()) {
                destroyFlow();
            }

            if (isDestroyEnvironmentAfterEachTest()) {
                cleanup();
            }
        } finally {
            if (nifiClient != null) {
                nifiClient.close();
            }
        }
    }

    protected boolean isDestroyEnvironmentAfterEachTest() {
        return false;
    }

    protected void destroyFlow() throws NiFiClientException, IOException {
        getClientUtil().stopProcessGroupComponents("root");
        getClientUtil().disableControllerServices("root");
        getClientUtil().stopTransmitting("root");
        getClientUtil().deleteAll("root");
    }

    protected void waitForAllNodesConnected() {
        waitForAllNodesConnected(getNumberOfNodes(true));
    }

    protected void waitForAllNodesConnected(final int expectedNumberOfNodes) {
        waitForAllNodesConnected(expectedNumberOfNodes, 100L);
    }

    protected void waitForAllNodesConnected(final int expectedNumberOfNodes, final long sleepMillis) {
        logger.info("Waiting for {} nodes to connect", expectedNumberOfNodes);

        final NiFiClient client = getNifiClient();

        final long maxTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
        while (true) {
            try {
                final ClusteSummaryEntity clusterSummary = client.getFlowClient().getClusterSummary();
                final int connectedNodeCount = clusterSummary.getClusterSummary().getConnectedNodeCount();
                if (connectedNodeCount == expectedNumberOfNodes) {
                    return;
                }

                logEverySecond("Waiting for {} nodes to connect but currently only {} nodes are connected", expectedNumberOfNodes, connectedNodeCount);

                if (System.currentTimeMillis() > maxTime) {
                    throw new RuntimeException("Waited up to 60 seconds for both nodes to connect but only " + connectedNodeCount + " nodes connected");
                }
            } catch (final Exception e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(sleepMillis);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    protected void setupClient() {
        nifiClient = createClient();
        clientUtil = new NiFiClientUtil(nifiClient, getNiFiVersion());
    }

    protected NiFiClientUtil getClientUtil() {
        return clientUtil;
    }

    protected NiFiClient createClient() {
        return createClient(getClientApiPort());
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
        return CLIENT_API_PORT;
    }


    protected String getTestName() {
        return name.getMethodName();
    }

    protected NiFiClient getNifiClient() {
        Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
        return nifiClient;
    }

    protected String getNiFiVersion() {
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

    protected NiFiInstance getNiFiInstance() {
        return nifiRef.get();
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

    protected NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedStandaloneNiFiInstanceFactory(
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/default/bootstrap.conf")
                .instanceDirectory("target/standalone-instance")
                .overrideNifiProperties(getNifiPropertiesOverrides())
                .build());
    }

    protected Map<String, String> getNifiPropertiesOverrides() {
        return Collections.emptyMap();
    }

    protected boolean isDestroyFlowAfterEachTest() {
        return true;
    }

    protected void waitFor(final BooleanSupplier condition) throws InterruptedException {
        waitFor(condition, 10L);
    }

    protected void waitFor(final BooleanSupplier condition, final long delayMillis) throws InterruptedException {
        while (!condition.getAsBoolean()) {
            Thread.sleep(delayMillis);
        }
    }

    protected void waitForQueueNotEmpty(final String connectionId) throws InterruptedException {
        logger.info("Waiting for Queue on Connection {} to not be empty", connectionId);

        waitForQueueCountToMatch(connectionId, size -> size > 0, "greater than 0");

        logger.info("Waiting for Queue on Connection {} is not empty", connectionId);
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

            return test.test(currentSize);
        });
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
            e.printStackTrace();
            Assert.fail("Failed to obtain connection status");
            return null;
        }
    }

    protected int getConnectionQueueSize(final String connectionId) {
        final ConnectionStatusEntity statusEntity = getConnectionStatus(connectionId);
        return statusEntity.getConnectionStatus().getAggregateSnapshot().getFlowFilesQueued();
    }
}
