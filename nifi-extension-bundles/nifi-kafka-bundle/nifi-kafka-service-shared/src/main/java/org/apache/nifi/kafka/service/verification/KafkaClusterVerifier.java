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
package org.apache.nifi.kafka.service.verification;

import org.apache.commons.io.function.IOTriConsumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.logging.ComponentLog;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SKIPPED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;

/**
 * Verifies Kafka cluster connectivity and node reachability.
 */
public class KafkaClusterVerifier {

    private static final String CLUSTER_DESCRIPTION_STEP = "Kafka Cluster Description";
    private static final String NODE_CONFIG_CHECK_STEP = "Kafka Node Reachability";
    private static final String BOOTSTRAP_REACHABILITY_STEP = "Bootstrap Server Reachability";

    private final Duration verifyTimeout;
    private final ComponentLog logger;
    private final IOTriConsumer<String, Integer, Duration> socketConnector;

    public KafkaClusterVerifier(final Duration verifyTimeout, final ComponentLog logger) {
        this.verifyTimeout = verifyTimeout;
        this.logger = logger;
        this.socketConnector = KafkaClusterVerifier::defaultReachServer;
    }

    @VisibleForTesting
    KafkaClusterVerifier(final Duration verifyTimeout, final ComponentLog logger, final IOTriConsumer<String, Integer, Duration> socketConnector) {
        this.verifyTimeout = verifyTimeout;
        this.logger = logger;
        this.socketConnector = socketConnector;
    }

    /**
     * Verifies cluster connectivity by describing the cluster and checking node connectivity.
     * Falls back to bootstrap server connectivity check if cluster description fails due to permissions.
     *
     * @param admin Kafka Admin client
     * @param bootstrapServers Bootstrap servers configuration string
     * @return List of verification results
     */
    public List<ConfigVerificationResult> verifyClusterConnectivity(final Admin admin, final String bootstrapServers) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        // Use virtual thread executor for lightweight parallel execution
        try (final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final DescribeClusterResultInternal describeClusterResultInternal = describeCluster(admin);
            results.add(describeClusterResultInternal.result());

            if (describeClusterResultInternal.nodes() != null) {
                // Check each node's configuration
                results.addAll(verifyNodesReachability(describeClusterResultInternal.nodes(), executor));

            } else {
                // Fallback to checking bootstrap servers
                results.addAll(verifyBootstrapServersReachability(bootstrapServers, executor));
            }
        }

        return results;
    }

    private record DescribeClusterResultInternal(Collection<Node> nodes, ConfigVerificationResult result) { }

    private DescribeClusterResultInternal describeCluster(final Admin admin) {
        Collection<Node> nodes = null;
        ConfigVerificationResult configVerificationResult;
        try {
            // Try to describe cluster
            final DescribeClusterResult describeClusterResult = admin.describeCluster(new DescribeClusterOptions().timeoutMs((int) verifyTimeout.toMillis()));
            final KafkaFuture<Collection<Node>> nodesFuture = describeClusterResult.nodes();
            nodes = nodesFuture.get();

            final String clusterExplanation = String.format("Cluster Nodes Found [%d]", nodes.size());
            configVerificationResult =
                new ConfigVerificationResult.Builder()
                    .verificationStepName(CLUSTER_DESCRIPTION_STEP)
                    .outcome(SUCCESSFUL)
                    .explanation(clusterExplanation)
                    .build();

        } catch (final Exception e) {

            if (isCausedByAuthorizationException(e)) {
                logger.warn("Describe Cluster insufficient permissions", e);
                configVerificationResult =
                    new ConfigVerificationResult.Builder()
                        .verificationStepName(CLUSTER_DESCRIPTION_STEP)
                        .outcome(SKIPPED)
                        .explanation("Insufficient permissions to describe cluster.")
                        .build();

            } else {
                logger.error("Describe Cluster failed", e);
                configVerificationResult =
                    new ConfigVerificationResult.Builder()
                        .verificationStepName(CLUSTER_DESCRIPTION_STEP)
                        .outcome(SKIPPED)
                        .explanation("Cluster description failed: " + e)
                        .build();
            }
        }

        return new DescribeClusterResultInternal(nodes, configVerificationResult);
    }

    private boolean isCausedByAuthorizationException(final Exception e) {
        Throwable cause = e;
        while (!(cause instanceof AuthorizationException || cause == null)) {
            cause = cause.getCause();
        }
        return cause != null;
    }

    /**
     * Verifies each node's reachability by attempting socket connections in parallel using virtual threads.
     * Returns a ConfigVerificationResult for each individual node.
     *
     * @param nodes Collection of cluster nodes
     * @return List of verification results, one per node
     */
    private List<ConfigVerificationResult> verifyNodesReachability(final Collection<Node> nodes, final ExecutorService executor) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        if (nodes.isEmpty()) {
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(NODE_CONFIG_CHECK_STEP)
                            .outcome(FAILED)
                            .explanation("No nodes found in cluster")
                            .build()
            );
            return results;
        }

        // Submit parallel tasks to verify each node's reachability
        final List<NodeVerificationContext> verificationContexts = new ArrayList<>();
        for (final Node node : nodes) {
            final Callable<ConfigVerificationResult> task = () -> verifyNodeReachability(node);
            verificationContexts.add(new NodeVerificationContext(node, executor.submit(task)));
        }

        // Collect results from all parallel tasks
        for (final NodeVerificationContext nodeVerificationContext : verificationContexts) {
            try {
                results.add(nodeVerificationContext.resultFuture().get());
            } catch (final Exception e) {
                final Node node = nodeVerificationContext.node();
                logger.warn("Node {} ({}:{}) reachability check result collection exception", node.id(), node.host(), node.port(), e);
                results.add(
                        new ConfigVerificationResult.Builder()
                                .verificationStepName(verifyNodeStepName(node))
                                .outcome(FAILED)
                                .explanation(String.format("Task execution failed: %s", e))
                                .build()
                );
            }
        }

        return results;
    }

    private record NodeVerificationContext(Node node, Future<ConfigVerificationResult> resultFuture) { }

    /**
     * Verifies a single node's reachability using socket connection.
     *
     * @param node The Kafka node being verified
     * @return ConfigVerificationResult indicating success or failure
     */
    private ConfigVerificationResult verifyNodeReachability(final Node node) {
        final ConfigVerificationResult.Builder stepBuilder = new ConfigVerificationResult.Builder()
            .verificationStepName(verifyNodeStepName(node));

        try {
            reachServer(node.host(), node.port());
            return stepBuilder
                .outcome(SUCCESSFUL)
                .explanation("Node is reachable")
                .build();

        } catch (final Exception e) {
            logger.warn("Node {} ({}:{}) is not reachable", node.id(), node.host(), node.port(), e);
            return stepBuilder
                .outcome(FAILED)
                .explanation(String.format("Connection failed: %s", e))
                .build();
        }
    }

    private String verifyNodeStepName(final Node node) {
        return String.format("%s - Node %s (%s:%d)", NODE_CONFIG_CHECK_STEP, node.id(), node.host(), node.port());
    }

    /**
     * Fallback method to verify bootstrap server reachability using socket connections in parallel using virtual threads.
     * Returns a ConfigVerificationResult for each individual bootstrap server.
     *
     * @param bootstrapServers Bootstrap servers configuration string
     * @return List of verification results, one per bootstrap server
     */
    private List<ConfigVerificationResult> verifyBootstrapServersReachability(final String bootstrapServers, final ExecutorService executor) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(BOOTSTRAP_REACHABILITY_STEP)
                            .outcome(FAILED)
                            .explanation("No bootstrap servers configured")
                            .build()
            );
            return results;
        }

        final String[] servers = bootstrapServers.split(",");
        final List<String> validServers = new ArrayList<>();

        // Filter out empty server entries
        for (final String server : servers) {
            final String trimmedServer = server.trim();
            if (!trimmedServer.isEmpty()) {
                validServers.add(trimmedServer);
            }
        }

        if (validServers.isEmpty()) {
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(BOOTSTRAP_REACHABILITY_STEP)
                            .outcome(FAILED)
                            .explanation("No valid bootstrap servers configured")
                            .build()
            );
            return results;
        }

        // Submit parallel tasks to verify each bootstrap server
        final List<BootstrapServerVerificationContext> verificationContexts = new ArrayList<>();
        for (final String server : validServers) {
            final Callable<ConfigVerificationResult> task = () -> verifyBootstrapServer(server);
            verificationContexts.add(new BootstrapServerVerificationContext(server, executor.submit(task)));
        }

        // Collect results from all parallel tasks
        for (final BootstrapServerVerificationContext bootstrapServerVerificationContext : verificationContexts) {
            try {
                results.add(bootstrapServerVerificationContext.resultFuture().get());
            } catch (final Exception e) {
                final String server = bootstrapServerVerificationContext.server();
                logger.warn("Bootstrap Server {} result collection exception", server, e);
                results.add(
                        new ConfigVerificationResult.Builder()
                                .verificationStepName(verifyBootstrapServerStepName(server))
                                .outcome(FAILED)
                                .explanation(String.format("Task execution failed: %s", e))
                                .build()
                );
            }
        }

        return results;
    }

    private record BootstrapServerVerificationContext(String server, Future<ConfigVerificationResult> resultFuture) { }

    /**
     * Verifies a single bootstrap server's reachability.
     *
     * @param server Bootstrap server address
     * @return ConfigVerificationResult indicating reachability status
     */
    private ConfigVerificationResult verifyBootstrapServer(final String server) {
        final ConfigVerificationResult.Builder stepBuilder = new ConfigVerificationResult.Builder()
            .verificationStepName(verifyBootstrapServerStepName(server));

        try {
            // Remove protocol if present
            String serverAddress = server;
            if (serverAddress.contains("://")) {
                serverAddress = serverAddress.substring(serverAddress.indexOf("://") + 3);
            }

            final String[] parts = serverAddress.split(":");
            final String host = parts[0];
            final int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092; // Default Kafka port

            try {
                reachServer(host, port);
                return stepBuilder
                    .outcome(SUCCESSFUL)
                    .explanation("Bootstrap Server is reachable")
                    .build();
            } catch (final Exception e) {
                logger.warn("Bootstrap Server {} is not reachable", server, e);
                return stepBuilder
                    .outcome(FAILED)
                    .explanation(String.format("Connection failed: %s", e))
                    .build();
            }

        } catch (final Exception e) {
            logger.warn("Bootstrap Server {} reachability check failed", server, e);
            return stepBuilder
                    .outcome(FAILED)
                    .explanation(String.format("Invalid format or error: %s", e))
                    .build();
        }
    }

    private String verifyBootstrapServerStepName(final String server) {
        return String.format("%s - %s", BOOTSTRAP_REACHABILITY_STEP, server);
    }

    private void reachServer(final String host, final int port) throws IOException {
        this.socketConnector.accept(host, port, verifyTimeout);
    }

    private static void defaultReachServer(final String host, final int port, final Duration timeout) throws IOException {
        try (final Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), (int) timeout.toMillis());
        }
    }
}

