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
package org.apache.nifi.kafka.service;

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SKIPPED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;

class KafkaConnectionVerifier {
    static final String ADDRESSES_STEP = "Broker Addresses";
    static final String CONFIGURATION_STEP = "Broker Configuration";
    static final String BROKER_CONNECTION_STEP = "Broker Connection";
    static final String NODE_CONNECTION_STEP = "Node Connection";
    static final String CLUSTER_DESCRIPTION_STEP = "Cluster Description";
    static final String CLUSTER_CONNECTION_STEP = "Cluster Connection";
    static final String TOPIC_LISTING_STEP = "Topic Listing";

    private static final int SOCKET_CONNECT_TIMEOUT = 2500;
    private static final int VERIFY_TIMEOUT = 5000;
    private static final TimeUnit VERIFY_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

    /**
     * Verify Connection to Kafka Brokers
     *
     * @param verificationLogger Logger specific to verification warnings and errors
     * @param resolvedProperties Resolved properties from Controller Service implementation containing required values for communicating with Kafka Brokers
     *
     * @return Verification Results
     */
    List<ConfigVerificationResult> verify(final ComponentLog verificationLogger, final Properties resolvedProperties) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        // Get validated Bootstrap Addresses before checking socket connections
        final List<InetSocketAddress> bootstrapAddresses = getBootstrapAddresses(verificationLogger, resolvedProperties, results);
        if (bootstrapAddresses.isEmpty()) {
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(CONFIGURATION_STEP)
                            .outcome(FAILED)
                            .explanation("Validated Bootstrap Servers not found")
                            .build()
            );
        } else {
            // Get connected Bootstrap Addresses before checking Topics
            final List<InetSocketAddress> connectedAddresses = getConnectedAddresses(verificationLogger, results, bootstrapAddresses);
            if (connectedAddresses.isEmpty()) {
                results.add(
                        new ConfigVerificationResult.Builder()
                                .verificationStepName(BROKER_CONNECTION_STEP)
                                .outcome(FAILED)
                                .explanation("Connected Bootstrap Servers not found")
                                .build()
                );
            } else {
                try (final Admin admin = Admin.create(resolvedProperties)) {
                    final List<Node> clusterNodes = getClusterNodes(verificationLogger, admin, results);
                    if (clusterNodes.isEmpty()) {
                        verificationLogger.info("Cluster Nodes not found");
                    } else {
                        verifyClusterNodes(verificationLogger, connectedAddresses, clusterNodes, results);
                    }

                    // Verify Topics regardless of Cluster Node status because Describe Cluster is independent of listing Topics
                    verifyTopics(verificationLogger, admin, results);
                }
            }
        }

        return results;
    }

    private List<InetSocketAddress> getBootstrapAddresses(final ComponentLog verificationLogger, final Properties resolvedProperties, final List<ConfigVerificationResult> results) {
        final List<InetSocketAddress> bootstrapAddresses = new ArrayList<>();

        try {
            final AdminClientConfig adminClientConfig = new AdminClientConfig(resolvedProperties);
            final List<InetSocketAddress> validatedAddresses = ClientUtils.parseAndValidateAddresses(adminClientConfig);
            bootstrapAddresses.addAll(validatedAddresses);

            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(ADDRESSES_STEP)
                            .outcome(SUCCESSFUL)
                            .explanation("Addresses validated [%d]".formatted(validatedAddresses.size()))
                            .build()
            );
        } catch (final Exception e) {
            verificationLogger.error("Broker Address verification failed", e);
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(ADDRESSES_STEP)
                            .outcome(FAILED)
                            .explanation(e.getMessage())
                            .build()
            );
        }

        return bootstrapAddresses;
    }

    private List<InetSocketAddress> getConnectedAddresses(final ComponentLog verificationLogger, final List<ConfigVerificationResult> results, final List<InetSocketAddress> bootstrapAddresses) {
        final List<InetSocketAddress> connectedAddresses = Collections.synchronizedList(new ArrayList<>());

        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            final List<Callable<ConfigVerificationResult>> socketConnectionTasks = getConnectionTasks(bootstrapAddresses, connectedAddresses);

            final List<Future<ConfigVerificationResult>> futures = executorService.invokeAll(socketConnectionTasks, VERIFY_TIMEOUT, VERIFY_TIMEOUT_UNIT);
            for (final Future<ConfigVerificationResult> future : futures) {
                final ConfigVerificationResult result = future.get();
                results.add(result);
            }
        } catch (final Exception e) {
            verificationLogger.error("Broker Connection verification failed", e);
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(BROKER_CONNECTION_STEP)
                            .outcome(FAILED)
                            .explanation(e.getMessage())
                            .build()
            );
        }

        return connectedAddresses;
    }

    private List<Callable<ConfigVerificationResult>> getConnectionTasks(final List<InetSocketAddress> addresses, final List<InetSocketAddress> connectedAddresses) {
        final List<Callable<ConfigVerificationResult>> socketConnectionTasks = new ArrayList<>();
        for (final InetSocketAddress address : addresses) {
            final Callable<ConfigVerificationResult> socketConnectionTask = () -> {
                final ConfigVerificationResult result = verifySocketConnection(address);
                final ConfigVerificationResult.Outcome outcome = result.getOutcome();
                if (SUCCESSFUL == outcome) {
                    connectedAddresses.add(address);
                }
                return result;
            };
            socketConnectionTasks.add(socketConnectionTask);
        }
        return socketConnectionTasks;
    }

    private List<Node> getClusterNodes(final ComponentLog verificationLogger, final Admin admin, final List<ConfigVerificationResult> results) {
        final List<Node> clusterNodes = new ArrayList<>();

        final DescribeClusterOptions describeClusterOptions = new DescribeClusterOptions().timeoutMs(VERIFY_TIMEOUT);
        try {
            final DescribeClusterResult describeClusterResult = admin.describeCluster(describeClusterOptions);
            final KafkaFuture<Collection<Node>> resultNodesFuture = describeClusterResult.nodes();
            final Collection<Node> resultNodes = resultNodesFuture.get(VERIFY_TIMEOUT, VERIFY_TIMEOUT_UNIT);
            clusterNodes.addAll(resultNodes);

            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(CLUSTER_DESCRIPTION_STEP)
                            .outcome(SUCCESSFUL)
                            .explanation("Cluster Nodes found [%d]".formatted(resultNodes.size()))
                            .build()
            );
        } catch (final Exception e) {
            if (hasAuthorizationException(e)) {
                verificationLogger.warn("Describe Cluster authorization failed", e);
                results.add(
                        new ConfigVerificationResult.Builder()
                                .verificationStepName(CLUSTER_DESCRIPTION_STEP)
                                .outcome(SKIPPED)
                                .explanation(e.getMessage())
                                .build()
                );
            } else {
                verificationLogger.warn("Describe Cluster failed", e);
                results.add(
                        new ConfigVerificationResult.Builder()
                                .verificationStepName(CLUSTER_DESCRIPTION_STEP)
                                .outcome(FAILED)
                                .explanation(e.getMessage())
                                .build()
                );
            }
        }

        return clusterNodes;
    }

    private ConfigVerificationResult verifySocketConnection(final InetSocketAddress socketAddress) {
        final ConfigVerificationResult.Builder builder = new ConfigVerificationResult.Builder().verificationStepName(NODE_CONNECTION_STEP);

        try (Socket socket = new Socket()) {
            socket.connect(socketAddress, SOCKET_CONNECT_TIMEOUT);
            builder.outcome(SUCCESSFUL);
            builder.explanation("[%s:%d] socket connected".formatted(socketAddress.getHostString(), socketAddress.getPort()));
        } catch (final IOException e) {
            builder.outcome(FAILED);
            builder.explanation("[%s:%d] connection failed".formatted(socketAddress.getHostString(), socketAddress.getPort()));
        }

        return builder.build();
    }

    private boolean hasAuthorizationException(final Exception e) {
        final Throwable cause;

        if (e instanceof ExecutionException executionException) {
            cause = executionException.getCause();
        } else {
            cause = e.getCause();
        }

        return cause instanceof AuthorizationException;
    }

    private void verifyClusterNodes(
            final ComponentLog verificationLogger,
            final List<InetSocketAddress> connectedAddresses,
            final List<Node> clusterNodes,
            final List<ConfigVerificationResult> results
    ) {
        // Build list of Cluster Node Addresses not found in connected Bootstrap Addresses
        final List<InetSocketAddress> nodeAddresses = new ArrayList<>();
        for (final Node clusterNode : clusterNodes) {
            final InetSocketAddress nodeAddress = new InetSocketAddress(clusterNode.host(), clusterNode.port());
            if (connectedAddresses.contains(nodeAddress)) {
                continue;
            }
            nodeAddresses.add(nodeAddress);
        }
        if (nodeAddresses.isEmpty()) {
            verificationLogger.info("Cluster Nodes match connected Bootstrap Servers");
        } else {
            final List<InetSocketAddress> connectedNodeAddresses = getConnectedAddresses(verificationLogger, results, nodeAddresses);
            if (connectedNodeAddresses.isEmpty()) {
                results.add(
                        new ConfigVerificationResult.Builder()
                                .verificationStepName(CLUSTER_CONNECTION_STEP)
                                .outcome(FAILED)
                                .explanation("Connected Cluster Nodes not found")
                                .build()
                );
            } else {
                results.add(
                        new ConfigVerificationResult.Builder()
                                .verificationStepName(CLUSTER_CONNECTION_STEP)
                                .outcome(SUCCESSFUL)
                                .explanation("Connected Cluster Nodes found [%d]".formatted(clusterNodes.size()))
                                .build()
                );
            }
        }
    }

    private void verifyTopics(final ComponentLog verificationLogger, final Admin admin, final List<ConfigVerificationResult> results) {
        try {
            final ListTopicsResult listTopicsResult = admin.listTopics();
            final KafkaFuture<Collection<TopicListing>> requestedListings = listTopicsResult.listings();
            final Collection<TopicListing> topicListings = requestedListings.get(VERIFY_TIMEOUT, VERIFY_TIMEOUT_UNIT);

            final String topicListingExplanation = "Topics found [%d]".formatted(topicListings.size());
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(TOPIC_LISTING_STEP)
                            .outcome(SUCCESSFUL)
                            .explanation(topicListingExplanation)
                            .build()
            );
        } catch (final Exception e) {
            verificationLogger.error("Topic Listing verification failed", e);
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(TOPIC_LISTING_STEP)
                            .outcome(FAILED)
                            .explanation(e.getMessage())
                            .build()
            );
        }
    }
}
