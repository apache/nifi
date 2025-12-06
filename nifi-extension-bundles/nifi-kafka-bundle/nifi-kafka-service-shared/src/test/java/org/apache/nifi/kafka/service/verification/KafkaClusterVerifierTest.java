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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SKIPPED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaClusterVerifierTest {

    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(2);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";

    @Mock
    private Admin admin;

    @Mock
    private ComponentLog logger;

    @Mock
    private DescribeClusterResult describeClusterResult;

    private IOTriConsumer<String, Integer, Duration> socketConnector;
    private KafkaClusterVerifier verifier;

    @BeforeEach
    void setUp() {
        socketConnector = (host, port, timeout) -> {
            // Simulate successful connection by default
        };
        verifier = new KafkaClusterVerifier(VERIFY_TIMEOUT, logger, (host, port, timeout) -> socketConnector.accept(host, port, timeout));
    }

    @Test
    void testVerifyClusterConnectivitySuccess() {
        // Arrange
        final Node node1 = new Node(1, "broker1.example.com", 9092);
        final Collection<Node> nodes = List.of(node1);

        final KafkaFuture<Collection<Node>> nodesFuture = KafkaFuture.completedFuture(nodes);
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(nodesFuture);

        // Act
        final List<ConfigVerificationResult> results = verifier.verifyClusterConnectivity(admin, BOOTSTRAP_SERVERS);

        // Assert - should have cluster description + 1 node result
        assertEquals(2, results.size());

        // Cluster description result
        assertEquals(SUCCESSFUL, results.getFirst().getOutcome());
        assertTrue(results.getFirst().getExplanation().contains("Cluster Nodes Found [1]"));

        // Node verification result - check that we have at least one node result
        final boolean hasNodeResult = results.stream()
                .anyMatch(r -> r.getVerificationStepName().contains("Node 1")
                    && r.getOutcome() == SUCCESSFUL
                    && r.getVerificationStepName().contains("broker1.example.com"));
        assertTrue(hasNodeResult, "Should have a node verification result");
    }

    @Test
    void testVerifyClusterConnectivityWithAuthorizationException() {
        // Arrange
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenThrow(new AuthorizationException("Unauthorized"));

        // Act
        final List<ConfigVerificationResult> results = verifier.verifyClusterConnectivity(admin, BOOTSTRAP_SERVERS);

        // Assert
        assertFalse(results.isEmpty());

        // Should have skipped cluster description
        final ConfigVerificationResult clusterResult = results.stream()
                .filter(r -> r.getVerificationStepName().contains("Cluster Description"))
                .findFirst()
                .orElseThrow();

        assertEquals(SKIPPED, clusterResult.getOutcome());
        assertTrue(clusterResult.getExplanation().contains("Insufficient permissions"));

        // Should have bootstrap server results (fallback)
        final long bootstrapResults = results.stream()
                .filter(r -> r.getVerificationStepName().contains("Bootstrap Server Reachability"))
                .count();

        assertTrue(bootstrapResults > 0);

        verify(logger).warn(eq("Describe Cluster insufficient permissions"), any(AuthorizationException.class));
    }

    @Test
    void testVerifyClusterConnectivityWithGeneralException() {
        // Arrange
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenThrow(new TimeoutException("Connection timeout"));

        // Act
        final List<ConfigVerificationResult> results = verifier.verifyClusterConnectivity(admin, BOOTSTRAP_SERVERS);

        // Assert
        assertFalse(results.isEmpty());

        // Should have skipped cluster description
        final ConfigVerificationResult clusterResult = results.stream()
                .filter(r -> r.getVerificationStepName().contains("Cluster Description"))
                .findFirst()
                .orElseThrow();

        assertEquals(SKIPPED, clusterResult.getOutcome());
        assertTrue(clusterResult.getExplanation().contains("Cluster description failed"));

        verify(logger).error(eq("Describe Cluster failed"), any(Exception.class));
    }

    @Test
    void testVerifyClusterConnectivityWithEmptyNodes() {
        // Arrange
        final Collection<Node> nodes = List.of();
        final KafkaFuture<Collection<Node>> nodesFuture = KafkaFuture.completedFuture(nodes);
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(nodesFuture);

        // Act
        final List<ConfigVerificationResult> results = verifier.verifyClusterConnectivity(admin, BOOTSTRAP_SERVERS);

        // Assert
        assertTrue(results.size() >= 2);

        // Cluster description result
        assertEquals(SUCCESSFUL, results.getFirst().getOutcome());
        assertTrue(results.getFirst().getExplanation().contains("Cluster Nodes Found [0]"));

        // Should have a failure for empty nodes
        final boolean hasEmptyNodesResult = results.stream()
                .anyMatch(r -> r.getOutcome() == FAILED && r.getExplanation().contains("No nodes found in cluster"));
        assertTrue(hasEmptyNodesResult);
    }

    @Test
    void testVerifyNodeReachabilityWithTimeout() {
        // Arrange
        final Node node = new Node(1, "broker1.example.com", 9092);
        final Collection<Node> nodes = List.of(node);

        final KafkaFuture<Collection<Node>> nodesFuture = KafkaFuture.completedFuture(nodes);
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenReturn(describeClusterResult);
        when(describeClusterResult.nodes()).thenReturn(nodesFuture);

        socketConnector = (host, port, timeout) -> {
            throw new TimeoutException("Timeout");
        };

        // Act
        final List<ConfigVerificationResult> results = verifier.verifyClusterConnectivity(admin, BOOTSTRAP_SERVERS);

        // Assert - Timeout should indicate failure (node not reachable)
        final boolean hasFailedNodeResult = results.stream()
                .anyMatch(r -> r.getVerificationStepName().contains("Node 1")
                        && r.getVerificationStepName().contains("broker1.example.com")
                        && r.getOutcome() == FAILED
                        && r.getExplanation().matches("Connection failed: .*: Timeout"));

        assertTrue(hasFailedNodeResult, "Should have a failed node result for Timeout");
    }

    @Test
    void testVerifyBootstrapServersReachabilityWithNullServers() {
        // Act
        final List<ConfigVerificationResult> results = verifier.verifyClusterConnectivity(admin, null);

        // Assert - Should skip cluster description due to exception, then fail on null bootstrap servers
        final ConfigVerificationResult bootstrapResult = results.stream()
                .filter(r -> r.getVerificationStepName().contains("Bootstrap Server"))
                .findFirst()
                .orElseThrow();

        assertEquals(FAILED, bootstrapResult.getOutcome());
        assertTrue(bootstrapResult.getExplanation().contains("No bootstrap servers configured"));
    }

    @Test
    void testVerifyBootstrapServersReachabilityWithEmptyServers() {
        // Act
        final List<ConfigVerificationResult> results = verifier.verifyClusterConnectivity(admin, "   ");

        // Assert
        final ConfigVerificationResult bootstrapResult = results.stream()
                .filter(r -> r.getVerificationStepName().contains("Bootstrap Server"))
                .findFirst()
                .orElseThrow();

        assertEquals(FAILED, bootstrapResult.getOutcome());
        assertTrue(bootstrapResult.getExplanation().contains("No bootstrap servers configured"));
    }

    @Test
    void testVerifyBootstrapServersReachabilityWithInvalidFormat() {
        // Arrange
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenThrow(new AuthorizationException("Unauthorized"));

        final String invalidServer = "invalid:server:format:9092";

        // Act
        final List<ConfigVerificationResult> results = verifier.verifyClusterConnectivity(admin, invalidServer);

        // Assert
        final ConfigVerificationResult bootstrapResult = results.stream()
                .filter(r -> r.getVerificationStepName().contains("Bootstrap Server Reachability - " + invalidServer))
                .findFirst()
                .orElseThrow();

        assertEquals(FAILED, bootstrapResult.getOutcome());
        assertTrue(bootstrapResult.getExplanation().contains("Invalid format or error"));
    }

    @Test
    void testVerifyBootstrapServersReachabilityMultiple() {
        // Arrange
        when(admin.describeCluster(any(DescribeClusterOptions.class))).thenThrow(new AuthorizationException("Unauthorized"));

        final String multipleServers = "broker1:9092,broker2:9093,broker3:9094";

        // Act
        final List<ConfigVerificationResult> results = verifier.verifyClusterConnectivity(admin, multipleServers);

        // Assert - Should have results for all bootstrap servers
        final long bootstrapResultCount = results.stream()
                .filter(r -> r.getVerificationStepName().contains("Bootstrap Server Reachability"))
                .count();

        assertTrue(bootstrapResultCount >= 3);
    }
}

