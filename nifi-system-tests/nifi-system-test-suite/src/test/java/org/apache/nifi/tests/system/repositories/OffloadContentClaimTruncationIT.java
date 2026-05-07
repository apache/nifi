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
package org.apache.nifi.tests.system.repositories;

import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * System test that exercises the interaction between Node Offload and Content Claim Truncation.
 *
 * <p>When a node is offloaded, every FlowFile in its local partitions is shipped to peer nodes via
 * the load-balance protocol. On the peer, {@code StandardLoadBalanceProtocol} packs every received
 * FlowFile in a transaction into a single {@code ContentClaim} produced by
 * {@code ContentRepository.create()}. If the peer's {@code writableClaimQueue} contains a
 * partially-filled Resource Claim at the time of receive, that {@code ContentClaim} ends up with
 * {@code offset > 0} and, given a sufficiently large transaction, with {@code length} above the
 * truncation threshold &mdash; making the claim a truncation candidate.</p>
 *
 * <p>The receive path persists each received FlowFile as an {@code UPDATE} repository record without
 * marking the record as content-modified, so the truncation reference count for the shared Content
 * Claim is never incremented to reflect the live FlowFiles that point inside it. Once a single
 * sibling FlowFile is removed downstream, the shared Content Claim's reference count is observed as
 * zero by {@code isTruncationAllowed} and the claim is queued for truncation while the surviving
 * FlowFiles still reference offsets inside it.</p>
 *
 * <p>This test reproduces that scenario end-to-end. It generates content on the primary node, offloads
 * the primary node so the data is shipped to the peer, removes a single FlowFile to arm truncation,
 * waits for the periodic truncate task to run, and then asserts that every remaining FlowFile in the
 * connection on the peer still returns its original content.</p>
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public class OffloadContentClaimTruncationIT extends NiFiSystemIT {

    private static final int CONTENT_BYTES_PER_FLOW_FILE = 8 * 1024;
    private static final int FLOW_FILE_COUNT = 30;
    private static final String CONTENT_TEXT = "x".repeat(CONTENT_BYTES_PER_FLOW_FILE);

    private static final int CLUSTER_NODE_COUNT = 2;

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        final Map<String, String> propertyOverrides = Map.of(
                "nifi.flowfile.repository.checkpoint.interval", "1 sec",
                "nifi.content.repository.archive.cleanup.frequency", "1 sec",
                "nifi.content.claim.max.appendable.size", "50 KB",
                "nifi.content.claim.truncation.enabled", "true",
                "nifi.content.repository.archive.max.usage.percentage", "1%");

        return new SpawnedClusterNiFiInstanceFactory(
                new InstanceConfiguration.Builder()
                        .bootstrapConfig("src/test/resources/conf/clustered/node1/bootstrap.conf")
                        .instanceDirectory("target/node1")
                        .overrideNifiProperties(propertyOverrides)
                        .build(),
                new InstanceConfiguration.Builder()
                        .bootstrapConfig("src/test/resources/conf/clustered/node2/bootstrap.conf")
                        .instanceDirectory("target/node2")
                        .overrideNifiProperties(propertyOverrides)
                        .build());
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Test
    public void testOffloadedFlowFileContentNotPrematurelyTruncated() throws Exception {
        // Pre-warm both nodes' writableClaimQueue with a partially-filled Resource Claim. Each node
        // generates a single small FlowFile, leaving it queued upstream of an unscheduled
        // TerminateFlowFile. The Resource Claim file remains writable and a few bytes long, so the
        // next ContentRepository.create() call on each node will reuse this Resource Claim and the
        // resulting ContentClaim will have offset > 0.
        final ProcessorEntity prewarmGenerator = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(prewarmGenerator, Map.of(
                "Text", "warmup",
                "Batch Size", "1",
                "Max FlowFiles", "1"));
        getClientUtil().updateProcessorSchedulingPeriod(prewarmGenerator, "0 sec");

        final ProcessorEntity prewarmTerminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity prewarmConnection = getClientUtil().createConnection(prewarmGenerator, prewarmTerminate, "success");

        getClientUtil().startProcessor(prewarmGenerator);
        waitForQueueCount(prewarmConnection.getId(), CLUSTER_NODE_COUNT);
        getClientUtil().stopProcessor(prewarmGenerator);

        // Source generator runs only on the primary node and produces FLOW_FILE_COUNT FlowFiles,
        // each carrying the same CONTENT_TEXT. The non-primary node generates nothing in this phase, so
        // its writableClaimQueue retains the partially-filled Resource Claim from pre-warm.
        final ProcessorEntity sourceGenerator = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(sourceGenerator, Map.of(
                "Text", CONTENT_TEXT,
                "Batch Size", String.valueOf(FLOW_FILE_COUNT),
                "Max FlowFiles", String.valueOf(FLOW_FILE_COUNT)));
        getClientUtil().updateProcessorExecutionNode(sourceGenerator, ExecutionNode.PRIMARY);
        getClientUtil().updateProcessorSchedulingPeriod(sourceGenerator, "0 sec");

        ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity dataConnection = getClientUtil().createConnection(sourceGenerator, terminate, "success");

        getClientUtil().startProcessor(sourceGenerator);
        waitForQueueCount(dataConnection.getId(), FLOW_FILE_COUNT);
        getClientUtil().stopProcessor(sourceGenerator);

        // Identify the primary and non-primary nodes. The primary node is the one holding the
        // FlowFiles produced above and is the node that will be offloaded. The non-primary node is
        // the receiver and must remain reachable for queue listings and content fetches after offload.
        final NodeDTO primaryNode = findNodeByRole(ClusterRoles.PRIMARY_NODE);
        assertNotNull(primaryNode, "Cluster does not have a Primary Node");
        final NodeDTO nonPrimaryNode = findOtherNode(primaryNode);
        assertNotNull(nonPrimaryNode, "Cluster does not have a non-primary node");

        final int nonPrimaryNodeIndex = nonPrimaryNode.getApiPort() - CLUSTERED_CLIENT_API_BASE_PORT + 1;
        switchClientToNode(nonPrimaryNodeIndex);

        // Disconnect and offload the primary node. All FlowFiles in the primary's local partition are
        // shipped to the non-primary via the load-balance receive path. On the receiver, every FlowFile
        // in the receive transaction is packed into a single ContentClaim that sits at offset > 0
        // inside the recycled Resource Claim. With FLOW_FILE_COUNT * CONTENT_BYTES_PER_FLOW_FILE bytes
        // exceeding the 50 KB truncation threshold, the shared ContentClaim is flagged as a truncation
        // candidate.
        getClientUtil().disconnectNode(primaryNode.getNodeId());
        waitForNodeStatus(primaryNode, "DISCONNECTED");
        getClientUtil().offloadNode(primaryNode.getNodeId());
        waitForNodeStatus(primaryNode, "OFFLOADED");

        waitForQueueCount(dataConnection.getId(), FLOW_FILE_COUNT);

        // Trigger a single DELETE record that references the shared, truncatable ContentClaim by
        // running TerminateFlowFile once on the receiver. Without correct truncation reference count
        // tracking on the receive path the ContentClaim's reference count is zero, so this single
        // DELETE is sufficient for updateContentClaims() to queue the shared claim for truncation
        // even though FLOW_FILE_COUNT - 1 sibling FlowFiles still point inside it.
        terminate = getNifiClient().getProcessorClient().getProcessor(terminate.getId());
        terminate = getNifiClient().getProcessorClient().runProcessorOnce(terminate);
        getClientUtil().waitForStoppedProcessor(terminate.getId());
        waitForQueueCount(dataConnection.getId(), FLOW_FILE_COUNT - 1);

        // Allow time for the periodic TruncateClaims task on the receiver to drain the truncatable
        // claim queue and call FileChannel.truncate(claim.getOffset()), which would otherwise destroy
        // the on-disk bytes for every remaining live FlowFile that references this claim.
        Thread.sleep(5_000L);

        // Read every remaining FlowFile and verify its content is intact. With the bug present, the
        // underlying Resource Claim file has been truncated back to claim.getOffset() and these reads
        // return zero bytes (or fewer than expected) for each offload FlowFile.
        final byte[] expectedContent = CONTENT_TEXT.getBytes(StandardCharsets.UTF_8);
        final int remaining = FLOW_FILE_COUNT - 1;
        for (int index = 0; index < remaining; index++) {
            final byte[] actualContent = getClientUtil().getFlowFileContentAsByteArray(dataConnection.getId(), index);
            assertArrayEquals(expectedContent, actualContent,
                    "FlowFile at index " + index + " has unexpected content; expected "
                            + expectedContent.length + " bytes, got " + actualContent.length);
        }
    }

    private NodeDTO findNodeByRole(final String role) throws Exception {
        for (final NodeDTO nodeDto : getNifiClient().getControllerClient().getNodes().getCluster().getNodes()) {
            final Set<String> roles = nodeDto.getRoles();
            if (roles != null && roles.contains(role)) {
                return nodeDto;
            }
        }
        return null;
    }

    private NodeDTO findOtherNode(final NodeDTO excludedNode) throws Exception {
        for (final NodeDTO nodeDto : getNifiClient().getControllerClient().getNodes().getCluster().getNodes()) {
            if (!nodeDto.getNodeId().equals(excludedNode.getNodeId())) {
                return nodeDto;
            }
        }
        return null;
    }
}
