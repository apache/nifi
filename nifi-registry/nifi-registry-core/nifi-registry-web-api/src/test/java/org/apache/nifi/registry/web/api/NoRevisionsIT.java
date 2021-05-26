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
package org.apache.nifi.registry.web.api;

import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.junit.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import java.util.UUID;

import static org.apache.nifi.registry.web.api.IntegrationTestUtils.assertBucketsEqual;
import static org.apache.nifi.registry.web.api.IntegrationTestUtils.assertFlowSnapshotsEqual;
import static org.apache.nifi.registry.web.api.IntegrationTestUtils.assertFlowsEqual;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NoRevisionsIT extends UnsecuredNoRevisionsITBase {

    @Test
    public void testNoRevisions() {
        // Create a bucket...

        final Bucket bucket = new Bucket();
        bucket.setName("Integration Test Bucket");
        bucket.setDescription("A bucket created by an integration test.");

        final Bucket createdBucket = client
                .target(createURL("buckets"))
                .request()
                .post(Entity.entity(bucket, MediaType.APPLICATION_JSON), Bucket.class);

        assertBucketsEqual(bucket, createdBucket, false);
        assertNotNull(createdBucket.getIdentifier());

        // Update bucket...

        createdBucket.setName("Renamed Bucket");
        createdBucket.setDescription("This bucket has been updated by an integration test.");

        final Bucket updatedBucket = client
                .target(createURL("buckets/" + createdBucket.getIdentifier()))
                .request()
                .put(Entity.entity(createdBucket, MediaType.APPLICATION_JSON), Bucket.class);

        assertBucketsEqual(updatedBucket, createdBucket, true);

        // Create a flow...

        final VersionedFlow flow = new VersionedFlow();
        flow.setIdentifier(UUID.randomUUID().toString()); // Simulate NiFi sending an identifier
        flow.setBucketIdentifier(createdBucket.getIdentifier());
        flow.setName("Test Flow");
        flow.setDescription("This is a flow created by an integration test.");

        final VersionedFlow createdFlow = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", flow.getBucketIdentifier())
                .request()
                .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);

        assertFlowsEqual(flow, createdFlow, false);
        assertNotNull(createdFlow.getIdentifier());

        // Update flow...

        createdFlow.setName("Renamed Flow");
        createdFlow.setDescription("This flow has been updated by an integration test.");

        final VersionedFlow updatedFlow = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}"))
                .resolveTemplate("bucketId",flow.getBucketIdentifier())
                .resolveTemplate("flowId", createdFlow.getIdentifier())
                .request()
                .put(Entity.entity(createdFlow, MediaType.APPLICATION_JSON), VersionedFlow.class);

        assertTrue(updatedFlow.getModifiedTimestamp() > createdFlow.getModifiedTimestamp());

        // Create a version of a flow...

        final VersionedFlowSnapshotMetadata flowSnapshotMetadata = new VersionedFlowSnapshotMetadata();
        flowSnapshotMetadata.setVersion(1);
        flowSnapshotMetadata.setBucketIdentifier(createdFlow.getBucketIdentifier());
        flowSnapshotMetadata.setFlowIdentifier(createdFlow.getIdentifier());
        flowSnapshotMetadata.setComments("This is snapshot 1, created by an integration test.");

        final VersionedFlowSnapshot flowSnapshot = new VersionedFlowSnapshot();
        flowSnapshot.setSnapshotMetadata(flowSnapshotMetadata);
        flowSnapshot.setFlowContents(new VersionedProcessGroup()); // an empty root process group

        final VersionedFlowSnapshot createdFlowSnapshot = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}/versions"))
                .resolveTemplate("bucketId", flowSnapshotMetadata.getBucketIdentifier())
                .resolveTemplate("flowId", flowSnapshotMetadata.getFlowIdentifier())
                .request()
                .post(Entity.entity(flowSnapshot, MediaType.APPLICATION_JSON), VersionedFlowSnapshot.class);

        assertFlowSnapshotsEqual(flowSnapshot, createdFlowSnapshot, false);

        // Delete flow...

        final VersionedFlow deletedFlow = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}"))
                .resolveTemplate("bucketId", createdFlow.getBucketIdentifier())
                .resolveTemplate("flowId", createdFlow.getIdentifier())
                .request()
                .delete(VersionedFlow.class);

        assertNotNull(deletedFlow);

        // Delete bucket...

        final Bucket deletedBucket = client
                .target(createURL("buckets/" + createdBucket.getIdentifier()))
                .request()
                .delete(Bucket.class);

        assertNotNull(deletedBucket);
    }
}
