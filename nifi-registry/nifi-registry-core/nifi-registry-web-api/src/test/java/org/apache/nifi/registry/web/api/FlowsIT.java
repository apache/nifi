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

import java.io.File;
import org.apache.nifi.registry.bucket.BucketItemType;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.junit.Assert;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.jdbc.Sql;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.apache.nifi.registry.web.api.IntegrationTestUtils.assertBucketsEqual;
import static org.apache.nifi.registry.web.api.IntegrationTestUtils.assertFlowSnapshotMetadataEqual;
import static org.apache.nifi.registry.web.api.IntegrationTestUtils.assertFlowSnapshotsEqual;
import static org.apache.nifi.registry.web.api.IntegrationTestUtils.assertFlowsEqual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = {"classpath:db/clearDB.sql", "classpath:db/FlowsIT.sql"})
public class FlowsIT extends UnsecuredITBase {

    private static final int LATEST_VERSION = -1;

    @Test
    public void testGetFlowsEmpty() throws Exception {

        // Given: an empty bucket with id "3" (see FlowsIT.sql)
        final String emptyBucketId = "3";

        // When: the /buckets/{id}/flows endpoint is queried

        final VersionedFlow[] flows = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", emptyBucketId)
                .request()
                .get(VersionedFlow[].class);

        // Then: an empty array is returned

        assertNotNull(flows);
        assertEquals(0, flows.length);
    }

    // NOTE: The tests that seed the DB directly from SQL end up with different results for the timestamp depending on
    // which DB is used, so for now these types of tests only run against H2.
    @Test
    @IfProfileValue(name="current.database.is.h2", value="true")
    public void testGetFlows() throws Exception {

        // Given: a few buckets and flows have been populated in the DB (see FlowsIT.sql)

        final String prePopulatedBucketId = "1";
        final String expected = "[" +
                "{\"identifier\":\"1\"," +
                "\"name\":\"Flow 1\"," +
                "\"description\":\"This is flow 1\"," +
                "\"bucketIdentifier\":\"1\"," +
                "\"createdTimestamp\":1505088000000," +
                "\"modifiedTimestamp\":1505088000000," +
                "\"type\":\"Flow\"," +
                "\"permissions\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"link\":{\"params\":{\"rel\":\"self\"},\"href\":\"buckets/1/flows/1\"}}," +
                "{\"identifier\":\"2\",\"name\":\"Flow 2\"," +
                "\"description\":\"This is flow 2\"," +
                "\"bucketIdentifier\":\"1\"," +
                "\"createdTimestamp\":1505088000000," +
                "\"modifiedTimestamp\":1505088000000," +
                "\"type\":\"Flow\"," +
                "\"permissions\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"versionCount\":0," +
                "\"link\":{\"params\":{\"rel\":\"self\"},\"href\":\"buckets/1/flows/2\"}}" +
                "]";

        // When: the /buckets/{id}/flows endpoint is queried

        final String flowsJson = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", prePopulatedBucketId)
                .request()
                .get(String.class);

        // Then: the pre-populated list of flows is returned

        JSONAssert.assertEquals(expected, flowsJson, false);
    }

    @Test
    public void testCreateFlowGetFlow() throws Exception {
        final RevisionInfo initialRevision = new RevisionInfo("FlowsIT", 0L);

        // Given: an empty bucket with id "3" (see FlowsIT.sql)

        long testStartTime = System.currentTimeMillis();
        final String bucketId = "3";

        // When: a flow is created

        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setName("Test Flow");
        flow.setDescription("This is a flow created by an integration test.");
        flow.setRevision(initialRevision);

        final VersionedFlow createdFlow = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", bucketId)
                .request()
                .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);

        // Then: the server returns the created flow, with server-set fields populated correctly

        assertFlowsEqual(flow, createdFlow, false);
        assertNotNull(createdFlow.getIdentifier());
        assertNotNull(createdFlow.getBucketName());
        assertEquals(0, createdFlow.getVersionCount());
        assertEquals(createdFlow.getType(), BucketItemType.Flow);
        assertTrue(createdFlow.getCreatedTimestamp() - testStartTime > 0L); // both server and client in same JVM, so there shouldn't be skew
        assertEquals(createdFlow.getCreatedTimestamp(), createdFlow.getModifiedTimestamp());
        assertNotNull(createdFlow.getLink());
        assertNotNull(createdFlow.getLink().getUri());
        assertNotNull(createdFlow.getRevision());
        assertEquals(initialRevision.getClientId(), createdFlow.getRevision().getClientId());
        assertEquals(initialRevision.getVersion() + 1, createdFlow.getRevision().getVersion().longValue());

        // And when .../flows is queried, then the newly created flow is returned in the list

        final VersionedFlow[] flows = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", bucketId)
                .request()
                .get(VersionedFlow[].class);
        assertNotNull(flows);
        assertEquals(1, flows.length);
        assertFlowsEqual(createdFlow, flows[0], true);

        // And when the link URI is queried, then the newly created flow is returned

        final VersionedFlow flowByLink = client
                .target(createURL(flows[0].getLink().getUri().toString()))
                .request()
                .get(VersionedFlow.class);
        assertFlowsEqual(createdFlow, flowByLink, true);

        // And when the bucket is queried by .../flows/ID, then the newly created flow is returned

        final VersionedFlow flowById = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", createdFlow.getIdentifier())
                .request()
                .get(VersionedFlow.class);
        assertFlowsEqual(createdFlow, flowById, true);
        assertNotNull(flowById.getRevision());
        assertEquals(initialRevision.getClientId(), flowById.getRevision().getClientId());
        assertEquals(initialRevision.getVersion() + 1, flowById.getRevision().getVersion().longValue());

    }

    @Test
    public void testUpdateFlow() throws Exception {
        final RevisionInfo initialRevision = new RevisionInfo("FlowsIT", 0L);

        // Given: a flow exists on the server

        final String bucketId = "3";
        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setName("Test Flow");
        flow.setDescription("This is a flow created by an integration test.");
        flow.setRevision(initialRevision);

        final VersionedFlow createdFlow = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", bucketId)
                .request()
                .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);

        // When: the flow is modified by the client and updated on the server

        createdFlow.setName("Renamed Flow");
        createdFlow.setDescription("This flow has been updated by an integration test.");

        final VersionedFlow updatedFlow = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", createdFlow.getIdentifier())
                .request()
                .put(Entity.entity(createdFlow, MediaType.APPLICATION_JSON), VersionedFlow.class);

        // Then: the server returns the updated flow, with a new modified timestamp

        assertTrue(updatedFlow.getModifiedTimestamp() > createdFlow.getModifiedTimestamp());
        createdFlow.setModifiedTimestamp(updatedFlow.getModifiedTimestamp());
        assertFlowsEqual(createdFlow, updatedFlow, true);
    }

    @Test
    public void testUpdateFlowWithIncorrectRevision() throws Exception {
        final RevisionInfo initialRevision = new RevisionInfo("FlowsIT", 0L);

        // Given: a flow exists on the server

        final String bucketId = "3";
        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setName("Test Flow");
        flow.setDescription("This is a flow created by an integration test.");
        flow.setRevision(initialRevision);

        final VersionedFlow createdFlow = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", bucketId)
                .request()
                .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);

        // When: the flow revision has no clientId and the incorrect version

        createdFlow.setName("Renamed Flow");
        createdFlow.setDescription("This flow has been updated by an integration test.");
        createdFlow.getRevision().setClientId(null);
        createdFlow.getRevision().setVersion(99L);

        final Response response = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", createdFlow.getIdentifier())
                .request()
                .put(Entity.entity(createdFlow, MediaType.APPLICATION_JSON));

        // Then: 400 bad request because of the incorrect version sent

        assertEquals(400, response.getStatus());

    }

    @Test
    public void testDeleteFlow() throws Exception {
        final RevisionInfo initialRevision = new RevisionInfo("FlowsIT", 0L);

        // Given: a flow exists on the server

        final String bucketId = "3";
        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setName("Test Flow");
        flow.setDescription("This is a flow created by an integration test.");
        flow.setRevision(initialRevision);

        final VersionedFlow createdFlow = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", bucketId)
                .request()
                .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);

        // When: the flow is deleted

        final VersionedFlow deletedFlow = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", createdFlow.getIdentifier())
                .queryParam("version", createdFlow.getRevision().getVersion().longValue())
                .request()
                .delete(VersionedFlow.class);

        // Then: the body of the server response matches the flow that was deleted
        //  and: the flow is no longer accessible (resource not found)

        createdFlow.setLink(null); // self URI will not be present in deletedBucket
        assertFlowsEqual(createdFlow, deletedFlow, true);

        final Response response = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", createdFlow.getIdentifier())
                .request()
                .get();
        assertEquals(404, response.getStatus());

    }

    @Test
    public void testDeleteFlowWithIncorrectRevision() throws Exception {
        final RevisionInfo initialRevision = new RevisionInfo("FlowsIT", 0L);

        // Given: a flow exists on the server

        final String bucketId = "3";
        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setName("Test Flow");
        flow.setDescription("This is a flow created by an integration test.");
        flow.setRevision(initialRevision);

        final VersionedFlow createdFlow = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", bucketId)
                .request()
                .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);

        // When: the flow is deleted

        final Response response = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", createdFlow.getIdentifier())
                .queryParam("version", 99L)
                .request()
                .delete();

        // Then: 400 bad request because of the incorrect version sent

        assertEquals(400, response.getStatus());
    }

    @Test
    public void testGetFlowVersionsEmpty() throws Exception {

        // Given: a Bucket "2" containing a flow "3" with no snapshots (see FlowsIT.sql)
        final String bucketId = "2";
        final String flowId = "3";

        // When: the /buckets/{id}/flows/{id}/versions endpoint is queried

        final VersionedFlowSnapshot[] flowSnapshots = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}/versions"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", flowId)
                .request()
                .get(VersionedFlowSnapshot[].class);

        // Then: an empty array is returned

        assertNotNull(flowSnapshots);
        assertEquals(0, flowSnapshots.length);
    }

    // NOTE: The tests that seed the DB directly from SQL end up with different results for the timestamp depending on
    // which DB is used, so for now these types of tests only run against H2.
    @Test
    @IfProfileValue(name="current.database.is.h2", value="true")
    public void testGetFlowVersions() throws Exception {

        // Given: a bucket "1" with flow "1" with existing snapshots has been populated in the DB (see FlowsIT.sql)

        final String prePopulatedBucketId = "1";
        final String prePopulatedFlowId = "1";
        // For this test case, the order of the expected list matters as we are asserting a strict equality check
        final String expected = "[" +
                "{\"bucketIdentifier\":\"1\"," +
                "\"flowIdentifier\":\"1\"," +
                "\"version\":2," +
                "\"timestamp\":1505174400000," +
                "\"author\" : \"user2\"," +
                "\"comments\":\"This is flow 1 snapshot 2\"," +
                "\"link\":{\"params\":{\"rel\":\"content\"},\"href\":\"buckets/1/flows/1/versions/2\"}}," +
                "{\"bucketIdentifier\":\"1\"," +
                "\"flowIdentifier\":\"1\"," +
                "\"version\":1," +
                "\"timestamp\":1505088000000," +
                "\"author\" : \"user1\"," +
                "\"comments\":\"This is flow 1 snapshot 1\"," +
                "\"link\":{\"params\":{\"rel\":\"content\"},\"href\":\"buckets/1/flows/1/versions/1\"}}" +
                "]";

        // When: the /buckets/{id}/flows/{id}/versions endpoint is queried
        final String flowSnapshotsJson = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}/versions"))
                .resolveTemplate("bucketId", prePopulatedBucketId)
                .resolveTemplate("flowId", prePopulatedFlowId)
                .request()
                .get(String.class);

        // Then: the pre-populated list of flow versions is returned, in descending order
        JSONAssert.assertEquals(expected, flowSnapshotsJson, true);

    }

    @Test
    public void testCreateFlowVersionGetFlowVersion() throws Exception {
        final RevisionInfo initialRevision = new RevisionInfo("FlowsIT", 0L);

        // Given: an empty Bucket "3" (see FlowsIT.sql) with a newly created flow

        long testStartTime = System.currentTimeMillis();
        final String bucketId = "2";
        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setName("Test Flow for creating snapshots");
        flow.setDescription("This is a randomly named flow created by an integration test for the purpose of holding snapshots.");
        flow.setRevision(initialRevision);

        final VersionedFlow createdFlow = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", bucketId)
                .request()
                .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);
        final String flowId = createdFlow.getIdentifier();

        // When: an initial flow snapshot is created *without* a version

        final VersionedFlowSnapshotMetadata flowSnapshotMetadata = new VersionedFlowSnapshotMetadata();
        flowSnapshotMetadata.setBucketIdentifier("2");
        flowSnapshotMetadata.setFlowIdentifier(flowId);
        flowSnapshotMetadata.setComments("This is snapshot 1, created by an integration test.");
        final VersionedFlowSnapshot flowSnapshot = new VersionedFlowSnapshot();
        flowSnapshot.setSnapshotMetadata(flowSnapshotMetadata);
        flowSnapshot.setFlowContents(new VersionedProcessGroup()); // an empty root process group

        WebTarget clientRequestTarget = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}/versions"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", flowId);
        final Response response =
                clientRequestTarget.request().post(Entity.entity(flowSnapshot, MediaType.APPLICATION_JSON), Response.class);

        // Then: an error is returned because version != 1

        assertEquals(400, response.getStatus());

        // But When: an initial flow snapshot is created with version == 1

        flowSnapshot.getSnapshotMetadata().setVersion(1);
        final VersionedFlowSnapshot createdFlowSnapshot =
                clientRequestTarget.request().post(Entity.entity(flowSnapshot, MediaType.APPLICATION_JSON), VersionedFlowSnapshot.class);

        // Then: the server returns the created flow snapshot, with server-set fields populated correctly :)

        assertFlowSnapshotsEqual(flowSnapshot, createdFlowSnapshot, false);
        assertTrue(createdFlowSnapshot.getSnapshotMetadata().getTimestamp() - testStartTime > 0L); // both server and client in same JVM, so there shouldn't be skew
        assertEquals("anonymous", createdFlowSnapshot.getSnapshotMetadata().getAuthor());
        assertNotNull(createdFlowSnapshot.getSnapshotMetadata().getLink());
        assertNotNull(createdFlowSnapshot.getSnapshotMetadata().getLink().getUri());
        assertNotNull(createdFlowSnapshot.getFlow());
        assertEquals(1, createdFlowSnapshot.getFlow().getVersionCount());
        assertNotNull(createdFlowSnapshot.getBucket());

        // And when .../flows/{id}/versions is queried, then the newly created flow snapshot is returned in the list

        final VersionedFlowSnapshotMetadata[] versionedFlowSnapshots =
                clientRequestTarget.request().get(VersionedFlowSnapshotMetadata[].class);
        assertNotNull(versionedFlowSnapshots);
        assertEquals(1, versionedFlowSnapshots.length);
        assertFlowSnapshotMetadataEqual(createdFlowSnapshot.getSnapshotMetadata(), versionedFlowSnapshots[0], true);

        // And when the link URI is queried, then the newly created flow snapshot is returned

        final VersionedFlowSnapshot flowSnapshotByLink = client
                .target(createURL(versionedFlowSnapshots[0].getLink().getUri().toString()))
                .request()
                .get(VersionedFlowSnapshot.class);
        assertFlowSnapshotsEqual(createdFlowSnapshot, flowSnapshotByLink, true);
        assertNotNull(flowSnapshotByLink.getFlow());
        assertNotNull(flowSnapshotByLink.getBucket());

        // And when the bucket is queried by .../versions/{v}, then the newly created flow snapshot is returned

        final VersionedFlowSnapshot flowSnapshotByVersionNumber = clientRequestTarget.path("/1").request().get(VersionedFlowSnapshot.class);
        assertFlowSnapshotsEqual(createdFlowSnapshot, flowSnapshotByVersionNumber, true);
        assertNotNull(flowSnapshotByVersionNumber.getFlow());
        assertNotNull(flowSnapshotByVersionNumber.getBucket());

        // And when the latest URI is queried, then the newly created flow snapshot is returned

        final VersionedFlowSnapshot flowSnapshotByLatest = clientRequestTarget.path("/latest").request().get(VersionedFlowSnapshot.class);
        assertFlowSnapshotsEqual(createdFlowSnapshot, flowSnapshotByLatest, true);
        assertNotNull(flowSnapshotByLatest.getFlow());
        assertNotNull(flowSnapshotByLatest.getBucket());

    }

    @Test
    public void testFlowNameUniquePerBucket() throws Exception {

        final String flowName = "Flow 1";

        // verify we have an existing flow with the name "Flow 1" in bucket 1
        final VersionedFlow existingFlow = client
                .target(createURL("buckets/1/flows/1"))
                .request()
                .get(VersionedFlow.class);

        assertNotNull(existingFlow);
        assertEquals(flowName, existingFlow.getName());

        // create a new flow with the same name

        final String bucketId = "3";

        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setName(flowName);
        flow.setDescription("This is a flow created by an integration test.");
        flow.setRevision(new RevisionInfo("FlowsIT", 0L));

        // saving this flow to bucket 3 should work because bucket 3 is empty

        final VersionedFlow createdFlow = client
                .target(createURL("buckets/3/flows"))
                .resolveTemplate("bucketId", bucketId)
                .request()
                .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);

        assertNotNull(createdFlow);

        // saving the flow to bucket 1 should not work because there is a flow with the same name
        flow.setBucketIdentifier("1");
        try {
            client.target(createURL("buckets/1/flows"))
                    .resolveTemplate("bucketId", bucketId)
                    .request()
                    .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);

            Assert.fail("Should have thrown exception");
        } catch (WebApplicationException e) {
            final String errorMessage = e.getResponse().readEntity(String.class);
            Assert.assertEquals("A versioned flow with the same name already exists in the selected bucket", errorMessage);
        }

    }

    @Test
    public void testImportVersionedFlowSnapshot() {
        final RevisionInfo initialRevision = new RevisionInfo("FlowsIT", 0L);

        // Create a versionedFlowSnapshot to export
        // Given: an empty Bucket "3" (see FlowsIT.sql) with a newly created flow

        final String bucketId = "3";
        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setName("Test Flow for creating snapshots");
        flow.setDescription("This is a randomly named flow created by an integration test for the purpose of holding snapshots.");
        flow.setRevision(initialRevision);

        final VersionedFlow createdFlow = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", bucketId)
                .request()
                .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);
        final String flowId = createdFlow.getIdentifier();

        // Create snapshotMetadata
        final VersionedFlowSnapshotMetadata flowSnapshotMetadata = new VersionedFlowSnapshotMetadata();
        flowSnapshotMetadata.setBucketIdentifier(bucketId);
        flowSnapshotMetadata.setFlowIdentifier(flowId);
        flowSnapshotMetadata.setComments("This is a snapshot created by an integration test.");

        // Create a VersionedFlowSnapshot
        final VersionedFlowSnapshot flowSnapshot = new VersionedFlowSnapshot();
        flowSnapshot.setSnapshotMetadata(flowSnapshotMetadata);
        flowSnapshot.setFlowContents(new VersionedProcessGroup()); // an empty root process group
        flowSnapshot.getFlowContents().setName("Test Flow name");
        flowSnapshot.getSnapshotMetadata().setVersion(LATEST_VERSION);

        final VersionedFlowSnapshot createdFlowSnapshot = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}/versions"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", flowId)
                .request()
                .post(Entity.entity(flowSnapshot, MediaType.APPLICATION_JSON), VersionedFlowSnapshot.class);

        assertNotNull(createdFlowSnapshot.getFlow());

        final VersionedFlowSnapshot importedFlowSnapshot = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}/versions/import"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", flowId)
                .request()
                .post(Entity.entity(createdFlowSnapshot, MediaType.APPLICATION_JSON), VersionedFlowSnapshot.class);

        assertNotNull(importedFlowSnapshot);
        assertEquals(bucketId, importedFlowSnapshot.getSnapshotMetadata().getBucketIdentifier());
        assertEquals(flowId, importedFlowSnapshot.getSnapshotMetadata().getFlowIdentifier());
        assertEquals(2, importedFlowSnapshot.getSnapshotMetadata().getVersion());

        // =========== Import a Versioned Flow Snapshot ===========

        // GET the versioned Flow that was just imported

        final VersionedFlowSnapshotMetadata[] versionedFlowSnapshots = client
                        .target(createURL("buckets/{bucketId}/flows/{flowId}/versions"))
                        .resolveTemplate("bucketId", bucketId)
                        .resolveTemplate("flowId", flowId)
                        .request().get(VersionedFlowSnapshotMetadata[].class);
        assertNotNull(versionedFlowSnapshots);
        assertEquals(2, versionedFlowSnapshots.length);
        assertFlowSnapshotMetadataEqual(importedFlowSnapshot.getSnapshotMetadata(), versionedFlowSnapshots[0], true);

        // GET the imported versionedFlowSnapshot by link

        final VersionedFlowSnapshot importedFlowSnapshotByLink = client
                .target(createURL(versionedFlowSnapshots[0].getLink().getUri().toString()))
                .request()
                .get(VersionedFlowSnapshot.class);
        assertFlowSnapshotsEqual(importedFlowSnapshot, importedFlowSnapshotByLink, true);

        // =========== Import another version ===========

        final File testSnapshotFile = new File("src/test/resources/test-versioned-flow-snapshot.json");

        // Imported Flow id = 2
        final String importedFlowId = importedFlowSnapshot.getSnapshotMetadata().getFlowIdentifier();
        // Imported Bucket id = 3
        final String importedBucketId = importedFlowSnapshot.getSnapshotMetadata().getBucketIdentifier();

        WebTarget clientRequestTarget = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}/versions/import"))
                .resolveTemplate("bucketId", importedBucketId)
                .resolveTemplate("flowId", importedFlowId);

        final VersionedFlowSnapshot nextImportedFlowSnapshot = clientRequestTarget
                .request(MediaType.APPLICATION_JSON)
                .header("content-type", MediaType.APPLICATION_JSON)
                .header("comments", "This is a test version")
                .post(Entity.entity(testSnapshotFile, MediaType.APPLICATION_JSON), VersionedFlowSnapshot.class);

        assertNotNull(nextImportedFlowSnapshot);
        assertBucketsEqual(importedFlowSnapshot.getBucket(), nextImportedFlowSnapshot.getBucket(), true);
        assertEquals(importedFlowId, nextImportedFlowSnapshot.getSnapshotMetadata().getFlowIdentifier());
        assertEquals(3, nextImportedFlowSnapshot.getSnapshotMetadata().getVersion());
    }

    @Test
    public void testExportVersionedFlowSnapshot() {
        final RevisionInfo initialRevision = new RevisionInfo("FlowsIT", 0L);

        // Create a versionedFlowSnapshot to export
        // Given: an empty Bucket "2" (see FlowsIT.sql) with a newly created flow

        final String bucketId = "2";
        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setName("Test Flow for creating snapshots");
        flow.setDescription("This is a randomly named flow created by an integration test for the purpose of holding snapshots.");
        flow.setRevision(initialRevision);

        final VersionedFlow createdFlow = client
                .target(createURL("buckets/{bucketId}/flows"))
                .resolveTemplate("bucketId", bucketId)
                .request()
                .post(Entity.entity(flow, MediaType.APPLICATION_JSON), VersionedFlow.class);
        final String flowId = createdFlow.getIdentifier();

        // Create snapshotMetadata
        final VersionedFlowSnapshotMetadata flowSnapshotMetadata = new VersionedFlowSnapshotMetadata();
        flowSnapshotMetadata.setBucketIdentifier(bucketId);
        flowSnapshotMetadata.setFlowIdentifier(flowId);
        flowSnapshotMetadata.setComments("This is a snapshot created by an integration test.");

        // Create a VersionedFlowSnapshot
        final VersionedFlowSnapshot flowSnapshot = new VersionedFlowSnapshot();
        flowSnapshot.setSnapshotMetadata(flowSnapshotMetadata);
        flowSnapshot.setFlowContents(new VersionedProcessGroup()); // an empty root process group
        flowSnapshot.getFlowContents().setName("Test Flow name");
        flowSnapshot.getSnapshotMetadata().setVersion(LATEST_VERSION);

        final VersionedFlowSnapshot createdFlowSnapshot = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}/versions"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", flowId)
                .request()
                .post(Entity.entity(flowSnapshot, MediaType.APPLICATION_JSON), VersionedFlowSnapshot.class);

        assertNotNull(createdFlowSnapshot.getFlow());
        assertEquals(1, createdFlowSnapshot.getFlow().getVersionCount());

        // Get the version number
        final Integer testVersionNumber = createdFlowSnapshot.getSnapshotMetadata().getVersion();

        // Test the exportVersionedFlow method with the version that was just created
        final VersionedFlowSnapshot exportedVersionedFlowSnapshot = client
                .target(createURL("buckets/{bucketId}/flows/{flowId}/versions/{versionNumber: \\d+}/export"))
                .resolveTemplate("bucketId", bucketId)
                .resolveTemplate("flowId", flowId)
                .resolveTemplate("versionNumber", testVersionNumber)
                .request()
                .get(VersionedFlowSnapshot.class);

        assertNotNull(exportedVersionedFlowSnapshot);
        assertEquals(createdFlowSnapshot.getSnapshotMetadata().getVersion(),
                exportedVersionedFlowSnapshot.getSnapshotMetadata().getVersion());
        assertNull(exportedVersionedFlowSnapshot.getBucket());
        assertNull(exportedVersionedFlowSnapshot.getFlow());
        assertNull(exportedVersionedFlowSnapshot.getSnapshotMetadata().getFlowIdentifier());
        assertNull(exportedVersionedFlowSnapshot.getSnapshotMetadata().getBucketIdentifier());
        assertNull(exportedVersionedFlowSnapshot.getSnapshotMetadata().getLink());
    }
}
