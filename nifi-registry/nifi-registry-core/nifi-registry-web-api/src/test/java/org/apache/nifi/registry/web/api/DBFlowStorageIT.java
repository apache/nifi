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

import org.apache.nifi.registry.NiFiRegistryTestApiApplication;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(
        classes = NiFiRegistryTestApiApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.include=ITDBFlowStorage")
@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = "classpath:db/clearDB.sql")
public class DBFlowStorageIT extends IntegrationTestBase {

    static final Logger LOGGER = LoggerFactory.getLogger(UnsecuredNiFiRegistryClientIT.class);

    private NiFiRegistryClient client;

    @Before
    public void setup() throws IOException {
        final String baseUrl = createBaseURL();
        LOGGER.info("Using base url = " + baseUrl);

        final NiFiRegistryClientConfig clientConfig = new NiFiRegistryClientConfig.Builder()
                .baseUrl(baseUrl)
                .build();
        assertNotNull(clientConfig);

        final NiFiRegistryClient client = new JerseyNiFiRegistryClient.Builder()
                .config(clientConfig)
                .build();
        assertNotNull(client);
        this.client = client;
    }

    @After
    public void teardown() {
        try {
            client.close();
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    @Test
    public void testAll() throws IOException, NiFiRegistryException {
        final RevisionInfo initialRevision = new RevisionInfo("DBFlowStorageIT", 0L);

        // Create two buckets...

        final Bucket b1 = new Bucket();
        b1.setName("b1");
        b1.setRevision(initialRevision);

        final Bucket createdB1 = client.getBucketClient().create(b1);
        assertNotNull(createdB1);

        final Bucket b2 = new Bucket();
        b2.setName("b2");
        b2.setRevision(initialRevision);

        final Bucket createdB2 = client.getBucketClient().create(b2);
        assertNotNull(createdB2);

        // Create two flows...

        final VersionedFlow f1 = new VersionedFlow();
        f1.setName("f1");
        f1.setBucketIdentifier(createdB1.getIdentifier());
        f1.setRevision(initialRevision);

        final VersionedFlow createdF1 = client.getFlowClient().create(f1);
        assertNotNull(createdF1);

        final VersionedFlow f2 = new VersionedFlow();
        f2.setName("f2");
        f2.setBucketIdentifier(createdB2.getIdentifier());
        f2.setRevision(initialRevision);

        final VersionedFlow createdF2 = client.getFlowClient().create(f2);
        assertNotNull(createdF2);

        // Create some versions for each flow...

        final VersionedFlowSnapshot snapshotF1V1 = createSnapshot(createdB1, createdF1, 1, "f1v1");
        final VersionedFlowSnapshot createdSnapshotF1V1 = client.getFlowSnapshotClient().create(snapshotF1V1);
        assertNotNull(createdSnapshotF1V1);

        final VersionedFlowSnapshot snapshotF1V2 = createSnapshot(createdB1, createdF1, 2, "f1v2");
        final VersionedFlowSnapshot createdSnapshotF1V2 = client.getFlowSnapshotClient().create(snapshotF1V2);
        assertNotNull(createdSnapshotF1V2);

        final VersionedFlowSnapshot snapshotF2V1 = createSnapshot(createdB2, createdF2, 1, "f2v1");
        final VersionedFlowSnapshot createdSnapshotF2V1 = client.getFlowSnapshotClient().create(snapshotF2V1);
        assertNotNull(createdSnapshotF2V1);

        final VersionedFlowSnapshot snapshotF2V2 = createSnapshot(createdB2, createdF2, 2, "f2v2");
        final VersionedFlowSnapshot createdSnapshotF2V2 = client.getFlowSnapshotClient().create(snapshotF2V2);
        assertNotNull(createdSnapshotF2V2);

        // Verify retrieving flow versions...

        final VersionedFlowSnapshot retrievedSnapshotF1V1 = client.getFlowSnapshotClient().get(createdF1.getIdentifier(), 1);
        assertNotNull(retrievedSnapshotF1V1);
        assertNotNull(retrievedSnapshotF1V1.getFlowContents());
        assertEquals("f1v1", retrievedSnapshotF1V1.getFlowContents().getName());

        final VersionedFlowSnapshot retrievedSnapshotF1V2 = client.getFlowSnapshotClient().get(createdF1.getIdentifier(), 2);
        assertNotNull(retrievedSnapshotF1V2);
        assertNotNull(retrievedSnapshotF1V2.getFlowContents());
        assertEquals("f1v2", retrievedSnapshotF1V2.getFlowContents().getName());

        // Verify deleting a flow...

        client.getFlowClient().delete(createdB1.getIdentifier(), createdF1.getIdentifier(), createdF1.getRevision());

        // All versions of f1 should be deleted
        try {
            client.getFlowSnapshotClient().get(createdF1.getIdentifier(), 1);
            fail("Should have thrown exception");
        } catch (NiFiRegistryException nre) {
        }

        // Versions of f2 should still exist...
        final VersionedFlowSnapshot retrievedSnapshotF2V1 = client.getFlowSnapshotClient().get(createdF2.getIdentifier(), 1);
        assertNotNull(retrievedSnapshotF2V1);
        assertNotNull(retrievedSnapshotF2V1.getFlowContents());
        assertEquals("f2v1", retrievedSnapshotF2V1.getFlowContents().getName());
    }

    private VersionedFlowSnapshot createSnapshot(final Bucket bucket, final VersionedFlow flow, final int version, final String rootPgName) {
        final VersionedProcessGroup rootPg = new VersionedProcessGroup();
        rootPg.setName(rootPgName);

        final VersionedFlowSnapshotMetadata snapshotMetadata = new VersionedFlowSnapshotMetadata();
        snapshotMetadata.setBucketIdentifier(bucket.getIdentifier());
        snapshotMetadata.setFlowIdentifier(flow.getIdentifier());
        snapshotMetadata.setVersion(version);
        snapshotMetadata.setComments("comments");

        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
        snapshot.setFlowContents(rootPg);
        snapshot.setSnapshotMetadata(snapshotMetadata);
        return snapshot;
    }
}
