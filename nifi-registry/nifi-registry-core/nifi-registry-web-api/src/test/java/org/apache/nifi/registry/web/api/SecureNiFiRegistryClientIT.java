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
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.authorization.Permissions;
import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.client.TenantsClient;
import org.apache.nifi.registry.client.UserClient;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.client.impl.request.ProxiedEntityRequestConfig;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

import javax.ws.rs.ForbiddenException;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(
        classes = NiFiRegistryTestApiApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.include=ITSecureFile")
@Import(SecureITClientConfiguration.class)
@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = {"classpath:db/clearDB.sql", "classpath:db/FlowsIT.sql"})
public class SecureNiFiRegistryClientIT extends IntegrationTestBase {

    static final Logger LOGGER = LoggerFactory.getLogger(SecureNiFiRegistryClientIT.class);

    static final String INITIAL_ADMIN_IDENTITY = "CN=user1, OU=nifi";
    static final String NO_ACCESS_IDENTITY = "CN=no-access, OU=nifi";

    private NiFiRegistryClient client;

    @Before
    public void setup() {
        final String baseUrl = createBaseURL();
        LOGGER.info("Using base url = " + baseUrl);

        final NiFiRegistryClientConfig clientConfig = createClientConfig(baseUrl);
        Assert.assertNotNull(clientConfig);

        final NiFiRegistryClient client = new JerseyNiFiRegistryClient.Builder()
                .config(clientConfig)
                .build();
        Assert.assertNotNull(client);
        this.client = client;
    }

    @After
    public void teardown() {
        try {
            client.close();
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetAccessStatus() throws IOException, NiFiRegistryException {
        final UserClient userClient = client.getUserClient();
        final CurrentUser currentUser = userClient.getAccessStatus();
        assertEquals(INITIAL_ADMIN_IDENTITY, currentUser.getIdentity());
        assertFalse(currentUser.isAnonymous());
        assertNotNull(currentUser.getResourcePermissions());
        final Permissions fullAccess = new Permissions().withCanRead(true).withCanWrite(true).withCanDelete(true);
        assertEquals(fullAccess, currentUser.getResourcePermissions().getAnyTopLevelResource());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getBuckets());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getTenants());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getPolicies());
        assertEquals(fullAccess, currentUser.getResourcePermissions().getProxy());
    }

    @Test
    public void testCrudOperations() throws IOException, NiFiRegistryException {
        final Bucket bucket = new Bucket();
        bucket.setName("Bucket 1 " + System.currentTimeMillis());
        bucket.setDescription("This is bucket 1");
        bucket.setRevision(new RevisionInfo(null, 0L));

        final BucketClient bucketClient = client.getBucketClient();
        final Bucket createdBucket = bucketClient.create(bucket);
        assertNotNull(createdBucket);
        assertNotNull(createdBucket.getIdentifier());
        assertNotNull(createdBucket.getRevision());

        final List<Bucket> buckets = bucketClient.getAll();
        Assert.assertEquals(4, buckets.size());
        buckets.forEach(b -> assertNotNull(b.getRevision()));

        final VersionedFlow flow = new VersionedFlow();
        flow.setBucketIdentifier(createdBucket.getIdentifier());
        flow.setName("Flow 1 - " + System.currentTimeMillis());
        flow.setRevision(new RevisionInfo(null, 0L));

        final FlowClient flowClient = client.getFlowClient();
        final VersionedFlow createdFlow = flowClient.create(flow);
        assertNotNull(createdFlow);
        assertNotNull(createdFlow.getIdentifier());
        assertNotNull(createdFlow.getRevision());

        final VersionedFlowSnapshotMetadata snapshotMetadata = new VersionedFlowSnapshotMetadata();
        snapshotMetadata.setBucketIdentifier(createdFlow.getBucketIdentifier());
        snapshotMetadata.setFlowIdentifier(createdFlow.getIdentifier());
        snapshotMetadata.setVersion(1);
        snapshotMetadata.setComments("This is snapshot #1");

        final VersionedProcessGroup rootProcessGroup = new VersionedProcessGroup();
        rootProcessGroup.setIdentifier("root-pg");
        rootProcessGroup.setName("Root Process Group");

        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
        snapshot.setSnapshotMetadata(snapshotMetadata);
        snapshot.setFlowContents(rootProcessGroup);

        final FlowSnapshotClient snapshotClient = client.getFlowSnapshotClient();
        final VersionedFlowSnapshot createdSnapshot = snapshotClient.create(snapshot);
        assertNotNull(createdSnapshot);
        assertEquals(INITIAL_ADMIN_IDENTITY, createdSnapshot.getSnapshotMetadata().getAuthor());
    }

    @Test
    public void testGetAccessStatusWithProxiedEntity() throws IOException, NiFiRegistryException {
        final String proxiedEntity = "user2";
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        final UserClient userClient = client.getUserClient(requestConfig);
        final CurrentUser status = userClient.getAccessStatus();
        assertEquals("user2", status.getIdentity());
        assertFalse(status.isAnonymous());
    }

    @Test
    public void testCreatedBucketWithProxiedEntity() throws IOException, NiFiRegistryException {
        final String proxiedEntity = "user2";
        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        final BucketClient bucketClient = client.getBucketClient(requestConfig);

        final Bucket bucket = new Bucket();
        bucket.setName("Bucket 1");
        bucket.setDescription("This is bucket 1");
        bucket.setRevision(new RevisionInfo(null, 0L));

        try {
            bucketClient.create(bucket);
            fail("Shouldn't have been able to create a bucket");
        } catch (Exception e) {

        }
    }

    @Test
    public void testDirectFlowAccess() throws IOException {
        // this user shouldn't have access to anything
        final String proxiedEntity = NO_ACCESS_IDENTITY;

        final RequestConfig requestConfig = new ProxiedEntityRequestConfig(proxiedEntity);
        final FlowClient proxiedFlowClient = client.getFlowClient(requestConfig);
        final FlowSnapshotClient proxiedFlowSnapshotClient = client.getFlowSnapshotClient(requestConfig);

        try {
            proxiedFlowClient.get("1");
            fail("Shouldn't have been able to retrieve flow");
        } catch (NiFiRegistryException e) {
            assertTrue(e.getCause()  instanceof ForbiddenException);
        }

        try {
            proxiedFlowSnapshotClient.getLatest("1");
            fail("Shouldn't have been able to retrieve flow");
        } catch (NiFiRegistryException e) {
            assertTrue(e.getCause()  instanceof ForbiddenException);
        }

        try {
            proxiedFlowSnapshotClient.getLatestMetadata("1");
            fail("Shouldn't have been able to retrieve flow");
        } catch (NiFiRegistryException e) {
            assertTrue(e.getCause()  instanceof ForbiddenException);
        }

        try {
            proxiedFlowSnapshotClient.get("1", 1);
            fail("Shouldn't have been able to retrieve flow");
        } catch (NiFiRegistryException e) {
            assertTrue(e.getCause()  instanceof ForbiddenException);
        }

        try {
            proxiedFlowSnapshotClient.getSnapshotMetadata("1");
            fail("Shouldn't have been able to retrieve flow");
        } catch (NiFiRegistryException e) {
            assertTrue(e.getCause()  instanceof ForbiddenException);
        }

    }

    @Test
    public void testTenantsClientUsers() throws Exception {
        final TenantsClient tenantsClient = client.getTenantsClient();

        // get all users
        final List<User> users = tenantsClient.getUsers();
        assertEquals(2, users.size());

        final User initialAdminUser = users.stream()
                .filter(u -> u.getIdentity().equals(INITIAL_ADMIN_IDENTITY))
                .findFirst()
                .orElse(null);
        assertNotNull(initialAdminUser);

        // get user by id
        final User retrievedInitialAdminUser = tenantsClient.getUser(initialAdminUser.getIdentifier());
        assertNotNull(retrievedInitialAdminUser);
        assertEquals(initialAdminUser.getIdentity(), retrievedInitialAdminUser.getIdentity());

        // add user
        final User userToAdd = new User();
        userToAdd.setIdentity("some-new-user");
        userToAdd.setRevision(new RevisionInfo(null, 0L));

        final User createdUser = tenantsClient.createUser(userToAdd);
        assertNotNull(createdUser);
        assertEquals(3, tenantsClient.getUsers().size());

        // update user
        createdUser.setIdentity(createdUser.getIdentity() + "-updated");
        final User updatedUser = tenantsClient.updateUser(createdUser);
        assertNotNull(updatedUser);
        assertEquals(createdUser.getIdentity(), updatedUser.getIdentity());

        // delete user
        final User deletedUser = tenantsClient.deleteUser(updatedUser.getIdentifier(), updatedUser.getRevision());
        assertNotNull(deletedUser);
        assertEquals(updatedUser.getIdentifier(), deletedUser.getIdentifier());
    }

    @Test
    public void testTenantsClientGroups() throws Exception {
        final TenantsClient tenantsClient = client.getTenantsClient();

        // get all groups
        final List<UserGroup> groups = tenantsClient.getUserGroups();
        assertEquals(0, groups.size());

        // create group
        final UserGroup userGroup = new UserGroup();
        userGroup.setIdentity("some-new group");
        userGroup.setRevision(new RevisionInfo(null, 0L));

        final UserGroup createdGroup = tenantsClient.createUserGroup(userGroup);
        assertNotNull(createdGroup);
        assertEquals(userGroup.getIdentity(), createdGroup.getIdentity());

        // get group by id
        final UserGroup retrievedGroup = tenantsClient.getUserGroup(createdGroup.getIdentifier());
        assertNotNull(retrievedGroup);
        assertEquals(createdGroup.getIdentifier(), retrievedGroup.getIdentifier());

        // update group
        retrievedGroup.setIdentity(retrievedGroup.getIdentity() + "-updated");
        final UserGroup updatedGroup = tenantsClient.updateUserGroup(retrievedGroup);
        assertEquals(retrievedGroup.getIdentity(), updatedGroup.getIdentity());

        // delete group
        final UserGroup deletedGroup = tenantsClient.deleteUserGroup(updatedGroup.getIdentifier(), updatedGroup.getRevision());
        assertNotNull(deletedGroup);
        assertEquals(retrievedGroup.getIdentifier(), deletedGroup.getIdentifier());

    }
}
