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
import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.PoliciesClient;
import org.apache.nifi.registry.client.TenantsClient;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.resource.ResourceFactory;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest(
        classes = NiFiRegistryTestApiApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.include=ITSecureDatabase")
@Import(SecureITClientConfiguration.class)
@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = {"classpath:db/clearDB.sql"})
public class SecureDatabaseIT extends IntegrationTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecureDatabaseIT.class);

    private static final String INITIAL_ADMIN_IDENTITY = "CN=user1, OU=nifi";
    private static final String OTHER_USER_IDENTITY = "CN=user2, OU=nifi";

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

    @Test
    public void testPoliciesClient() throws Exception {
        // Create a bucket...
        final Bucket bucket = new Bucket();
        bucket.setName("Bucket 1 " + System.currentTimeMillis());
        bucket.setDescription("This is bucket 1");
        bucket.setRevision(new RevisionInfo(null, 0L));

        final BucketClient bucketClient = client.getBucketClient();
        final Bucket createdBucket = bucketClient.create(bucket);
        assertNotNull(createdBucket);
        assertNotNull(createdBucket.getIdentifier());
        assertNotNull(createdBucket.getRevision());

        // Get initial users...
        final TenantsClient tenantsClient = client.getTenantsClient();

        final List<User> users = tenantsClient.getUsers();
        assertEquals(2, users.size());

        final User initialAdminUser = users.stream()
                .filter(u -> u.getIdentity().equals(INITIAL_ADMIN_IDENTITY))
                .findFirst()
                .orElse(null);
        assertNotNull(initialAdminUser);

        final User otherUser = users.stream()
                .filter(u -> u.getIdentity().equals(OTHER_USER_IDENTITY))
                .findFirst()
                .orElse(null);
        assertNotNull(otherUser);

        // Create a policy on the bucket...
        final PoliciesClient policiesClient = client.getPoliciesClient();

        final AccessPolicy readBucketAccessPolicy = new AccessPolicy();
        readBucketAccessPolicy.setResource(ResourceFactory.getBucketResource(
                createdBucket.getIdentifier(), createdBucket.getName())
                .getIdentifier());
        readBucketAccessPolicy.setAction(RequestAction.READ.toString());
        readBucketAccessPolicy.setUsers(Collections.singleton(initialAdminUser));
        readBucketAccessPolicy.setRevision(new RevisionInfo(null, 0L));

        final AccessPolicy createdAccessPolicy = policiesClient.createAccessPolicy(readBucketAccessPolicy);
        assertNotNull(createdAccessPolicy);
        assertEquals(readBucketAccessPolicy.getAction(), createdAccessPolicy.getAction());
        assertEquals(readBucketAccessPolicy.getResource(), createdAccessPolicy.getResource());
        assertEquals(1, createdAccessPolicy.getUsers().size());
        assertEquals(INITIAL_ADMIN_IDENTITY, createdAccessPolicy.getUsers().iterator().next().getIdentity());
        assertEquals(1, createdAccessPolicy.getRevision().getVersion().longValue());

        // Retrieve the policy by action + resource
        final AccessPolicy retrievedAccessPolicy = policiesClient.getAccessPolicy(
                createdAccessPolicy.getAction(), createdAccessPolicy.getResource());
        assertNotNull(retrievedAccessPolicy);
        assertEquals(createdAccessPolicy.getAction(), retrievedAccessPolicy.getAction());
        assertEquals(createdAccessPolicy.getResource(), retrievedAccessPolicy.getResource());
        assertEquals(1, retrievedAccessPolicy.getUsers().size());
        assertEquals(INITIAL_ADMIN_IDENTITY, retrievedAccessPolicy.getUsers().iterator().next().getIdentity());
        assertEquals(1, retrievedAccessPolicy.getRevision().getVersion().longValue());

        // Update the policy
        retrievedAccessPolicy.setUsers(new HashSet<>(Arrays.asList(initialAdminUser, otherUser)));

        final AccessPolicy updatedAccessPolicy = policiesClient.updateAccessPolicy(retrievedAccessPolicy);
        assertNotNull(updatedAccessPolicy);
        assertEquals(retrievedAccessPolicy.getAction(), updatedAccessPolicy.getAction());
        assertEquals(retrievedAccessPolicy.getResource(), updatedAccessPolicy.getResource());
        assertEquals(2, updatedAccessPolicy.getUsers().size());
        assertEquals(2, updatedAccessPolicy.getRevision().getVersion().longValue());
    }

}
