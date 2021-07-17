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
import org.apache.nifi.registry.authorization.ResourcePermissions;
import org.apache.nifi.registry.authorization.Tenant;
import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Deploy the Web API Application using an embedded Jetty Server for local integration testing, with the follow characteristics:
 *
 * - A NiFiRegistryProperties has to be explicitly provided to the ApplicationContext using a profile unique to this test suite.
 * - A NiFiRegistryClientConfig has been configured to create a client capable of completing two-way TLS
 * - The database is embed H2 using volatile (in-memory) persistence
 * - Custom SQL is clearing the DB before each test method by default, unless method overrides this behavior
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
        classes = NiFiRegistryTestApiApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.profiles.include=ITSecureFile")
@Import(SecureITClientConfiguration.class)
@Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = "classpath:db/clearDB.sql")
public class SecureFileIT extends IntegrationTestBase {

    @Test
    public void testAccessStatus() throws Exception {

        // Given: the client and server have been configured correctly for two-way TLS
        String expectedJson = "{" +
                "\"identity\":\"CN=user1, OU=nifi\"," +
                "\"anonymous\":false," +
                "\"resourcePermissions\":{" +
                "\"anyTopLevelResource\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"buckets\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"tenants\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"policies\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"proxy\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}}" +
                "}";

        // When: the /access endpoint is queried
        final Response response = client
                .target(createURL("access"))
                .request()
                .get(Response.class);

        // Then: the server returns 200 OK with the expected client identity
        assertEquals(200, response.getStatus());
        String actualJson = response.readEntity(String.class);
        JSONAssert.assertEquals(expectedJson, actualJson, false);
    }

    @Test
    public void testRetrieveResources() throws Exception {

        // Given: an empty registry returns these resources
        String expected = "[" +
                "{\"identifier\":\"/actuator\",\"name\":\"Actuator\"}," +
                "{\"identifier\":\"/swagger\",\"name\":\"Swagger\"}," +
                "{\"identifier\":\"/policies\",\"name\":\"Access Policies\"}," +
                "{\"identifier\":\"/tenants\",\"name\":\"Tenants\"}," +
                "{\"identifier\":\"/proxy\",\"name\":\"Proxy User Requests\"}," +
                "{\"identifier\":\"/buckets\",\"name\":\"Buckets\"}" +
                "]";

        // When: the /resources endpoint is queried
        final String resourcesJson = client
                .target(createURL("/policies/resources"))
                .request()
                .get(String.class);

        // Then: the expected array of resources is returned
        JSONAssert.assertEquals(expected, resourcesJson, false);
    }

    @Test
    public void testCreateUser() throws Exception {

        // Given: the server has been configured with FileUserGroupProvider, which is configurable,
        //   and: the initial admin client wants to create a tenant
        Long initialVersion = new Long(0);
        String clientId = UUID.randomUUID().toString();
        RevisionInfo revisionInfo = new RevisionInfo(clientId, initialVersion);

        Tenant tenant = new Tenant();
        tenant.setIdentity("New User");
        tenant.setRevision(revisionInfo);

        // When: the POST /tenants/users endpoint is accessed
        final Response createUserResponse = client
                .target(createURL("tenants/users"))
                .request()
                .post(Entity.entity(tenant, MediaType.APPLICATION_JSON_TYPE), Response.class);

        // Then: "201 created" is returned with the expected user
        assertEquals(201, createUserResponse.getStatus());
        User actualUser = createUserResponse.readEntity(User.class);
        assertNotNull(actualUser.getIdentifier());
        assertNotNull(actualUser.getRevision());
        assertNotNull(actualUser.getRevision().getVersion());

        try {
            assertEquals(tenant.getIdentity(), actualUser.getIdentity());
            assertEquals(true, actualUser.getConfigurable());
            assertEquals(0, actualUser.getUserGroups().size());
            assertEquals(0, actualUser.getAccessPolicies().size());
            assertEquals(new ResourcePermissions(), actualUser.getResourcePermissions());
        } finally {
            // cleanup user for other tests
            final long version = actualUser.getRevision().getVersion();
            client.target(createURL("tenants/users/" + actualUser.getIdentifier() + "?version=" + version))
                    .request()
                    .delete();
        }

    }

    @Test
    public void testCreateUserGroup() throws Exception {

        // Given: the server has been configured with FileUserGroupProvider, which is configurable,
        //   and: the initial admin client wants to create a tenant
        Long initialVersion = new Long(0);
        String clientId = UUID.randomUUID().toString();
        RevisionInfo revisionInfo = new RevisionInfo(clientId, initialVersion);

        Tenant tenant = new Tenant();
        tenant.setIdentity("New Group");
        tenant.setRevision(revisionInfo);

        // When: the POST /tenants/user-groups endpoint is used
        final Response createUserGroupResponse = client
                .target(createURL("tenants/user-groups"))
                .request()
                .post(Entity.entity(tenant, MediaType.APPLICATION_JSON_TYPE), Response.class);

        // Then: 201 created is returned with the expected group
        assertEquals(201, createUserGroupResponse.getStatus());
        UserGroup actualUserGroup = createUserGroupResponse.readEntity(UserGroup.class);
        assertNotNull(actualUserGroup.getIdentifier());
        assertNotNull(actualUserGroup.getRevision());
        assertNotNull(actualUserGroup.getRevision().getVersion());

        try {
            assertEquals(tenant.getIdentity(), actualUserGroup.getIdentity());
            assertEquals(true, actualUserGroup.getConfigurable());
            assertEquals(0, actualUserGroup.getUsers().size());
            assertEquals(0, actualUserGroup.getAccessPolicies().size());
            assertEquals(new ResourcePermissions(), actualUserGroup.getResourcePermissions());
        } finally {
            // cleanup user for other tests
            final long version = actualUserGroup.getRevision().getVersion();
            client.target(createURL("tenants/user-groups/" + actualUserGroup.getIdentifier() + "?version=" + version))
                    .request()
                    .delete();
        }

    }

}
