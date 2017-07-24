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
package org.apache.nifi.integration.accesscontrol;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.integration.util.NiFiTestAuthorizer;
import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.integration.accesscontrol.AccessControlHelper.NONE_CLIENT_ID;
import static org.apache.nifi.integration.accesscontrol.AccessControlHelper.READ_CLIENT_ID;
import static org.apache.nifi.integration.accesscontrol.AccessControlHelper.READ_WRITE_CLIENT_ID;
import static org.apache.nifi.integration.accesscontrol.AccessControlHelper.WRITE_CLIENT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Access control test for connections.
 */
public class ITConnectionAccessControl {

    private static AccessControlHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AccessControlHelper();
    }

    /**
     * Ensures the READ user can get a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserGetConnection() throws Exception {
        final ConnectionEntity entity = getRandomConnection(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the READ WRITE user can get a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserGetConnection() throws Exception {
        final ConnectionEntity entity = getRandomConnection(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the WRITE user can get a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserGetConnection() throws Exception {
        final ConnectionEntity entity = getRandomConnection(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the NONE user can get a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserGetConnection() throws Exception {
        final ConnectionEntity entity = getRandomConnection(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the READ user cannot put a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserPutConnection() throws Exception {
        final ConnectionEntity entity = getRandomConnection(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        // attempt update the name
        entity.getRevision().setClientId(READ_CLIENT_ID);
        entity.getComponent().setName("Updated Name");

        // perform the request
        final ClientResponse response = updateConnection(helper.getReadUser(), entity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ_WRITE user can put a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutConnection() throws Exception {
        final ConnectionEntity entity = getRandomConnection(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String updatedName = "Updated Name";

        // attempt to update the name
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(AccessControlHelper.READ_WRITE_CLIENT_ID);
        entity.getComponent().setName(updatedName);

        // perform the request
        final ClientResponse response = updateConnection(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ConnectionEntity responseEntity = response.getEntity(ConnectionEntity.class);

        // verify
        assertEquals(READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(updatedName, responseEntity.getComponent().getName());
    }

    /**
     * Ensures the READ_WRITE user can put a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutConnectionThroughInheritedPolicy() throws Exception {
        final ConnectionEntity entity = createConnection(NiFiTestAuthorizer.NO_POLICY_COMPONENT_NAME);

        final String updatedName = "Updated name";

        // attempt to update the name
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(READ_WRITE_CLIENT_ID);
        entity.getComponent().setName(updatedName);

        // perform the request
        final ClientResponse response = updateConnection(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ConnectionEntity responseEntity = response.getEntity(ConnectionEntity.class);

        // verify
        assertEquals(AccessControlHelper.READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(updatedName, responseEntity.getComponent().getName());
    }

    /**
     * Ensures the WRITE user can put a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserPutConnection() throws Exception {
        final ConnectionEntity entity = getRandomConnection(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final String updatedName = "Updated Name";

        // attempt to update the name
        final ConnectionDTO requestDto = new ConnectionDTO();
        requestDto.setId(entity.getId());
        requestDto.setName(updatedName);

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.WRITE_CLIENT_ID);

        final ConnectionEntity requestEntity = new ConnectionEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateConnection(helper.getWriteUser(), requestEntity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ConnectionEntity responseEntity = response.getEntity(ConnectionEntity.class);

        // verify
        assertEquals(WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
    }

    /**
     * Ensures the NONE user cannot put a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserPutConnection() throws Exception {
        final ConnectionEntity entity = getRandomConnection(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final String updatedName = "Updated Name";

        // attempt to update the name
        final ConnectionDTO requestDto = new ConnectionDTO();
        requestDto.setId(entity.getId());
        requestDto.setName(updatedName);

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.NONE_CLIENT_ID);

        final ConnectionEntity requestEntity = new ConnectionEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateConnection(helper.getNoneUser(), requestEntity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ user cannot delete a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserDeleteConnection() throws Exception {
        verifyDelete(helper.getReadUser(), AccessControlHelper.READ_CLIENT_ID, 403);
    }

    /**
     * Ensures the READ WRITE user can delete a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserDeleteConnection() throws Exception {
        verifyDelete(helper.getReadWriteUser(), AccessControlHelper.READ_WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the WRITE user can delete a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserDeleteConnection() throws Exception {
        verifyDelete(helper.getWriteUser(), AccessControlHelper.WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the NONE user can delete a connection.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserDeleteConnection() throws Exception {
        verifyDelete(helper.getNoneUser(), NONE_CLIENT_ID, 403);
    }

    private ConnectionEntity getRandomConnection(final NiFiTestUser user) throws Exception {
        final String url = helper.getBaseUrl() + "/flow/process-groups/root";

        // get the connections
        final ClientResponse response = user.testGet(url);

        // ensure the response was successful
        assertEquals(200, response.getStatus());

        // unmarshal
        final ProcessGroupFlowEntity flowEntity = response.getEntity(ProcessGroupFlowEntity.class);
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();
        final Set<ConnectionEntity> connections = flowDto.getConnections();

        // ensure the correct number of connection
        assertFalse(connections.isEmpty());

        // use the first connection as the target
        Iterator<ConnectionEntity> connectionIter = connections.iterator();
        assertTrue(connectionIter.hasNext());
        return connectionIter.next();
    }

    private ClientResponse updateConnection(final NiFiTestUser user, final ConnectionEntity entity) throws Exception {
        final String url = helper.getBaseUrl() + "/connections/" + entity.getId();

        // perform the request
        return user.testPut(url, entity);
    }

    private ConnectionEntity createConnection(final String name) throws Exception {
        String url = helper.getBaseUrl() + "/process-groups/root/connections";

        // get two processors
        final ProcessorEntity one = ITProcessorAccessControl.createProcessor(helper, "one");
        final ProcessorEntity two = ITProcessorAccessControl.createProcessor(helper, "two");

        // create the source connectable
        ConnectableDTO source = new ConnectableDTO();
        source.setId(one.getId());
        source.setGroupId(one.getComponent().getParentGroupId());
        source.setType(ConnectableType.PROCESSOR.name());

        // create the target connectable
        ConnectableDTO target = new ConnectableDTO();
        target.setId(two.getId());
        target.setGroupId(two.getComponent().getParentGroupId());
        target.setType(ConnectableType.PROCESSOR.name());

        // create the relationships
        Set<String> relationships = new HashSet<>();
        relationships.add("success");

        // create the connection
        ConnectionDTO connection = new ConnectionDTO();
        connection.setName(name);
        connection.setSource(source);
        connection.setDestination(target);
        connection.setSelectedRelationships(relationships);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(READ_WRITE_CLIENT_ID);
        revision.setVersion(0L);

        // create the entity body
        ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(revision);
        entity.setComponent(connection);

        // perform the request
        ClientResponse response = helper.getReadWriteUser().testPost(url, entity);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(ConnectionEntity.class);

        // verify creation
        connection = entity.getComponent();
        assertEquals(name, connection.getName());

        // get the connection
        return entity;
    }

    private void verifyDelete(final NiFiTestUser user, final String clientId, final int responseCode) throws Exception {
        final ConnectionEntity entity = createConnection("Copy");

        // create the entity body
        final Map<String, String> queryParams = new HashMap<>();
        queryParams.put("version", String.valueOf(entity.getRevision().getVersion()));
        queryParams.put("clientId", clientId);

        // perform the request
        ClientResponse response = user.testDelete(entity.getUri(), queryParams);

        // ensure the request is failed with a forbidden status code
        assertEquals(responseCode, response.getStatus());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        helper.cleanup();
    }
}
