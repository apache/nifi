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
import org.apache.nifi.integration.util.NiFiTestAuthorizer;
import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
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
 * Access control test for process groups.
 */
public class ITProcessGroupAccessControl {

    private static AccessControlHelper helper;
    private static int count = 0;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AccessControlHelper();
    }

    /**
     * Ensures the READ user can get a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserGetProcessGroup() throws Exception {
        final ProcessGroupEntity entity = getRandomProcessGroup(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the READ WRITE user can get a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserGetProcessGroup() throws Exception {
        final ProcessGroupEntity entity = getRandomProcessGroup(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the WRITE user can get a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserGetProcessGroup() throws Exception {
        final ProcessGroupEntity entity = getRandomProcessGroup(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the NONE user can get a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserGetProcessGroup() throws Exception {
        final ProcessGroupEntity entity = getRandomProcessGroup(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the READ user cannot put a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserPutProcessGroup() throws Exception {
        final ProcessGroupEntity entity = getRandomProcessGroup(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        // attempt update the name
        entity.getRevision().setClientId(READ_CLIENT_ID);
        entity.getComponent().setName("Updated Name" + count++);

        // perform the request
        final ClientResponse response = updateProcessGroup(helper.getReadUser(), entity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ_WRITE user can put a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutProcessGroup() throws Exception {
        final ProcessGroupEntity entity = getRandomProcessGroup(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String updatedName = "Updated Name" + count++;

        // attempt to update the name
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(AccessControlHelper.READ_WRITE_CLIENT_ID);
        entity.getComponent().setName(updatedName);

        // perform the request
        final ClientResponse response = updateProcessGroup(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ProcessGroupEntity responseEntity = response.getEntity(ProcessGroupEntity.class);

        // verify
        assertEquals(READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(updatedName, responseEntity.getComponent().getName());
    }

    /**
     * Ensures the READ_WRITE user can put a process grup.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutProcessGroupThroughInheritedPolicy() throws Exception {
        final ProcessGroupEntity entity = createProcessGroup(NiFiTestAuthorizer.NO_POLICY_COMPONENT_NAME);

        final String updatedName = "Updated name" + count++;

        // attempt to update the name
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(READ_WRITE_CLIENT_ID);
        entity.getComponent().setName(updatedName);

        // perform the request
        final ClientResponse response = updateProcessGroup(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ProcessGroupEntity responseEntity = response.getEntity(ProcessGroupEntity.class);

        // verify
        assertEquals(AccessControlHelper.READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(updatedName, responseEntity.getComponent().getName());
    }

    /**
     * Ensures the WRITE user can put a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserPutProcessGroup() throws Exception {
        final ProcessGroupEntity entity = getRandomProcessGroup(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final String updatedName = "Updated Name" + count++;

        // attempt to update the name
        final ProcessGroupDTO requestDto = new ProcessGroupDTO();
        requestDto.setId(entity.getId());
        requestDto.setName(updatedName);

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.WRITE_CLIENT_ID);

        final ProcessGroupEntity requestEntity = new ProcessGroupEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateProcessGroup(helper.getWriteUser(), requestEntity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ProcessGroupEntity responseEntity = response.getEntity(ProcessGroupEntity.class);

        // verify
        assertEquals(WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
    }

    /**
     * Ensures the NONE user cannot put a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserPutProcessGroup() throws Exception {
        final ProcessGroupEntity entity = getRandomProcessGroup(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final String updatedName = "Updated Name" + count++;

        // attempt to update the name
        final ProcessGroupDTO requestDto = new ProcessGroupDTO();
        requestDto.setId(entity.getId());
        requestDto.setName(updatedName);

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.NONE_CLIENT_ID);

        final ProcessGroupEntity requestEntity = new ProcessGroupEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateProcessGroup(helper.getNoneUser(), requestEntity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ user cannot delete a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserDeleteProcessGroup() throws Exception {
        verifyDelete(helper.getReadUser(), AccessControlHelper.READ_CLIENT_ID, 403);
    }

    /**
     * Ensures the READ WRITE user can delete a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserDeleteProcessGroup() throws Exception {
        verifyDelete(helper.getReadWriteUser(), AccessControlHelper.READ_WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the WRITE user can delete a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserDeleteProcessGroup() throws Exception {
        verifyDelete(helper.getWriteUser(), AccessControlHelper.WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the NONE user can delete a process group.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserDeleteProcessGroup() throws Exception {
        verifyDelete(helper.getNoneUser(), NONE_CLIENT_ID, 403);
    }

    private ProcessGroupEntity getRandomProcessGroup(final NiFiTestUser user) throws Exception {
        final String url = helper.getBaseUrl() + "/flow/process-groups/root";

        // get the process groups
        final ClientResponse response = user.testGet(url);

        // ensure the response was successful
        assertEquals(200, response.getStatus());

        // unmarshal
        final ProcessGroupFlowEntity flowEntity = response.getEntity(ProcessGroupFlowEntity.class);
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();
        final Set<ProcessGroupEntity> processGroups = flowDto.getProcessGroups();

        // ensure the correct number of process groups
        assertFalse(processGroups.isEmpty());

        // use the first process group as the target
        Iterator<ProcessGroupEntity> processGroupIter = processGroups.iterator();
        assertTrue(processGroupIter.hasNext());
        return processGroupIter.next();
    }

    private ClientResponse updateProcessGroup(final NiFiTestUser user, final ProcessGroupEntity entity) throws Exception {
        final String url = helper.getBaseUrl() + "/process-groups/" + entity.getId();

        // perform the request
        return user.testPut(url, entity);
    }

    private ProcessGroupEntity createProcessGroup(final String name) throws Exception {
        String url = helper.getBaseUrl() + "/process-groups/root/process-groups";

        final String updatedName = name + count++;

        // create the process group
        ProcessGroupDTO processor = new ProcessGroupDTO();
        processor.setName(updatedName);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(READ_WRITE_CLIENT_ID);
        revision.setVersion(0L);

        // create the entity body
        ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request
        ClientResponse response = helper.getReadWriteUser().testPost(url, entity);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(ProcessGroupEntity.class);

        // verify creation
        processor = entity.getComponent();
        assertEquals(updatedName, processor.getName());

        // get the processor
        return entity;
    }

    private void verifyDelete(final NiFiTestUser user, final String clientId, final int responseCode) throws Exception {
        final ProcessGroupEntity entity = createProcessGroup("Copy");

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
