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
import org.apache.nifi.integration.util.SourceTestProcessor;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
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
 * Access control test for processors.
 */
public class ITProcessorAccessControl {

    private static AccessControlHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AccessControlHelper();
    }

    /**
     * Ensures the READ user can get a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserGetProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the READ WRITE user can get a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserGetProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the WRITE user can get a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserGetProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the NONE user can get a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserGetProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getNoneUser());
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
    public void testReadUserPutProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        // attempt update the name
        entity.getRevision().setClientId(READ_CLIENT_ID);
        entity.getComponent().setName("Updated Name");

        // perform the request
        final ClientResponse response = updateProcessor(helper.getReadUser(), entity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ_WRITE user can put a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String updatedName = "Updated Name";

        // attempt to update the name
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(AccessControlHelper.READ_WRITE_CLIENT_ID);
        entity.getComponent().setName(updatedName);

        // perform the request
        final ClientResponse response = updateProcessor(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ProcessorEntity responseEntity = response.getEntity(ProcessorEntity.class);

        // verify
        assertEquals(READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(updatedName, responseEntity.getComponent().getName());
    }

    /**
     * Ensures the READ_WRITE user can put a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutProcessorThroughInheritedPolicy() throws Exception {
        final ProcessorEntity entity = createProcessor(helper, NiFiTestAuthorizer.NO_POLICY_COMPONENT_NAME);

        final String updatedName = "Updated name";

        // attempt to update the name
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(READ_WRITE_CLIENT_ID);
        entity.getComponent().setName(updatedName);

        // perform the request
        final ClientResponse response = updateProcessor(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ProcessorEntity responseEntity = response.getEntity(ProcessorEntity.class);

        // verify
        assertEquals(AccessControlHelper.READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(updatedName, responseEntity.getComponent().getName());
    }

    /**
     * Ensures the WRITE user can put a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserPutProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final String updatedName = "Updated Name";

        // attempt to update the name
        final ProcessorDTO requestDto = new ProcessorDTO();
        requestDto.setId(entity.getId());
        requestDto.setName(updatedName);

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.WRITE_CLIENT_ID);

        final ProcessorEntity requestEntity = new ProcessorEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateProcessor(helper.getWriteUser(), requestEntity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final ProcessorEntity responseEntity = response.getEntity(ProcessorEntity.class);

        // verify
        assertEquals(WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
    }

    /**
     * Ensures the NONE user cannot put a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserPutProcessor() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final String updatedName = "Updated Name";

        // attempt to update the name
        final ProcessorDTO requestDto = new ProcessorDTO();
        requestDto.setId(entity.getId());
        requestDto.setName(updatedName);

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.NONE_CLIENT_ID);

        final ProcessorEntity requestEntity = new ProcessorEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateProcessor(helper.getNoneUser(), requestEntity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ user cannot clear state.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserClearState() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String url = helper.getBaseUrl() + "/processors/" + entity.getId() + "/state/clear-requests";

        // perform the request
        final ClientResponse response = helper.getReadUser().testPost(url);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the NONE user cannot clear state.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserClearState() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String url = helper.getBaseUrl() + "/processors/" + entity.getId() + "/state/clear-requests";

        // perform the request
        final ClientResponse response = helper.getNoneUser().testPost(url);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ WRITE user can clear state.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserClearState() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String url = helper.getBaseUrl() + "/processors/" + entity.getId() + "/state/clear-requests";

        // perform the request
        final ClientResponse response = helper.getReadWriteUser().testPost(url);

        // ensure ok response
        assertEquals(200, response.getStatus());
    }

    /**
     * Ensures the WRITE user can clear state.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserClearState() throws Exception {
        final ProcessorEntity entity = getRandomProcessor(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String url = helper.getBaseUrl() + "/processors/" + entity.getId() + "/state/clear-requests";

        // perform the request
        final ClientResponse response = helper.getWriteUser().testPost(url);

        // ensure ok response
        assertEquals(200, response.getStatus());
    }

    /**
     * Ensures the READ user cannot delete a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserDeleteProcessor() throws Exception {
        verifyDelete(helper.getReadUser(), AccessControlHelper.READ_CLIENT_ID, 403);
    }

    /**
     * Ensures the READ WRITE user can delete a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserDeleteProcessor() throws Exception {
        verifyDelete(helper.getReadWriteUser(), AccessControlHelper.READ_WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the WRITE user can delete a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserDeleteProcessor() throws Exception {
        verifyDelete(helper.getWriteUser(), AccessControlHelper.WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the NONE user can delete a processor.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserDeleteProcessor() throws Exception {
        verifyDelete(helper.getNoneUser(), NONE_CLIENT_ID, 403);
    }

    private ProcessorEntity getRandomProcessor(final NiFiTestUser user) throws Exception {
        final String url = helper.getBaseUrl() + "/flow/process-groups/root";

        // get the processors
        final ClientResponse response = user.testGet(url);

        // ensure the response was successful
        assertEquals(200, response.getStatus());

        // unmarshal
        final ProcessGroupFlowEntity flowEntity = response.getEntity(ProcessGroupFlowEntity.class);
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();
        final Set<ProcessorEntity> processors = flowDto.getProcessors();

        // ensure the correct number of processors
        assertFalse(processors.isEmpty());

        // use the first processor as the target
        Iterator<ProcessorEntity> processorIter = processors.iterator();
        assertTrue(processorIter.hasNext());
        return processorIter.next();
    }

    private ClientResponse updateProcessor(final NiFiTestUser user, final ProcessorEntity entity) throws Exception {
        final String url = helper.getBaseUrl() + "/processors/" + entity.getId();

        // perform the request
        return user.testPut(url, entity);
    }

    public static ProcessorEntity createProcessor(final AccessControlHelper ach, final String name) throws Exception {
        String url = ach.getBaseUrl() + "/process-groups/root/processors";

        // create the processor
        ProcessorDTO processor = new ProcessorDTO();
        processor.setName(name);
        processor.setType(SourceTestProcessor.class.getName());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(READ_WRITE_CLIENT_ID);
        revision.setVersion(0L);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request
        ClientResponse response = ach.getReadWriteUser().testPost(url, entity);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(ProcessorEntity.class);

        // verify creation
        processor = entity.getComponent();
        assertEquals(name, processor.getName());
        assertEquals("org.apache.nifi.integration.util.SourceTestProcessor", processor.getType());

        // get the processor
        return entity;
    }

    private void verifyDelete(final NiFiTestUser user, final String clientId, final int responseCode) throws Exception {
        final ProcessorEntity entity = createProcessor(helper, "Copy");

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
