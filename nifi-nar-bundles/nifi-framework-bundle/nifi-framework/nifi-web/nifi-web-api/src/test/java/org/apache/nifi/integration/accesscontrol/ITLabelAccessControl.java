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
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.LabelEntity;
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
 * Access control test for labels.
 */
public class ITLabelAccessControl {

    private static AccessControlHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AccessControlHelper();
    }

    /**
     * Ensures the READ user can get a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserGetLabel() throws Exception {
        final LabelEntity entity = getRandomLabel(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the READ WRITE user can get a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserGetLabel() throws Exception {
        final LabelEntity entity = getRandomLabel(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the WRITE user can get a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserGetLabel() throws Exception {
        final LabelEntity entity = getRandomLabel(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the NONE user can get a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserGetLabel() throws Exception {
        final LabelEntity entity = getRandomLabel(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the READ user cannot put a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserPutLabel() throws Exception {
        final LabelEntity entity = getRandomLabel(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        // attempt update the name
        entity.getRevision().setClientId(READ_CLIENT_ID);
        entity.getComponent().setLabel("Updated Label");

        // perform the request
        final ClientResponse response = updateLabel(helper.getReadUser(), entity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ_WRITE user can put a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutLabel() throws Exception {
        final LabelEntity entity = getRandomLabel(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final String updatedLabel = "Updated Name";

        // attempt to update the name
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(AccessControlHelper.READ_WRITE_CLIENT_ID);
        entity.getComponent().setLabel(updatedLabel);

        // perform the request
        final ClientResponse response = updateLabel(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final LabelEntity responseEntity = response.getEntity(LabelEntity.class);

        // verify
        assertEquals(READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(updatedLabel, responseEntity.getComponent().getLabel());
    }

    /**
     * Ensures the READ_WRITE user can put a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutLabelThroughInheritedPolicy() throws Exception {
        final LabelEntity entity = createLabel(NiFiTestAuthorizer.NO_POLICY_COMPONENT_NAME);

        final String updatedLabel = "Updated name";

        // attempt to update the name
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(READ_WRITE_CLIENT_ID);
        entity.getComponent().setLabel(updatedLabel);

        // perform the request
        final ClientResponse response = updateLabel(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final LabelEntity responseEntity = response.getEntity(LabelEntity.class);

        // verify
        assertEquals(AccessControlHelper.READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(updatedLabel, responseEntity.getComponent().getLabel());
    }

    /**
     * Ensures the WRITE user can put a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserPutLabel() throws Exception {
        final LabelEntity entity = getRandomLabel(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final String updatedLabel = "Updated Name";

        // attempt to update the label
        final LabelDTO requestDto = new LabelDTO();
        requestDto.setId(entity.getId());
        requestDto.setLabel(updatedLabel);

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.WRITE_CLIENT_ID);

        final LabelEntity requestEntity = new LabelEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateLabel(helper.getWriteUser(), requestEntity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final LabelEntity responseEntity = response.getEntity(LabelEntity.class);

        // verify
        assertEquals(WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
    }

    /**
     * Ensures the NONE user cannot put a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserPutLabel() throws Exception {
        final LabelEntity entity = getRandomLabel(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final String updatedName = "Updated Name";

        // attempt to update the name
        final LabelDTO requestDto = new LabelDTO();
        requestDto.setId(entity.getId());
        requestDto.setLabel(updatedName);

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.NONE_CLIENT_ID);

        final LabelEntity requestEntity = new LabelEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateLabel(helper.getNoneUser(), requestEntity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ user cannot delete a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserDeleteLabel() throws Exception {
        verifyDelete(helper.getReadUser(), AccessControlHelper.READ_CLIENT_ID, 403);
    }

    /**
     * Ensures the READ WRITE user can delete a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserDeleteLabel() throws Exception {
        verifyDelete(helper.getReadWriteUser(), AccessControlHelper.READ_WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the WRITE user can delete a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserDeleteLabel() throws Exception {
        verifyDelete(helper.getWriteUser(), AccessControlHelper.WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the NONE user can delete a label.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserDeleteLabel() throws Exception {
        verifyDelete(helper.getNoneUser(), NONE_CLIENT_ID, 403);
    }

    private LabelEntity getRandomLabel(final NiFiTestUser user) throws Exception {
        final String url = helper.getBaseUrl() + "/flow/process-groups/root";

        // get the labels
        final ClientResponse response = user.testGet(url);

        // ensure the response was successful
        assertEquals(200, response.getStatus());

        // unmarshal
        final ProcessGroupFlowEntity flowEntity = response.getEntity(ProcessGroupFlowEntity.class);
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();
        final Set<LabelEntity> labels = flowDto.getLabels();

        // ensure the correct number of labels
        assertFalse(labels.isEmpty());

        // use the first label as the target
        Iterator<LabelEntity> labelIter = labels.iterator();
        assertTrue(labelIter.hasNext());
        return labelIter.next();
    }

    private ClientResponse updateLabel(final NiFiTestUser user, final LabelEntity entity) throws Exception {
        final String url = helper.getBaseUrl() + "/labels/" + entity.getId();

        // perform the request
        return user.testPut(url, entity);
    }

    private LabelEntity createLabel(final String name) throws Exception {
        String url = helper.getBaseUrl() + "/process-groups/root/labels";

        // create the label
        LabelDTO label = new LabelDTO();
        label.setLabel(name);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(READ_WRITE_CLIENT_ID);
        revision.setVersion(0L);

        // create the entity body
        LabelEntity entity = new LabelEntity();
        entity.setRevision(revision);
        entity.setComponent(label);

        // perform the request
        ClientResponse response = helper.getReadWriteUser().testPost(url, entity);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(LabelEntity.class);

        // verify creation
        label = entity.getComponent();
        assertEquals(name, label.getLabel());

        // get the label id
        return entity;
    }

    private void verifyDelete(final NiFiTestUser user, final String clientId, final int responseCode) throws Exception {
        final LabelEntity entity = createLabel("Copy");

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
