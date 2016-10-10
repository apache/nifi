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
import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.FunnelEntity;
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
 * Access control test for funnels.
 */
public class ITFunnelAccessControl {

    private static AccessControlHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AccessControlHelper();
    }

    /**
     * Ensures the READ user can get a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserGetFunnel() throws Exception {
        final FunnelEntity entity = getRandomFunnel(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the READ WRITE user can get a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserGetFunnel() throws Exception {
        final FunnelEntity entity = getRandomFunnel(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());
    }

    /**
     * Ensures the WRITE user can get a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserGetFunnel() throws Exception {
        final FunnelEntity entity = getRandomFunnel(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the NONE user can get a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserGetFunnel() throws Exception {
        final FunnelEntity entity = getRandomFunnel(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());
    }

    /**
     * Ensures the READ user cannot put a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserPutFunnel() throws Exception {
        final FunnelEntity entity = getRandomFunnel(helper.getReadUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        // attempt update the position
        entity.getRevision().setClientId(READ_CLIENT_ID);
        entity.getComponent().setPosition(new PositionDTO(0.0, 10.0));

        // perform the request
        final ClientResponse response = updateFunnel(helper.getReadUser(), entity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ_WRITE user can put a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserPutFunnel() throws Exception {
        final FunnelEntity entity = getRandomFunnel(helper.getReadWriteUser());
        assertTrue(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNotNull(entity.getComponent());

        final double y = 15.0;

        // attempt to update the position
        final long version = entity.getRevision().getVersion();
        entity.getRevision().setClientId(AccessControlHelper.READ_WRITE_CLIENT_ID);
        entity.getComponent().setPosition(new PositionDTO(0.0, y));

        // perform the request
        final ClientResponse response = updateFunnel(helper.getReadWriteUser(), entity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final FunnelEntity responseEntity = response.getEntity(FunnelEntity.class);

        // verify
        assertEquals(READ_WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
        assertEquals(y, responseEntity.getComponent().getPosition().getY().doubleValue(), 0);
    }

    /**
     * Ensures the WRITE user can put a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserPutFunnel() throws Exception {
        final FunnelEntity entity = getRandomFunnel(helper.getWriteUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertTrue(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        final double y = 15.0;

        // attempt to update the position
        final FunnelDTO requestDto = new FunnelDTO();
        requestDto.setId(entity.getId());
        requestDto.setPosition(new PositionDTO(0.0, y));

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.WRITE_CLIENT_ID);

        final FunnelEntity requestEntity = new FunnelEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateFunnel(helper.getWriteUser(), requestEntity);

        // ensure successful response
        assertEquals(200, response.getStatus());

        // get the response
        final FunnelEntity responseEntity = response.getEntity(FunnelEntity.class);

        // verify
        assertEquals(WRITE_CLIENT_ID, responseEntity.getRevision().getClientId());
        assertEquals(version + 1, responseEntity.getRevision().getVersion().longValue());
    }

    /**
     * Ensures the NONE user cannot put a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserPutFunnel() throws Exception {
        final FunnelEntity entity = getRandomFunnel(helper.getNoneUser());
        assertFalse(entity.getPermissions().getCanRead());
        assertFalse(entity.getPermissions().getCanWrite());
        assertNull(entity.getComponent());

        // attempt to update the position
        final FunnelDTO requestDto = new FunnelDTO();
        requestDto.setId(entity.getId());
        requestDto.setPosition(new PositionDTO(0.0, 15.0));

        final long version = entity.getRevision().getVersion();
        final RevisionDTO requestRevision = new RevisionDTO();
        requestRevision.setVersion(version);
        requestRevision.setClientId(AccessControlHelper.NONE_CLIENT_ID);

        final FunnelEntity requestEntity = new FunnelEntity();
        requestEntity.setId(entity.getId());
        requestEntity.setRevision(requestRevision);
        requestEntity.setComponent(requestDto);

        // perform the request
        final ClientResponse response = updateFunnel(helper.getNoneUser(), requestEntity);

        // ensure forbidden response
        assertEquals(403, response.getStatus());
    }

    /**
     * Ensures the READ user cannot delete a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadUserDeleteFunnel() throws Exception {
        verifyDelete(helper.getReadUser(), AccessControlHelper.READ_CLIENT_ID, 403);
    }

    /**
     * Ensures the READ WRITE user can delete a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testReadWriteUserDeleteFunnel() throws Exception {
        verifyDelete(helper.getReadWriteUser(), AccessControlHelper.READ_WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the WRITE user can delete a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testWriteUserDeleteFunnel() throws Exception {
        verifyDelete(helper.getWriteUser(), AccessControlHelper.WRITE_CLIENT_ID, 200);
    }

    /**
     * Ensures the NONE user can delete a funnel.
     *
     * @throws Exception ex
     */
    @Test
    public void testNoneUserDeleteFunnel() throws Exception {
        verifyDelete(helper.getNoneUser(), NONE_CLIENT_ID, 403);
    }

    private FunnelEntity getRandomFunnel(final NiFiTestUser user) throws Exception {
        final String url = helper.getBaseUrl() + "/flow/process-groups/root";

        // get the flow
        final ClientResponse response = user.testGet(url);

        // ensure the response was successful
        assertEquals(200, response.getStatus());

        // unmarshal
        final ProcessGroupFlowEntity flowEntity = response.getEntity(ProcessGroupFlowEntity.class);
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();
        final Set<FunnelEntity> funnels = flowDto.getFunnels();

        // ensure the correct number of funnels
        assertFalse(funnels.isEmpty());

        // use the first funnel as the target
        Iterator<FunnelEntity> funnelIter = funnels.iterator();
        assertTrue(funnelIter.hasNext());
        return funnelIter.next();
    }

    private ClientResponse updateFunnel(final NiFiTestUser user, final FunnelEntity entity) throws Exception {
        final String url = helper.getBaseUrl() + "/funnels/" + entity.getId();

        // perform the request
        return user.testPut(url, entity);
    }

    private FunnelEntity createFunnel() throws Exception {
        String url = helper.getBaseUrl() + "/process-groups/root/funnels";

        // create the funnel
        FunnelDTO funnel = new FunnelDTO();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(READ_WRITE_CLIENT_ID);
        revision.setVersion(0L);

        // create the entity body
        FunnelEntity entity = new FunnelEntity();
        entity.setRevision(revision);
        entity.setComponent(funnel);

        // perform the request
        ClientResponse response = helper.getReadWriteUser().testPost(url, entity);

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        // get the entity body
        return response.getEntity(FunnelEntity.class);
    }

    private void verifyDelete(final NiFiTestUser user, final String clientId, final int responseCode) throws Exception {
        final FunnelEntity entity = createFunnel();

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
