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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Access control test for funnels.
 */
public class ITFlowAccessControl {

    private static AccessControlHelper helper;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AccessControlHelper("src/test/resources/access-control/nifi-flow.properties");
    }

    /**
     * Test get flow.
     *
     * @throws Exception exception
     */
    @Test
    public void testGetFlow() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/process-groups/root");
    }

    // TODO - test update flow

    /**
     * Test generate client.
     *
     * @throws Exception exception
     */
    @Test
    public void testGenerateClientId() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/client-id");
    }

    /**
     * Test get identity.
     *
     * @throws Exception exception
     */
    @Test
    public void testGetIdentity() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/current-user");
    }

    /**
     * Test get controller services.
     *
     * @throws Exception exception
     */
    @Test
    public void testGetControllerServices() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/controller/controller-services");
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/process-groups/root/controller-services");
    }

    /**
     * Test get reporting tasks.
     *
     * @throws Exception exception
     */
    @Test
    public void testGetReportingTasks() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/reporting-tasks");
    }

    /**
     * Test search.
     *
     * @throws Exception exception
     */
    @Test
    public void testSearch() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/search-results");
    }

    /**
     * Test status.
     *
     * @throws Exception exception
     */
    @Test
    public void testStatus() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/status");
    }

    /**
     * Test banners.
     *
     * @throws Exception exception
     */
    @Test
    public void testBanners() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/status");
    }

    /**
     * Test bulletin board.
     *
     * @throws Exception exception
     */
    @Test
    public void testBulletinBoard() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/bulletin-board");
    }

    /**
     * Test about.
     *
     * @throws Exception exception
     */
    @Test
    public void testAbout() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/about");
    }

    /**
     * Test get flow config.
     *
     * @throws Exception exception
     */
    @Test
    public void testGetFlowConfig() throws Exception {
        helper.testGenericGetUri(helper.getBaseUrl() + "/flow/config");
    }

    /**
     * Test get status.
     *
     * @throws Exception exception
     */
    @Test
    public void testGetStatus() throws Exception {
        testComponentSpecificGetUri(helper.getBaseUrl() + "/flow/processors/my-component/status");
        testComponentSpecificGetUri(helper.getBaseUrl() + "/flow/input-ports/my-component/status");
        testComponentSpecificGetUri(helper.getBaseUrl() + "/flow/output-ports/my-component/status");
        testComponentSpecificGetUri(helper.getBaseUrl() + "/flow/remote-process-groups/my-component/status");
        testComponentSpecificGetUri(helper.getBaseUrl() + "/flow/process-groups/my-component/status");
        testComponentSpecificGetUri(helper.getBaseUrl() + "/flow/connections/my-component/status");
    }

    /**
     * Test get status history.
     *
     * @throws Exception exception
     */
    @Test
    public void testGetStatusHistory() throws Exception {
        testComponentSpecificGetUri(helper.getBaseUrl() + "/flow/processors/my-component/status/history");
        testComponentSpecificGetUri(helper.getBaseUrl() + "/flow/remote-process-groups/my-component/status/history");
        testComponentSpecificGetUri(helper.getBaseUrl() + "/flow/process-groups/my-component/status/history");
        testComponentSpecificGetUri(helper.getBaseUrl() + "/flow/connections/my-component/status/history");
    }

    /**
     * Test get action.
     *
     * @throws Exception exception
     */
    @Test
    public void testGetAction() throws Exception {
        final String uri = helper.getBaseUrl() + "/flow/history/98766";

        ClientResponse response;

        // the action does not exist... should return 404

        // read
        response = helper.getReadUser().testGet(uri);
        assertEquals(404, response.getStatus());

        // read/write
        response = helper.getReadWriteUser().testGet(uri);
        assertEquals(404, response.getStatus());

        // no read access should return 403

        // write
        response = helper.getWriteUser().testGet(uri);
        assertEquals(403, response.getStatus());

        // none
        response = helper.getNoneUser().testGet(uri);
        assertEquals(403, response.getStatus());
    }

    /**
     * Test get action.
     *
     * @throws Exception exception
     */
    @Test
    public void testGetComponentHistory() throws Exception {
        final String uri = helper.getBaseUrl() + "/flow/history/components/my-component-id";

        // will succeed due to controller level access

        // read
        ClientResponse response = helper.getReadUser().testGet(uri);
        assertEquals(200, response.getStatus());

        // read/write
        response = helper.getReadWriteUser().testGet(uri);
        assertEquals(200, response.getStatus());

        // will be denied because component does not exist and no controller level access

        // write
        response = helper.getWriteUser().testGet(uri);
        assertEquals(403, response.getStatus());

        // none
        response = helper.getNoneUser().testGet(uri);
        assertEquals(403, response.getStatus());
    }

    public void testComponentSpecificGetUri(final String uri) throws Exception {
        ClientResponse response;

        // read
        response = helper.getReadUser().testGet(uri);
        assertEquals(404, response.getStatus());

        // read/write
        response = helper.getReadWriteUser().testGet(uri);
        assertEquals(404, response.getStatus());

        // write
        response = helper.getWriteUser().testGet(uri);
        assertEquals(403, response.getStatus());

        // none
        response = helper.getNoneUser().testGet(uri);
        assertEquals(403, response.getStatus());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        helper.cleanup();
    }
}
