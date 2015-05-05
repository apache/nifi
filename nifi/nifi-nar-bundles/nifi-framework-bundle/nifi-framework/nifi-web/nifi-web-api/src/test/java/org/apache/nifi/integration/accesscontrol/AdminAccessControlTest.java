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
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.integration.NiFiWebApiTest;
import org.apache.nifi.integration.util.NiFiTestServer;
import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.entity.AuthorityEntity;
import org.apache.nifi.web.api.entity.BannerEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionsEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.InputPortEntity;
import org.apache.nifi.web.api.entity.InputPortsEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.LabelsEntity;
import org.apache.nifi.web.api.entity.OutputPortEntity;
import org.apache.nifi.web.api.entity.OutputPortsEntity;
import org.apache.nifi.web.api.entity.PrioritizerTypesEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UsersEntity;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Access control test for the admin user.
 */
public class AdminAccessControlTest {

    public static final String ADMIN_USER_DN = "CN=Lastname Firstname Middlename admin, OU=Unknown, OU=Unknown, OU=Unknown, O=Unknown, C=Unknown";

    private static final String CLIENT_ID = "admin-client-id";
    private static final String CONTEXT_PATH = "/nifi-api";
    private static final String FLOW_XML_PATH = "target/test-classes/access-control/flow-admin.xml";

    private static NiFiTestServer SERVER;
    private static NiFiTestUser ADMIN_USER;
    private static String BASE_URL;

    @BeforeClass
    public static void setup() throws Exception {
        // configure the location of the nifi properties
        File nifiPropertiesFile = new File("src/test/resources/access-control/nifi.properties");
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, nifiPropertiesFile.getAbsolutePath());

        // update the flow.xml property
        NiFiProperties props = NiFiProperties.getInstance();
        props.setProperty("nifi.flow.configuration.file", FLOW_XML_PATH);

        // load extensions
        NarClassLoaders.load(props);
        ExtensionManager.discoverExtensions();

        // start the server
        SERVER = new NiFiTestServer("src/main/webapp", CONTEXT_PATH);
        SERVER.startServer();
        SERVER.loadFlow();

        // get the base url
        BASE_URL = SERVER.getBaseUrl() + CONTEXT_PATH;

        // create the user
        ADMIN_USER = new NiFiTestUser(SERVER.getClient(), ADMIN_USER_DN);

        // populate the flow
        NiFiWebApiTest.populateFlow(SERVER.getClient(), BASE_URL, CLIENT_ID);
    }

    // ----------------------------------------------
    // PROCESS GROUPS
    // ----------------------------------------------
    /**
     * Ensures the admin user can get a groups content.
     *
     * @throws Exception ex
     */
    @Test
    public void testGroupGet() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root";

        // build the query params
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("verbose", Boolean.TRUE.toString());

        ClientResponse response = ADMIN_USER.testGet(url, queryParams);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // extract the process group
        ProcessGroupEntity processGroupEntity = response.getEntity(ProcessGroupEntity.class);

        // ensure there is content
        Assert.assertNotNull(processGroupEntity);

        // extract the process group dto
        ProcessGroupDTO processGroupDTO = processGroupEntity.getProcessGroup();
        FlowSnippetDTO processGroupContentsDTO = processGroupDTO.getContents();

        // verify graph
        Assert.assertEquals(2, processGroupContentsDTO.getProcessors().size());
        Assert.assertEquals(1, processGroupContentsDTO.getConnections().size());
        Assert.assertEquals(1, processGroupContentsDTO.getProcessGroups().size());
        Assert.assertEquals(1, processGroupContentsDTO.getInputPorts().size());
        Assert.assertEquals(1, processGroupContentsDTO.getOutputPorts().size());
        Assert.assertEquals(1, processGroupContentsDTO.getLabels().size());
    }

    /**
     * Verifies the admin user cannot update a group.
     *
     * @throws Exception ex
     */
    @Test
    public void testGroupPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root";

        // create the entity body
        Map<String, String> formData = new HashMap<>();
        formData.put("revision", String.valueOf(NiFiTestUser.REVISION));
        formData.put("clientId", CLIENT_ID);

        // perform the request
        ClientResponse response = ADMIN_USER.testPut(url, formData);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    // ----------------------------------------------
    // CONTROLLER
    // ----------------------------------------------
    /**
     * Verifies the admin user can retrieve the controller configuration.
     *
     * @throws Exception ex
     */
    @Test
    public void testControllerConfigurationGet() throws Exception {
        String url = BASE_URL + "/controller/config";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // verify the results
        ControllerConfigurationEntity entity = response.getEntity(ControllerConfigurationEntity.class);
        Assert.assertNotNull(entity);
        Assert.assertNotNull(entity.getConfig());
        Assert.assertEquals("NiFi Flow", entity.getConfig().getName());
        Assert.assertEquals(10, entity.getConfig().getMaxTimerDrivenThreadCount().intValue());
        Assert.assertEquals(5, entity.getConfig().getMaxEventDrivenThreadCount().intValue());
        Assert.assertEquals(30, entity.getConfig().getAutoRefreshIntervalSeconds().intValue());
    }

    /**
     * Verifies the admin user cannot update the controller configuration.
     *
     * @throws Exception ex
     */
    @Test
    public void testControllerConfigurationPut() throws Exception {
        String url = BASE_URL + "/controller/config";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setVersion(0L);

        // create the entity body
        ControllerConfigurationEntity entity = new ControllerConfigurationEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPut(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies the read only user cannot create a new flow archive.
     *
     * @throws Exception ex
     */
    @Test
    public void testFlowConfigurationArchivePost() throws Exception {
        String url = BASE_URL + "/controller/archive";

        // create the entity body
        Map<String, String> formData = new HashMap<>();
        formData.put("revision", String.valueOf(NiFiTestUser.REVISION));
        formData.put("clientId", CLIENT_ID);

        // perform the request
        ClientResponse response = ADMIN_USER.testPost(url, formData);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies the admin user can retrieve his credentials.
     *
     * @throws Exception ex
     */
    @Test
    public void testAuthoritiesGet() throws Exception {
        String url = BASE_URL + "/controller/authorities";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // verify the result
        AuthorityEntity entity = response.getEntity(AuthorityEntity.class);
        Assert.assertNotNull(entity);
        Assert.assertNotNull(entity.getAuthorities());
        Assert.assertEquals(1, entity.getAuthorities().size());
        Assert.assertEquals("ROLE_ADMIN", entity.getAuthorities().toArray()[0]);
    }

    /**
     * Verifies the admin user can retrieve the banners.
     *
     * @throws Exception ex
     */
    @Test
    public void testBannersGet() throws Exception {
        String url = BASE_URL + "/controller/banners";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // verify the result
        BannerEntity entity = response.getEntity(BannerEntity.class);
        Assert.assertNotNull(entity);
        Assert.assertNotNull(entity.getBanners());
        Assert.assertEquals("TEST BANNER", entity.getBanners().getHeaderText());
        Assert.assertEquals("TEST BANNER", entity.getBanners().getFooterText());
    }

    /**
     * Verifies the admin user can retrieve the processor types.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessorTypesGet() throws Exception {
        String url = BASE_URL + "/controller/processor-types";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // verify the result
        ProcessorTypesEntity entity = response.getEntity(ProcessorTypesEntity.class);
        Assert.assertNotNull(entity);
        Assert.assertNotNull(entity.getProcessorTypes());
        Assert.assertFalse(entity.getProcessorTypes().isEmpty());
    }

    /**
     * Verifies the admin user can retrieve the prioritizer types.
     *
     * @throws Exception ex
     */
    @Test
    public void testPrioritizerTypesGet() throws Exception {
        String url = BASE_URL + "/controller/prioritizers";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // verify the result
        PrioritizerTypesEntity entity = response.getEntity(PrioritizerTypesEntity.class);
        Assert.assertNotNull(entity);
        Assert.assertNotNull(entity.getPrioritizerTypes());
        Assert.assertFalse(entity.getPrioritizerTypes().isEmpty());
    }

    // ----------------------------------------------
    // PROCESS GROUP
    // ----------------------------------------------
    /**
     * Verifies that the admin user can get process groups.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessorGroupsGet() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/process-group-references";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // get the response
        ProcessGroupsEntity entity = response.getEntity(ProcessGroupsEntity.class);

        // ensure the request was successful
        Assert.assertEquals(200, response.getStatus());
        Assert.assertNotNull(entity.getProcessGroups());
        Assert.assertEquals(1, entity.getProcessGroups().size());
    }

    /**
     * Verifies that the operator user cannot create new process groups.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessGroupPost() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/process-group-references";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPost(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the operator user cannot update process group configuration.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessGroupPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/process-group-references/1";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPut(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the operator user cannot delete process groups.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessGroupDelete() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/process-group-references/1";

        // perform the request
        ClientResponse response = ADMIN_USER.testDelete(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    // ----------------------------------------------
    // PROCESSOR
    // ----------------------------------------------
    /**
     * Verifies that the admin user can get processors.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessorsGet() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/processors";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // get the response
        ProcessorsEntity entity = response.getEntity(ProcessorsEntity.class);

        // ensure the request was successful
        Assert.assertEquals(200, response.getStatus());
        Assert.assertNotNull(entity.getProcessors());
        Assert.assertEquals(2, entity.getProcessors().size());
    }

    /**
     * Verifies that the admin user cannot create new processors.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessorPost() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/processors";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPost(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the admin user cannot create new processors.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessorPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/processors/1";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPut(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the admin user cannot delete processors.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessorDelete() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/processors/1";

        // perform the request
        ClientResponse response = ADMIN_USER.testDelete(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    // ----------------------------------------------
    // CONNECTION
    // ----------------------------------------------
    /**
     * Verifies that the admin user can get connections.
     *
     * @throws Exception ex
     */
    @Test
    public void testConnectionsGet() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/connections";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // get the response
        ConnectionsEntity entity = response.getEntity(ConnectionsEntity.class);

        // ensure the request was successful
        Assert.assertEquals(200, response.getStatus());
        Assert.assertNotNull(entity.getConnections());
        Assert.assertEquals(1, entity.getConnections().size());
    }

    /**
     * Verifies that the admin user cannot create connections.
     *
     * @throws Exception ex
     */
    @Test
    public void testConnectionPost() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/connections";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPost(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the admin user cannot create connections.
     *
     * @throws Exception ex
     */
    @Test
    public void testConnectionPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/connections/1";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPut(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the admin user cannot delete connections.
     *
     * @throws Exception ex
     */
    @Test
    public void testConnectionDelete() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/connections/1";

        // perform the request
        ClientResponse response = ADMIN_USER.testDelete(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    // ----------------------------------------------
    // INPUT PORTS
    // ----------------------------------------------
    /**
     * Verifies that the admin user can get input ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testInputPortsGet() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/input-ports";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // get the response
        InputPortsEntity entity = response.getEntity(InputPortsEntity.class);

        // ensure the request was successful
        Assert.assertEquals(200, response.getStatus());
        Assert.assertNotNull(entity.getInputPorts());
        Assert.assertEquals(1, entity.getInputPorts().size());
    }

    /**
     * Verifies that the admin user cannot create input ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testInputPortPost() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/input-ports";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        InputPortEntity entity = new InputPortEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPost(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the admin user cannot create input ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testInputPortPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/input-ports/1";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        InputPortEntity entity = new InputPortEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPut(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the admin user cannot delete input ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testInputPortDelete() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/input-ports/1";

        // perform the request
        ClientResponse response = ADMIN_USER.testDelete(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    // ----------------------------------------------
    // OUTPUT PORTS
    // ----------------------------------------------
    /**
     * Verifies that the admin user can get output ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testOutputPortsGet() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/output-ports";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // get the response
        OutputPortsEntity entity = response.getEntity(OutputPortsEntity.class);

        // ensure the request was successful
        Assert.assertEquals(200, response.getStatus());
        Assert.assertNotNull(entity.getOutputPorts());
        Assert.assertEquals(1, entity.getOutputPorts().size());
    }

    /**
     * Verifies that the admin user cannot create output ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testOutputPortPost() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/output-ports";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        OutputPortEntity entity = new OutputPortEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPost(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the admin user cannot create input ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testOutputPortPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/output-ports/1";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        OutputPortEntity entity = new OutputPortEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPut(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the admin user cannot delete output ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testOutputPortDelete() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/output-ports/1";

        // perform the request
        ClientResponse response = ADMIN_USER.testDelete(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    // ----------------------------------------------
    // LABEL
    // ----------------------------------------------
    /**
     * Verifies that the admin user can get input ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testLabelsGet() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/labels";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // get the response
        LabelsEntity entity = response.getEntity(LabelsEntity.class);

        // ensure the request was successful
        Assert.assertEquals(200, response.getStatus());
        Assert.assertNotNull(entity.getLabels());
        Assert.assertEquals(1, entity.getLabels().size());
    }

    /**
     * Verifies that the admin user cannot create labels.
     *
     * @throws Exception ex
     */
    @Test
    public void testLabelPost() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/labels";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        LabelEntity entity = new LabelEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPost(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the admin user cannot create labels.
     *
     * @throws Exception ex
     */
    @Test
    public void testLabelPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/labels/1";

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        LabelEntity entity = new LabelEntity();
        entity.setRevision(revision);

        // perform the request
        ClientResponse response = ADMIN_USER.testPut(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies that the admin user cannot delete labels.
     *
     * @throws Exception ex
     */
    @Test
    public void testLabelDelete() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/labels/1";

        // perform the request
        ClientResponse response = ADMIN_USER.testDelete(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    // ----------------------------------------------
    // REMOTE PROCESS GROUP
    // ----------------------------------------------
    // ----------------------------------------------
    // HISTORY
    // ----------------------------------------------
    /**
     * Tests the ability to retrieve the NiFi history.
     *
     * @throws Exception ex
     */
    @Test
    public void testHistoryGet() throws Exception {
        String url = BASE_URL + "/controller/history";

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("offset", "1");
        queryParams.put("count", "1");

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url, queryParams);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    /**
     * Tests the ability to retrieve a specific action.
     *
     * @throws Exception ex
     */
    @Test
    public void testActionGet() throws Exception {
        int nonExistentActionId = 98775;
        String url = BASE_URL + "/controller/history/" + nonExistentActionId;

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(404, response.getStatus());
    }

    /**
     * Verifies the admin user can purge history.
     *
     * @throws Exception ex
     */
    @Test
    public void testHistoryDelete() throws Exception {
        String url = BASE_URL + "/controller/history/";

        // create the form data
        Map<String, String> formData = new HashMap<>();
        formData.put("endDate", "06/15/2011 13:45:50");

        // perform the request
        ClientResponse response = ADMIN_USER.testDelete(url, formData);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    // ----------------------------------------------
    // USER
    // ----------------------------------------------
    /**
     * Tests the ability to retrieve the NiFi users.
     *
     * @throws Exception ex
     */
    @Test
    public void testUsersGet() throws Exception {
        String url = BASE_URL + "/controller/users";

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    /**
     * Tests the ability to retrieve a specific user.
     *
     * @throws Exception ex
     */
    @Test
    public void testUserGet() throws Exception {
        int nonExistentActionId = 98775;
        String url = BASE_URL + "/controller/users/" + nonExistentActionId;

        // perform the request
        ClientResponse response = ADMIN_USER.testGet(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(404, response.getStatus());
    }

    /**
     * Verifies the admin user can update a person.
     *
     * @throws Exception ex
     */
    //@Test
    public void testUserUpdate() throws Exception {
        String url = BASE_URL + "/controller/users";

        // get all the users
        ClientResponse getResponse = ADMIN_USER.testGet(url);
        Assert.assertEquals(200, getResponse.getStatus());

        // get the response entity
        UsersEntity userCollectionEntity = getResponse.getEntity(UsersEntity.class);
        Assert.assertTrue(CollectionUtils.isNotEmpty(userCollectionEntity.getUsers()));

        // get the first user
        UserDTO userDTO = (UserDTO) userCollectionEntity.getUsers().toArray()[0];

        // create the form data
        Map<String, String> formData = new HashMap<>();
        formData.put("status", "ACTIVE");
        formData.put("authorities[]", "ROLE_MONITOR");

        // perform the request
        ClientResponse putResponse = ADMIN_USER.testPut(url + "/" + userDTO.getId(), formData);

        // ensure the request succeeded
        Assert.assertEquals(200, putResponse.getStatus());
        Assert.assertEquals("ACTIVE", putResponse.getEntity(UserEntity.class).getUser().getStatus());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        // shutdown the server
        SERVER.shutdownServer();
        SERVER = null;

        // look for the flow.xml
        File flow = new File(FLOW_XML_PATH);
        if (flow.exists()) {
            flow.delete();
        }
    }
}
