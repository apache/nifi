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
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.integration.NiFiWebApiTest;
import org.apache.nifi.integration.util.NiFiTestServer;
import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.integration.util.SourceTestProcessor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.entity.AuthorityEntity;
import org.apache.nifi.web.api.entity.BannerEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionsEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.InputPortsEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.LabelsEntity;
import org.apache.nifi.web.api.entity.OutputPortsEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.PrioritizerTypesEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;
import org.apache.nifi.web.api.entity.TemplateEntity;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Access control test for the dfm user.
 */
@Ignore
public class DfmAccessControlTest {

    public static final String DFM_USER_DN = "CN=Lastname Firstname Middlename dfm, OU=Unknown, OU=Unknown, OU=Unknown, O=Unknown, C=Unknown";

    private static final String CLIENT_ID = "dfm-client-id";
    private static final String CONTEXT_PATH = "/nifi-api";
    private static final String FLOW_XML_PATH = "target/test-classes/access-control/flow-dfm.xml";
    private static final String FLOW_TEMPLATES_PATH = "target/test-classes/access-control/templates";

    private static NiFiTestServer SERVER;
    private static NiFiTestUser DFM_USER;
    private static String BASE_URL;

    @BeforeClass
    public static void setup() throws Exception {
        // look for the flow.xml and toss it
        File flow = new File(FLOW_XML_PATH);
        if (flow.exists()) {
            flow.delete();
        }
        // look for templates and toss them
        final Path templatePath = Paths.get(FLOW_TEMPLATES_PATH);
        if (Files.exists(templatePath)) {
            final DirectoryStream<Path> dirStream = Files.newDirectoryStream(Paths.get(FLOW_TEMPLATES_PATH), "*.template");
            for (final Path path : dirStream) {
                Files.delete(path);
            }
        }

        // configure the location of the nifi properties
        File nifiPropertiesFile = new File("src/test/resources/access-control/nifi.properties");
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, nifiPropertiesFile.getAbsolutePath());

        // update the flow.xml property
        NiFiProperties props = NiFiProperties.getInstance();
        props.setProperty(NiFiProperties.FLOW_CONFIGURATION_FILE, FLOW_XML_PATH);

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
        DFM_USER = new NiFiTestUser(SERVER.getClient(), DFM_USER_DN);

        // populate the flow
        NiFiWebApiTest.populateFlow(SERVER.getClient(), BASE_URL, CLIENT_ID);
    }

    // ----------------------------------------------
    // PROCESS GROUPS
    // ----------------------------------------------
    @Test
    public void testGroupGet() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root";

        // build the query params
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("verbose", Boolean.TRUE.toString());

        // perform the request
        ClientResponse response = DFM_USER.testGet(url, queryParams);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // extract the process group
        ProcessGroupEntity processGroupEntity = response.getEntity(ProcessGroupEntity.class);

        // ensure there is content
        Assert.assertNotNull(processGroupEntity);

        // extract the process group dto
        ProcessGroupDTO processGroupDTO = processGroupEntity.getComponent();
        FlowSnippetDTO processGroupContentsDTO = processGroupDTO.getContents();

        // verify graph
        Assert.assertEquals(2, processGroupContentsDTO.getProcessors().size());
        Assert.assertEquals(1, processGroupContentsDTO.getConnections().size());
        Assert.assertEquals(1, processGroupContentsDTO.getProcessGroups().size());
        Assert.assertEquals(1, processGroupContentsDTO.getInputPorts().size());
        Assert.assertEquals(1, processGroupContentsDTO.getOutputPorts().size());
        Assert.assertEquals(1, processGroupContentsDTO.getLabels().size());
//        Assert.assertEquals(1, processGroupContentsDTO.getRemoteProcessGroups().size());
    }

    /**
     * Verifies the dfm user can update a group.
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
        ClientResponse response = DFM_USER.testPut(url, formData);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    // ----------------------------------------------
    // CONTROLLER
    // ----------------------------------------------
    /**
     * Verifies the dfm user can retrieve the controller configuration.
     *
     * @throws Exception ex
     */
    @Test
    public void testControllerConfiguration() throws Exception {
        String url = BASE_URL + "/controller/config";

        // create the controller configuration
        ControllerConfigurationDTO controllerConfig = new ControllerConfigurationDTO();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ControllerConfigurationEntity entity = new ControllerConfigurationEntity();
        entity.setRevision(revision);
        entity.setConfig(controllerConfig);

        // update the config
        ClientResponse response = DFM_USER.testPut(url, entity);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());

        // verify the results
        entity = response.getEntity(ControllerConfigurationEntity.class);
        Assert.assertNotNull(entity);
        Assert.assertNotNull(entity.getConfig());
        Assert.assertEquals(10, entity.getConfig().getMaxTimerDrivenThreadCount().intValue());
        Assert.assertEquals(5, entity.getConfig().getMaxEventDrivenThreadCount().intValue());
        Assert.assertEquals(30, entity.getConfig().getAutoRefreshIntervalSeconds().intValue());
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
        ClientResponse response = DFM_USER.testPost(url, formData);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(201, response.getStatus());
    }

    /**
     * Verifies the dfm user can retrieve his credentials.
     *
     * @throws Exception ex
     */
    @Test
    public void testAuthoritiesGet() throws Exception {
        String url = BASE_URL + "/controller/authorities";

        // perform the request
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // verify the result
        AuthorityEntity entity = response.getEntity(AuthorityEntity.class);
        Assert.assertNotNull(entity);
        Assert.assertNotNull(entity.getAuthorities());
        Assert.assertEquals(1, entity.getAuthorities().size());
        Assert.assertEquals("ROLE_DFM", entity.getAuthorities().toArray()[0]);
    }

    /**
     * Verifies the dfm user can retrieve the banners.
     *
     * @throws Exception ex
     */
    @Test
    public void testBannersGet() throws Exception {
        String url = BASE_URL + "/controller/banners";

        // perform the request
        ClientResponse response = DFM_USER.testGet(url);

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
     * Verifies the dfm user can retrieve the processor types.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessorTypesGet() throws Exception {
        String url = BASE_URL + "/controller/processor-types";

        // perform the request
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // verify the result
        ProcessorTypesEntity entity = response.getEntity(ProcessorTypesEntity.class);
        Assert.assertNotNull(entity);
        Assert.assertNotNull(entity.getProcessorTypes());
        Assert.assertFalse(entity.getProcessorTypes().isEmpty());
    }

    /**
     * Verifies the dfm user can retrieve the prioritizer types.
     *
     * @throws Exception ex
     */
    @Test
    @Ignore
    public void testPrioritizerTypesGet() throws Exception {
        String url = BASE_URL + "/controller/prioritizers";

        // perform the request
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the request is successful
        Assert.assertEquals(200, response.getStatus());

        // verify the result
        PrioritizerTypesEntity entity = response.getEntity(PrioritizerTypesEntity.class);
        Assert.assertNotNull(entity);
        Assert.assertNotNull(entity.getPrioritizerTypes());
        Assert.assertFalse(entity.getPrioritizerTypes().isEmpty());
    }

    // ----------------------------------------------
    // GET and PUT
    // ----------------------------------------------
    /**
     * Verifies that the dfm user can update a process group state.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessGroupPutState() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/process-group-references";

        // get a processor and update its configuration state
        ProcessGroupDTO processGroup = getRandomProcessGroup();

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);
        entity.setComponent(processGroup);

        // perform the request
        ClientResponse response = DFM_USER.testPut(url + "/" + processGroup.getId(), entity);

        // ensure the request succeeded
        Assert.assertEquals(200, response.getStatus());
    }

    /**
     * Verifies that the dfm user can update a process group configuration.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessGroupPutConfiguration() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/process-group-references";

        // get a processor and update its configuration state
        ProcessGroupDTO processGroup = getRandomProcessGroup();
        processGroup.setName("new group name");

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);
        entity.setComponent(processGroup);

        // perform the request
        ClientResponse response = DFM_USER.testPut(url + "/" + processGroup.getId(), entity);

        // ensure the request succeeded
        Assert.assertEquals(200, response.getStatus());

        // get the result
        entity = response.getEntity(ProcessGroupEntity.class);
        Assert.assertNotNull(entity.getComponent());
        Assert.assertEquals("new group name", entity.getComponent().getName());
    }

    /**
     * Verifies that the dfm user can update processor state.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessorPutState() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/processors";

        // get a processor and update its configuration state
        ProcessorDTO processor = getRandomProcessor();
        processor.setState(ScheduledState.STOPPED.toString());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request
        ClientResponse response = DFM_USER.testPut(url + "/" + processor.getId(), entity);

        // ensure the request succeeded
        Assert.assertEquals(200, response.getStatus());
    }

    /**
     * Verifies that the dfm user can update processor state.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessorPutConfiguration() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/processors";

        // get a processor and update its configuration state
        ProcessorDTO processor = getRandomProcessor();
        processor.setName("new processor name");

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request
        ClientResponse response = DFM_USER.testPut(url + "/" + processor.getId(), entity);

        // ensure the request succeeded
        Assert.assertEquals(200, response.getStatus());

        // get the result
        entity = response.getEntity(ProcessorEntity.class);
        Assert.assertNotNull(entity.getComponent());
        Assert.assertEquals("new processor name", entity.getComponent().getName());
    }

    /**
     * Verifies that the dfm user can update connections.
     *
     * @throws Exception ex
     */
    @Test
    public void testConnectionPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/connections";

        // get a random connection
        ConnectionDTO connection = getRandomConnection();
        connection.setName("new name");

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(revision);
        entity.setComponent(connection);

        // perform the request
        ClientResponse response = DFM_USER.testPut(url + "/" + connection.getId(), entity);

        // ensure the request succeeded
        Assert.assertEquals(200, response.getStatus());
    }

    /**
     * Verifies that the dfm user can update labels.
     *
     * @throws Exception ex
     */
    @Test
    public void testLabelPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/labels";

        // get a random label
        LabelDTO label = getRandomLabel();
        label.setLabel("new label");

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        LabelEntity entity = new LabelEntity();
        entity.setRevision(revision);
        entity.setComponent(label);

        // perform the request
        ClientResponse response = DFM_USER.testPut(url + "/" + label.getId(), entity);

        // ensure the request succeeded
        Assert.assertEquals(200, response.getStatus());

        // get the result
        entity = response.getEntity(LabelEntity.class);
        Assert.assertNotNull(entity.getComponent());
        Assert.assertEquals("new label", entity.getComponent().getLabel());
    }

    /**
     * Verifies that the dfm user can update input ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testInputPortPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/input-ports";

        // get a random input port
        PortDTO inputPort = getRandomInputPort();
        inputPort.setName("new input port name");

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        PortEntity entity = new PortEntity();
        entity.setRevision(revision);
        entity.setComponent(inputPort);

        // perform the request
        ClientResponse response = DFM_USER.testPut(url + "/" + inputPort.getId(), entity);

        // ensure the request succeeded
        Assert.assertEquals(200, response.getStatus());

        // get the result
        entity = response.getEntity(PortEntity.class);
        Assert.assertNotNull(entity.getComponent());
        Assert.assertEquals("new input port name", entity.getComponent().getName());
    }

    /**
     * Verifies that the dfm user can update output ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testOutputPortPut() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/output-ports";

        // get a random output port
        PortDTO outputPort = getRandomOutputPort();
        outputPort.setName("new output port name");

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        PortEntity entity = new PortEntity();
        entity.setRevision(revision);
        entity.setComponent(outputPort);

        // perform the request
        ClientResponse response = DFM_USER.testPut(url + "/" + outputPort.getId(), entity);

        // ensure the request succeeded
        Assert.assertEquals(200, response.getStatus());

        // get the result
        entity = response.getEntity(PortEntity.class);
        Assert.assertNotNull(entity.getComponent());
        Assert.assertEquals("new output port name", entity.getComponent().getName());
    }

    // ----------------------------------------------
    // POST and DELETE
    // ----------------------------------------------
    /**
     * Verifies that the dfm user can create/delete processors.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessorCreateDelete() throws Exception {
        ProcessorDTO processor = createProcessor();
        deleteProcessor(processor.getId());
    }

    /**
     * Verifies that the dfm user can create/delete process groups.
     *
     * @throws Exception ex
     */
    @Test
    public void testProcessGroupCreateDelete() throws Exception {
        ProcessGroupDTO processGroup = createProcessGroup();
        deleteProcessGroup(processGroup.getId());
    }

    /**
     * Verifies that the dfm user can create/delete input ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testInputPortCreateDelete() throws Exception {
        PortDTO portDTO = createInputPort();
        deleteInputPort(portDTO.getId());
    }

    /**
     * Verifies that the dfm user can create/delete output ports.
     *
     * @throws Exception ex
     */
    @Test
    public void testOutputPortCreateDelete() throws Exception {
        PortDTO portDTO = createOutputPort();
        deleteOutputPort(portDTO.getId());
    }

    /**
     * Verifies that the dfm user can create/delete connections.
     *
     * @throws Exception ex
     */
    @Test
    public void testConnectionCreateDelete() throws Exception {
        ProcessorDTO target = getRandomProcessor();
        ProcessorDTO source = createProcessor();
        ConnectionDTO connection = createConnection(source.getId(), target.getId(), "success");
        deleteConnection(connection.getId());
        deleteProcessor(source.getId());
    }

    /**
     * Verifies that the dfm user can create/delete labels.
     *
     * @throws Exception ex
     */
    @Test
    public void testLabelCreateDelete() throws Exception {
        LabelDTO label = createLabel();
        deleteLabel(label.getId());
    }

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
        ClientResponse response = DFM_USER.testGet(url, queryParams);

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
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(404, response.getStatus());
    }

    /**
     * Verifies the dfm user can purge history.
     *
     * @throws Exception ex
     */
    @Test
    public void testHistoryDelete() throws Exception {
        String url = BASE_URL + "/controller/history/";

        // create the form data
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("endDate", "06/15/2011 13:45:50");

        // perform the request
        ClientResponse response = DFM_USER.testDelete(url, queryParams);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    // ----------------------------------------------
    // Create resource utility methods
    // ----------------------------------------------
    private ProcessGroupDTO getRandomProcessGroup() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/process-group-references";

        // get the process groups
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the response was successful
        Assert.assertEquals(200, response.getStatus());

        // get the process group dtos
        ProcessGroupsEntity processGroupEntity = response.getEntity(ProcessGroupsEntity.class);
        Collection<ProcessGroupEntity> processGroups = processGroupEntity.getProcessGroups();

        // ensure the correct number of processor groups
        Assert.assertFalse(processGroups.isEmpty());

        // use the first process group as the target
        Iterator<ProcessGroupEntity> processorIter = processGroups.iterator();
        Assert.assertTrue(processorIter.hasNext());
        return processorIter.next().getComponent();
    }

    private ProcessorDTO getRandomProcessor() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/processors";

        // get the processors
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the response was successful
        Assert.assertEquals(200, response.getStatus());

        // get the processor dtos
        ProcessorsEntity processorsEntity = response.getEntity(ProcessorsEntity.class);
        Collection<ProcessorEntity> processors = processorsEntity.getProcessors();

        // ensure the correct number of processors
        Assert.assertFalse(processors.isEmpty());

        // use the first processor as the target
        Iterator<ProcessorEntity> processorIter = processors.iterator();
        Assert.assertTrue(processorIter.hasNext());
        return processorIter.next().getComponent();
    }

    private ConnectionDTO getRandomConnection() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/connections";

        // get the connections
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the response was successful
        Assert.assertEquals(200, response.getStatus());

        // get the connection dtos
        ConnectionsEntity connectionEntity = response.getEntity(ConnectionsEntity.class);
        Collection<ConnectionEntity> connections = connectionEntity.getConnections();

        // ensure the correct number of connections
        Assert.assertFalse(connections.isEmpty());

        // use the first connection as the target
        Iterator<ConnectionEntity> connectionIter = connections.iterator();
        Assert.assertTrue(connectionIter.hasNext());
        return connectionIter.next().getComponent();
    }

    private LabelDTO getRandomLabel() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/labels";

        // get the labels
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the response was successful
        Assert.assertEquals(200, response.getStatus());

        // get the label dtos
        LabelsEntity labelEntity = response.getEntity(LabelsEntity.class);
        Collection<LabelEntity> labels = labelEntity.getLabels();

        // ensure the correct number of labels
        Assert.assertFalse(labels.isEmpty());

        // use the first label as the target
        Iterator<LabelEntity> labelIter = labels.iterator();
        Assert.assertTrue(labelIter.hasNext());
        return labelIter.next().getComponent();
    }

    private PortDTO getRandomInputPort() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/input-ports";

        // get the input ports
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the response was successful
        Assert.assertEquals(200, response.getStatus());

        // get the port dtos
        InputPortsEntity inputPortsEntity = response.getEntity(InputPortsEntity.class);
        Collection<PortEntity> inputPorts = inputPortsEntity.getInputPorts();

        // ensure the correct number of ports
        Assert.assertFalse(inputPorts.isEmpty());

        // use the first port as the target
        Iterator<PortEntity> inputPortsIter = inputPorts.iterator();
        Assert.assertTrue(inputPortsIter.hasNext());
        return inputPortsIter.next().getComponent();
    }

    private PortDTO getRandomOutputPort() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/output-ports";

        // get the output ports
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the response was successful
        Assert.assertEquals(200, response.getStatus());

        // get the port dtos
        OutputPortsEntity outputPortsEntity = response.getEntity(OutputPortsEntity.class);
        Collection<PortEntity> outputPorts = outputPortsEntity.getOutputPorts();

        // ensure the correct number of ports
        Assert.assertFalse(outputPorts.isEmpty());

        // use the first port as the target
        Iterator<PortEntity> inputPortsIter = outputPorts.iterator();
        Assert.assertTrue(inputPortsIter.hasNext());
        return inputPortsIter.next().getComponent();
    }

    // ----------------------------------------------
    // Create resource utility methods
    // ----------------------------------------------
    private ProcessGroupDTO createProcessGroup() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/process-group-references";

        // create the processor
        ProcessGroupDTO processGroup = new ProcessGroupDTO();
        processGroup.setName("Group1");

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ProcessGroupEntity entity = new ProcessGroupEntity();
        entity.setRevision(revision);
        entity.setComponent(processGroup);

        // perform the request
        ClientResponse response = DFM_USER.testPost(url, entity);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(ProcessGroupEntity.class);

        // verify creation
        processGroup = entity.getComponent();
        Assert.assertEquals("Group1", processGroup.getName());

        // get the process group
        return processGroup;
    }

    private PortDTO createInputPort() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/input-ports";

        // create the processor
        PortDTO portDTO = new PortDTO();
        portDTO.setName("Input Port");

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        PortEntity entity = new PortEntity();
        entity.setRevision(revision);
        entity.setComponent(portDTO);

        // perform the request
        ClientResponse response = DFM_USER.testPost(url, entity);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(PortEntity.class);

        // verify creation
        portDTO = entity.getComponent();
        Assert.assertEquals("Input Port", portDTO.getName());

        // get the process group
        return portDTO;
    }

    private PortDTO createOutputPort() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/output-ports";

        // create the processor
        PortDTO portDTO = new PortDTO();
        portDTO.setName("Output Port");

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        PortEntity entity = new PortEntity();
        entity.setRevision(revision);
        entity.setComponent(portDTO);

        // perform the request
        ClientResponse response = DFM_USER.testPost(url, entity);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(PortEntity.class);

        // verify creation
        portDTO = entity.getComponent();
        Assert.assertEquals("Output Port", portDTO.getName());

        // get the process group
        return portDTO;
    }

    private ProcessorDTO createProcessor() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/processors";

        // create the processor
        ProcessorDTO processor = new ProcessorDTO();
        processor.setName("Copy");
        processor.setType(SourceTestProcessor.class.getName());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ProcessorEntity entity = new ProcessorEntity();
        entity.setRevision(revision);
        entity.setComponent(processor);

        // perform the request
        ClientResponse response = DFM_USER.testPost(url, entity);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(ProcessorEntity.class);

        // verify creation
        processor = entity.getComponent();
        Assert.assertEquals("Copy", processor.getName());
        Assert.assertEquals("org.apache.nifi.integration.util.SourceTestProcessor", processor.getType());

        // get the processor id
        return processor;
    }

    private ConnectionDTO createConnection(String sourceId, String targetId, String relationship) throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/connections";

        // create the source connectable
        ConnectableDTO source = new ConnectableDTO();
        source.setId(sourceId);
        source.setType(ConnectableType.PROCESSOR.name());

        // create the target connectable
        ConnectableDTO target = new ConnectableDTO();
        target.setId(targetId);
        target.setType(ConnectableType.PROCESSOR.name());

        // create the relationships
        Set<String> relationships = new HashSet<>();
        relationships.add(relationship);

        // create the connection
        ConnectionDTO connection = new ConnectionDTO();
        connection.setSource(source);
        connection.setDestination(target);
        connection.setSelectedRelationships(relationships);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        ConnectionEntity entity = new ConnectionEntity();
        entity.setRevision(revision);
        entity.setComponent(connection);

        // perform the request
        ClientResponse response = DFM_USER.testPost(url, entity);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(ConnectionEntity.class);

        // verify the results
        connection = entity.getComponent();
        Assert.assertEquals(sourceId, connection.getSource().getId());
        Assert.assertEquals(targetId, connection.getDestination().getId());
        Assert.assertEquals(1, connection.getSelectedRelationships().size());
        Assert.assertEquals("success", connection.getSelectedRelationships().toArray()[0]);

        // get the connection id
        return connection;
    }

    public LabelDTO createLabel() throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/labels";

        // create the label
        LabelDTO label = new LabelDTO();
        label.setLabel("Label2");

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(CLIENT_ID);
        revision.setVersion(NiFiTestUser.REVISION);

        // create the entity body
        LabelEntity entity = new LabelEntity();
        entity.setRevision(revision);
        entity.setComponent(label);

        // perform the request
        ClientResponse response = DFM_USER.testPost(url, entity);

        // ensure the request is successful
        Assert.assertEquals(201, response.getStatus());

        // get the entity body
        entity = response.getEntity(LabelEntity.class);

        // verify the results
        label = entity.getComponent();
        Assert.assertEquals("Label2", label.getLabel());

        // get the label id
        return label;
    }

    // ----------------------------------------------
    // Delete resource utility methods
    // ----------------------------------------------
    private void deleteProcessGroup(String processGroupId) throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/process-group-references/" + processGroupId;

        // create the entity body
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("revision", String.valueOf(NiFiTestUser.REVISION));
        queryParams.put("clientId", CLIENT_ID);

        // perform the request
        ClientResponse response = DFM_USER.testDelete(url, queryParams);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    private void deleteInputPort(String portId) throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/input-ports/" + portId;

        // create the entity body
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("revision", String.valueOf(NiFiTestUser.REVISION));
        queryParams.put("clientId", CLIENT_ID);

        // perform the request
        ClientResponse response = DFM_USER.testDelete(url, queryParams);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    private void deleteOutputPort(String portId) throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/output-ports/" + portId;

        // create the entity body
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("revision", String.valueOf(NiFiTestUser.REVISION));
        queryParams.put("clientId", CLIENT_ID);

        // perform the request
        ClientResponse response = DFM_USER.testDelete(url, queryParams);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    private void deleteProcessor(String processorId) throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/processors/" + processorId;

        // create the entity body
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("revision", String.valueOf(NiFiTestUser.REVISION));
        queryParams.put("clientId", CLIENT_ID);

        // perform the request
        ClientResponse response = DFM_USER.testDelete(url, queryParams);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    private void deleteConnection(String connectionId) throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/connections/" + connectionId;

        // create the entity body
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("revision", String.valueOf(NiFiTestUser.REVISION));
        queryParams.put("clientId", CLIENT_ID);

        // perform the request
        ClientResponse response = DFM_USER.testDelete(url, queryParams);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    private void deleteLabel(String labelId) throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/labels/" + labelId;

        // create the entity body
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("revision", String.valueOf(NiFiTestUser.REVISION));
        queryParams.put("clientId", CLIENT_ID);

        // perform the request
        ClientResponse response = DFM_USER.testDelete(url, queryParams);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    private void deleteRemoteProcessGroup(String remoteProcessGroupId) throws Exception {
        String url = BASE_URL + "/controller/process-groups/root/remote-process-groups/" + remoteProcessGroupId;

        // create the entity body
        Map<String, String> formData = new HashMap<>();
        formData.put("revision", String.valueOf(NiFiTestUser.REVISION));
        formData.put("clientId", CLIENT_ID);

        // perform the request
        ClientResponse response = DFM_USER.testDelete(url, formData);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(200, response.getStatus());
    }

    // ----------------------------------------------
    // TEMPLATE
    // ----------------------------------------------
    @Test
    public void testImportTemplate() throws Exception {
        String url = BASE_URL + "/controller/templates";

        // build the template
        TemplateDTO template = new TemplateDTO();
        template.setName("template-name");
        template.setSnippet(new FlowSnippetDTO());

        // build the body part
        BodyPart body = new FormDataBodyPart("template", template, MediaType.APPLICATION_XML_TYPE);

        // build the form
        FormDataMultiPart form = new FormDataMultiPart();
        form.bodyPart(body);

        // perform the request
        ClientResponse response = DFM_USER.testPostMultiPart(url, form);

        // verify success
        Assert.assertEquals(201, response.getStatus());

        // ensure the template is returned
        TemplateEntity responseTemplate = response.getEntity(TemplateEntity.class);
        Assert.assertEquals("template-name", responseTemplate.getTemplate().getName());
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
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Tests the ability to retrieve a specific user.
     *
     * @throws Exception ex
     */
    @Test
    public void testUserGet() throws Exception {
        int nonExistentUserId = 98775;
        String url = BASE_URL + "/controller/users/" + nonExistentUserId;

        // perform the request
        ClientResponse response = DFM_USER.testGet(url);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    /**
     * Verifies the admin user can update a person.
     *
     * @throws Exception ex
     */
    @Test
    public void testUserPut() throws Exception {
        int nonExistentUserId = 98775;
        String url = BASE_URL + "/controller/users/" + nonExistentUserId;

        // create the form data
        Map<String, String> formData = new HashMap<>();
        formData.put("status", "DISABLED");

        // perform the request
        ClientResponse response = DFM_USER.testPut(url, formData);

        // ensure the request is failed with a forbidden status code
        Assert.assertEquals(403, response.getStatus());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        // shutdown the server
        SERVER.shutdownServer();
        SERVER = null;

        // look for the flow.xml and toss it
        File flow = new File(FLOW_XML_PATH);
        if (flow.exists()) {
            flow.delete();
        }
        // look for templates and toss them
        final DirectoryStream<Path> dirStream = Files.newDirectoryStream(Paths.get(FLOW_TEMPLATES_PATH), "*.template");
        for (final Path path : dirStream) {
            Files.delete(path);
        }
    }
}
