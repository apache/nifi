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
package org.apache.nifi.integration;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.integration.util.NiFiTestUser;
import org.apache.nifi.integration.util.SourceTestProcessor;
import org.apache.nifi.integration.util.TerminationTestProcessor;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Ignore;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
@Ignore
public class NiFiWebApiTest {

    public static void populateFlow(Client client, String baseUrl, NiFiTestUser user, String clientId) throws Exception {

        // -----------------------------------------------
        // Create a source processor
        // -----------------------------------------------

        // create the local selection processor
        ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setName("Pick up");
        processorDTO.setType(SourceTestProcessor.class.getName());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId);
        revision.setVersion(0l);

        // create the local selection processor entity
        ProcessorEntity processorEntity = new ProcessorEntity();
        processorEntity.setRevision(revision);
        processorEntity.setComponent(processorDTO);

        // add the processor
        ClientResponse response = user.testPost(baseUrl + "/process-groups/root/processors", processorEntity);

        // ensure a successful response
        if (Status.CREATED.getStatusCode() != response.getStatusInfo().getStatusCode()) {
            // since it was unable to create the component attempt to extract an
            // error message from the response body
            final String responseEntity = response.getEntity(String.class);
            throw new Exception("Unable to populate initial flow: " + responseEntity);
        }

        // get the processors id
        processorEntity = response.getEntity(ProcessorEntity.class);
        processorDTO = processorEntity.getComponent();
        String localSelectionId = processorDTO.getId();
        String localSelectionGroupId = processorDTO.getParentGroupId();

        // -----------------------------------------------
        // Create a termination processor
        // -----------------------------------------------

        // create the termination processor
        processorDTO = new ProcessorDTO();
        processorDTO.setName("End");
        processorDTO.setType(TerminationTestProcessor.class.getName());

        // create the termination processor entity
        processorEntity = new ProcessorEntity();
        processorEntity.setRevision(revision);
        processorEntity.setComponent(processorDTO);

        // add the processor
        response = user.testPost(baseUrl + "/process-groups/root/processors", processorEntity);

        // ensure a successful response
        if (Status.CREATED.getStatusCode() != response.getStatusInfo().getStatusCode()) {
            // since it was unable to create the component attempt to extract an
            // error message from the response body
            final String responseEntity = response.getEntity(String.class);
            throw new Exception("Unable to populate initial flow: " + responseEntity);
        }

        // get the processors id
        processorEntity = response.getEntity(ProcessorEntity.class);
        processorDTO = processorEntity.getComponent();
        String terminationId = processorDTO.getId();
        String terminationGroupId = processorDTO.getParentGroupId();

        // -----------------------------------------------
        // Connect the two processors
        // -----------------------------------------------

        ConnectableDTO source = new ConnectableDTO();
        source.setId(localSelectionId);
        source.setGroupId(localSelectionGroupId);
        source.setType(ConnectableType.PROCESSOR.name());

        ConnectableDTO target = new ConnectableDTO();
        target.setId(terminationId);
        target.setGroupId(terminationGroupId);
        target.setType(ConnectableType.PROCESSOR.name());

        // create the relationships
        Set<String> relationships = new HashSet<>();
        relationships.add("success");

        // create the connection
        ConnectionDTO connectionDTO = new ConnectionDTO();
        connectionDTO.setSource(source);
        connectionDTO.setDestination(target);
        connectionDTO.setSelectedRelationships(relationships);

        // create the connection entity
        ConnectionEntity connectionEntity = new ConnectionEntity();
        connectionEntity.setRevision(revision);
        connectionEntity.setComponent(connectionDTO);

        // add the processor
        response = user.testPost(baseUrl + "/process-groups/root/connections", connectionEntity);

        // ensure a successful response
        if (Status.CREATED.getStatusCode() != response.getStatusInfo().getStatusCode()) {
            // since it was unable to create the component attempt to extract an
            // error message from the response body
            final String responseEntity = response.getEntity(String.class);
            throw new Exception("Unable to populate initial flow: " + responseEntity);
        }

        // -----------------------------------------------
        // Create a label
        // -----------------------------------------------

        // create the label
        LabelDTO labelDTO = new LabelDTO();
        labelDTO.setLabel("Test label");

        // create the label entity
        LabelEntity labelEntity = new LabelEntity();
        labelEntity.setRevision(revision);
        labelEntity.setComponent(labelDTO);

        // add the label
        response = user.testPost(baseUrl + "/process-groups/root/labels", labelEntity);

        // ensure a successful response
        if (Status.CREATED.getStatusCode() != response.getStatusInfo().getStatusCode()) {
            // since it was unable to create the component attempt to extract an
            // error message from the response body
            final String responseEntity = response.getEntity(String.class);
            throw new Exception("Unable to populate initial flow: " + responseEntity);
        }

        // -----------------------------------------------
        // Create a funnel
        // -----------------------------------------------

        // create the funnel
        FunnelDTO funnelDTO = new FunnelDTO();

        // create the funnel entity
        FunnelEntity funnelEntity = new FunnelEntity();
        funnelEntity.setRevision(revision);
        funnelEntity.setComponent(funnelDTO);

        // add the funnel
        response = user.testPost(baseUrl + "/process-groups/root/funnels", funnelEntity);

        // ensure a successful response
        if (Status.CREATED.getStatusCode() != response.getStatusInfo().getStatusCode()) {
            // since it was unable to create the component attempt to extract an
            // error message from the response body
            final String responseEntity = response.getEntity(String.class);
            throw new Exception("Unable to populate initial flow: " + responseEntity);
        }

        // -----------------------------------------------
        // Create a process group
        // -----------------------------------------------

        // create the process group
        ProcessGroupDTO processGroup = new ProcessGroupDTO();
        processGroup.setName("group name");

        // create the process group entity
        ProcessGroupEntity processGroupEntity = new ProcessGroupEntity();
        processGroupEntity.setRevision(revision);
        processGroupEntity.setComponent(processGroup);

        // add the process group
        response = user.testPost(baseUrl + "/process-groups/root/process-groups", processGroupEntity);

        // ensure a successful response
        if (Status.CREATED.getStatusCode() != response.getStatusInfo().getStatusCode()) {
            // since it was unable to create the component attempt to extract an
            // error message from the response body
            final String responseEntity = response.getEntity(String.class);
            throw new Exception("Unable to populate initial flow: " + responseEntity);
        }

        // -----------------------------------------------
        // Create an input port
        // -----------------------------------------------

        // create the input port
        PortDTO inputPort = new PortDTO();
        inputPort.setName("input");

        // create the input port entity
        PortEntity inputPortEntity = new PortEntity();
        inputPortEntity.setRevision(revision);
        inputPortEntity.setComponent(inputPort);

        // add the input port
        response = user.testPost(baseUrl + "/process-groups/root/input-ports", inputPortEntity);

        // ensure a successful response
        if (Status.CREATED.getStatusCode() != response.getStatusInfo().getStatusCode()) {
            // since it was unable to create the component attempt to extract an
            // error message from the response body
            final String responseEntity = response.getEntity(String.class);
            throw new Exception("Unable to populate initial flow: " + responseEntity);
        }

        // -----------------------------------------------
        // Create a output ports
        // -----------------------------------------------

        // create the process group
        PortDTO outputPort = new PortDTO();
        outputPort.setName("output");

        // create the process group entity
        PortEntity outputPortEntity = new PortEntity();
        outputPortEntity.setRevision(revision);
        outputPortEntity.setComponent(outputPort);

        // add the output port
        response = user.testPost(baseUrl + "/process-groups/root/output-ports", outputPortEntity);

        // ensure a successful response
        if (Status.CREATED.getStatusCode() != response.getStatusInfo().getStatusCode()) {
            // since it was unable to create the component attempt to extract an
            // error message from the response body
            final String responseEntity = response.getEntity(String.class);
            throw new Exception("Unable to populate initial flow: " + responseEntity);
        }

    }

}
