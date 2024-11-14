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

package org.apache.nifi.tests.system.stateless;

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClusteredStatelessFlowIT extends NiFiSystemIT {
    private static final String HELLO_WORLD = "Hello World";
    private static final String HELLO = "Hello";

    private ProcessorEntity generate;
    private ProcessorEntity terminate;
    private PortEntity inputPort;
    private PortEntity outputPort;
    private ConnectionEntity outputToTerminate;
    private ConnectionEntity generateToInput;
    private ProcessGroupEntity statelessGroup;


    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }


    @Test
    public void testChangeStatelessFlowWhileNotDisconnected() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();
        final ConnectionEntity inputToOutput = getClientUtil().createConnection(inputPort, outputPort, statelessGroup.getId());

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        disconnectNode(2);
        switchClientToNode(1);

        getClientUtil().stopProcessGroupComponents(statelessGroup.getId());

        final ProcessorEntity reverseContents = getClientUtil().createProcessor("ReverseContents", statelessGroup.getId());
        getClientUtil().createConnection(reverseContents, outputPort, "success", statelessGroup.getId());
        getClientUtil().waitForValidProcessor(reverseContents.getId());

        // Change connection from Input to Output to instead have a destination of ReverseContents
        final ConnectionDTO inputOutputConnectionDto = inputToOutput.getComponent();
        inputOutputConnectionDto.getDestination().setId(reverseContents.getId());
        inputOutputConnectionDto.getDestination().setType("PROCESSOR");
        getNifiClient().getConnectionClient().updateConnection(inputToOutput);

        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        reconnectNode(2);
        waitForAllNodesConnected();

        getClientUtil().startProcessor(generate);
        waitForQueueCount(outputToTerminate.getId(), 2);

        final String reversedContent = new StringBuilder(HELLO_WORLD).reverse().toString();
        for (int i = 0; i < 2; i++) {
            final String flowFileContents = getClientUtil().getFlowFileContentAsUtf8(outputToTerminate.getId(), i);
            assertEquals(reversedContent, flowFileContents);
        }
    }

    @Test
    public void testUpdateParameterReferencedByStatelessFlow() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell();

        final Map<String, String> parameterValues = new HashMap<>();
        parameterValues.put("Greeting", "Hello");
        final ParameterContextEntity paramContext = getClientUtil().createParameterContext("Context 1", parameterValues);
        getClientUtil().setParameterContext(statelessGroup.getId(), paramContext);

        final ProcessorEntity setAttribute = getClientUtil().createProcessor("SetAttribute", statelessGroup.getId());
        getClientUtil().updateProcessorProperties(setAttribute, Collections.singletonMap("Greeting", "#{Greeting}"));

        getClientUtil().createConnection(inputPort, setAttribute, statelessGroup.getId());
        getClientUtil().createConnection(setAttribute, outputPort, "success", statelessGroup.getId());

        getClientUtil().waitForValidProcessor(setAttribute.getId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToInput, 2);
        getClientUtil().startProcessGroupComponents(statelessGroup.getId());

        waitForQueueCount(outputToTerminate, 2);

        for (int i = 0; i < 2; i++) {
            final FlowFileEntity flowFileEntity = getClientUtil().getQueueFlowFile(outputToTerminate.getId(), i);
            assertEquals("Hello", flowFileEntity.getFlowFile().getAttributes().get("Greeting"));
        }

        getClientUtil().stopProcessor(generate);
        final ParameterContextUpdateRequestEntity updateRequest = getClientUtil().updateParameterContext(paramContext, "Greeting", "Good-bye");
        getClientUtil().waitForParameterContextRequestToComplete(paramContext.getId(), updateRequest.getRequest().getRequestId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(outputToTerminate, 4);

        for (int i = 2; i < 4; i++) {
            final FlowFileEntity flowFileEntity = getClientUtil().getQueueFlowFile(outputToTerminate.getId(), i);
            assertEquals("Good-bye", flowFileEntity.getFlowFile().getAttributes().get("Greeting"));
        }
    }

    private void createFlowShell() throws NiFiClientException, IOException, InterruptedException {
        createFlowShell("1 min");
    }

    private void createFlowShell(final String timeout) throws NiFiClientException, IOException, InterruptedException {
        generate = getClientUtil().createProcessor(GENERATE_FLOWFILE);
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", HELLO_WORLD);
        generateProperties.put("Greeting", HELLO);
        getClientUtil().updateProcessorProperties(generate, generateProperties);

        statelessGroup = getClientUtil().createProcessGroup("Stateless", "root");
        getClientUtil().markStateless(statelessGroup, timeout);

        inputPort = getClientUtil().createInputPort("In", statelessGroup.getId());
        outputPort = getClientUtil().createOutputPort("Out", statelessGroup.getId());

        terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE);

        generateToInput = getClientUtil().createConnection(generate, inputPort, "success");
        outputToTerminate = getClientUtil().createConnection(outputPort, terminate);

        getClientUtil().waitForValidProcessor(generate.getId());
    }

}
