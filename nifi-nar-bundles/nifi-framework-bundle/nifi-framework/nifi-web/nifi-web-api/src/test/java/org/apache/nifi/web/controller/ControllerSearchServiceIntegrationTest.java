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
package org.apache.nifi.web.controller;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.web.controller.ComponentMockUtil.getBasicRelationships;
import static org.apache.nifi.web.controller.ComponentMockUtil.getChildProcessGroup;
import static org.apache.nifi.web.controller.ComponentMockUtil.getConnection;
import static org.apache.nifi.web.controller.ComponentMockUtil.getFunnel;
import static org.apache.nifi.web.controller.ComponentMockUtil.getPort;
import static org.apache.nifi.web.controller.ComponentMockUtil.getProcessorNode;
import static org.apache.nifi.web.controller.ComponentMockUtil.getPublicPort;
import static org.apache.nifi.web.controller.ComponentMockUtil.getRemoteProcessGroup;

public class ControllerSearchServiceIntegrationTest extends AbstractControllerSearchIntegrationTest {

    @Test
    public void testSearchForRootBasedOnID() {
        // given
        givenRootProcessGroup();

        // when
        whenExecuteSearch(ROOT_PROCESSOR_GROUP_ID.substring(2, 7));

        // then
        thenResultConsists()
                .ofProcessGroup(getSimpleResult(ROOT_PROCESSOR_GROUP_ID,
                        ROOT_PROCESSOR_GROUP_NAME,
                        ROOT_PROCESSOR_GROUP_ID,
                        null,
                        null,
                        "Id: " + ROOT_PROCESSOR_GROUP_ID))
                .validate(results);
    }

    @Test
    public void testSearchForRootBasedOnNameAndComments() {
        // given
        final String commentForRoot = "test comment for " + ROOT_PROCESSOR_GROUP_NAME + " process group";
        final String searchQuery = ROOT_PROCESSOR_GROUP_NAME;
        final String processor2Id = "processor2";
        final String processor2Name = "NAME2";
        final String processor2Comment = "This comment is a test comment containing " + ROOT_PROCESSOR_GROUP_NAME;

        givenRootProcessGroup(commentForRoot)
                .withProcessor(getProcessorNode("processor1", "name1", AUTHORIZED))
                .withProcessor(getProcessorNode(processor2Id, processor2Name, processor2Comment,
                        Optional.of("versionId"), SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING, ValidationStatus.VALID,
                        new HashSet<>(),"Processor", Mockito.mock(Processor.class), new HashMap<>(), AUTHORIZED));

        // when
        whenExecuteSearch(searchQuery);

        // then
        thenResultConsists()
                .ofProcessGroup(getSimpleResult(ROOT_PROCESSOR_GROUP_ID,
                        ROOT_PROCESSOR_GROUP_NAME,
                        ROOT_PROCESSOR_GROUP_ID,
                        null,
                        null,
                        "Name: " + ROOT_PROCESSOR_GROUP_NAME,
                        "Comments: " + commentForRoot))
                .ofProcessor(getSimpleResultFromRoot(processor2Id, processor2Name, "Comments: " + processor2Comment))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnBasicAttributes() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("processor1", "name1", AUTHORIZED))
                .withProcessor(getProcessorNode("processor2", "NAME2", AUTHORIZED))
                .withProcessor(getProcessorNode("processor3", "name3", NOT_AUTHORIZED))
                .withProcessor(getProcessorNode("processor4", "other", AUTHORIZED))
                .withProcessor(getProcessorNode("processor5", "something", "The name of the processor is something",
                        Optional.of("versionId"), SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING, ValidationStatus.VALID,
                        new HashSet<>(),"Processor", Mockito.mock(Processor.class), new HashMap<>(), AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("childId", "child", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withInputPort(getPort("port1", "name4", "comment consisting name", ScheduledState.RUNNING, true, AUTHORIZED))
                .withOutputPort(getPort("port2", "TheName5", "comment", ScheduledState.RUNNING, true, AUTHORIZED))
                .withFunnel(getFunnel("hasNoName1", Optional.empty(), AUTHORIZED))
                .withFunnel(getFunnel("hasNoName2", Optional.empty(), NOT_AUTHORIZED));

        // when
        whenExecuteSearch("name");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor1", "name1", "Name: name1"))
                .ofProcessor(getSimpleResultFromRoot("processor2", "NAME2", "Name: NAME2"))
                .ofProcessor(getSimpleResultFromRoot("processor5", "something", "Comments: The name of the processor is something"))
                .ofInputPort(getSimpleResult("port1", "name4", "childId", "childId", "child", "Name: name4", "Comments: comment consisting name"))
                .ofOutputPort(getSimpleResult("port2", "TheName5", "childId", "childId", "child", "Name: TheName5"))
                .ofFunnel(getSimpleResult("hasNoName1", null, "childId", "childId", "child", "Id: hasNoName1"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnScheduling() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("processor1", "processor1name", SchedulingStrategy.EVENT_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING, ValidationStatus.VALID, AUTHORIZED))
                .withProcessor(getProcessorNode("processor2", "processor2name", SchedulingStrategy.EVENT_DRIVEN, ExecutionNode.ALL, ScheduledState.DISABLED, ValidationStatus.INVALID, AUTHORIZED))
                .withProcessor(getProcessorNode("processor3", "processor3name", SchedulingStrategy.EVENT_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING, ValidationStatus.VALID, NOT_AUTHORIZED))
                .withProcessor(getProcessorNode("processor4", "processor4name", SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.STOPPED, ValidationStatus.VALID, AUTHORIZED))
                .withProcessor(getProcessorNode("processor5", "eventHandlerProcessor", SchedulingStrategy.CRON_DRIVEN, ExecutionNode.PRIMARY, ScheduledState.RUNNING, ValidationStatus.VALID,
                        AUTHORIZED));

        // when
        whenExecuteSearch("event");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor1", "processor1name", "Scheduling strategy: Event driven"))
                .ofProcessor(getSimpleResultFromRoot("processor2", "processor2name", "Scheduling strategy: Event driven"))
                .ofProcessor(getSimpleResultFromRoot("processor5", "eventHandlerProcessor", "Name: eventHandlerProcessor"))
                .validate(results);


        // when
        whenExecuteSearch("timer");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor4", "processor4name", "Scheduling strategy: Timer driven"))
                .validate(results);


        // when
        whenExecuteSearch("primary");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor5", "eventHandlerProcessor", "Execution node: primary"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnExecution() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("processor1", "processor1name", SchedulingStrategy.PRIMARY_NODE_ONLY, ExecutionNode.PRIMARY, ScheduledState.RUNNING, ValidationStatus.VALID,
                        AUTHORIZED));

        // when
        whenExecuteSearch("primary");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor1", "processor1name",  "Execution node: primary", "Scheduling strategy: On primary node"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnScheduledState() {
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("processor1", "name1", SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING, ValidationStatus.VALID, AUTHORIZED))
                .withProcessor(getProcessorNode("processor2", "name2", SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING, ValidationStatus.VALID, NOT_AUTHORIZED))
                .withProcessor(getProcessorNode("processor3", "name3", SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING, ValidationStatus.INVALID, AUTHORIZED))
                .withProcessor(getProcessorNode("processor4", "name4", SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.STOPPING, ValidationStatus.VALID, AUTHORIZED))
                .withProcessor(getProcessorNode("processor5", "name5notDisabled", SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.STOPPED, ValidationStatus.VALID, AUTHORIZED))
                .withProcessor(getProcessorNode("processor6", "name6", SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.STARTING, ValidationStatus.VALIDATING, AUTHORIZED))
                .withProcessor(getProcessorNode("processor7", "name7", SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.DISABLED, ValidationStatus.VALID, AUTHORIZED))
                .withProcessor(getProcessorNode("processor8", "name8disabled", SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.DISABLED, ValidationStatus.INVALID, AUTHORIZED))
                .withInputPort(getPort("port1", "portName1", "", ScheduledState.RUNNING, true, AUTHORIZED))
                .withInputPort(getPort("port2", "portName2", "", ScheduledState.RUNNING, true, NOT_AUTHORIZED))
                .withInputPort(getPort("port3", "portName3", "", ScheduledState.DISABLED, true, AUTHORIZED))
                .withInputPort(getPort("port4", "portName4", "", ScheduledState.STOPPING, true, AUTHORIZED))
                .withInputPort(getPort("port5", "portName5", "", ScheduledState.STOPPED, true, AUTHORIZED))
                .withOutputPort(getPort("port6", "portName6", "", ScheduledState.RUNNING, true, AUTHORIZED))
                .withOutputPort(getPort("port7", "portName7", "", ScheduledState.STARTING, false, AUTHORIZED));

        // when
        whenExecuteSearch("disabled");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor5", "name5notDisabled", "Name: name5notDisabled"))
                .ofProcessor(getSimpleResultFromRoot("processor7", "name7", "Run status: Disabled"))
                .ofProcessor(getSimpleResultFromRoot("processor8", "name8disabled", "Run status: Disabled", "Name: name8disabled"))
                .ofInputPort(getSimpleResultFromRoot("port3", "portName3", "Run status: Disabled"))
                .validate(results);


        // when
        whenExecuteSearch("invalid");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor3", "name3", "Run status: Invalid"))
                .ofOutputPort(getSimpleResultFromRoot("port7", "portName7", "Run status: Invalid"))
                .validate(results);


        // when
        whenExecuteSearch("validating");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor6", "name6", "Run status: Validating"))
                .validate(results);


        // when
        whenExecuteSearch("running");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor1", "name1", "Run status: Running"))
                .ofProcessor(getSimpleResultFromRoot("processor3", "name3", "Run status: Running"))
                .ofInputPort(getSimpleResultFromRoot("port1", "portName1", "Run status: Running"))
                .ofOutputPort(getSimpleResultFromRoot("port6", "portName6", "Run status: Running"))
                .validate(results);


        // when
        whenExecuteSearch("stopped");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor5", "name5notDisabled", "Run status: Stopped"))
                .ofInputPort(getSimpleResultFromRoot("port5", "portName5", "Run status: Stopped"))
                .validate(results);


        // when
        whenExecuteSearch("stopping");

        // then
        thenResultIsEmpty();


        // when
        whenExecuteSearch("starting");

        // then
        thenResultIsEmpty();
    }

    @Test
    public void testSearchBasedOnRelationship() {
        // given
        final ProcessorNode processorNode1 = getProcessorNode("processor1", "name1", "", Optional.empty(), SchedulingStrategy.TIMER_DRIVEN,
                ExecutionNode.ALL, ScheduledState.RUNNING, ValidationStatus.VALID, getBasicRelationships(), "Processor", Mockito.mock(Processor.class),
                new HashMap<>(), AUTHORIZED);
        final ProcessorNode processorNode2 = getProcessorNode("processor2", "name2", "", Optional.empty(), SchedulingStrategy.TIMER_DRIVEN,
                ExecutionNode.ALL, ScheduledState.RUNNING, ValidationStatus.VALID, getBasicRelationships(), "Processor", Mockito.mock(Processor.class),
                new HashMap<>(), AUTHORIZED);

        givenRootProcessGroup()
                .withProcessor(processorNode1)
                .withProcessor(processorNode2)
                .withConnection(getConnection("connection1", "connection1name", getBasicRelationships(), processorNode1, processorNode2, AUTHORIZED));

        // when
        whenExecuteSearch("success");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor1", "name1", "Relationship: success"))
                .ofProcessor(getSimpleResultFromRoot("processor2", "name2", "Relationship: success"))
                .ofConnection(getSimpleResultFromRoot("connection1", "connection1name", "Relationship: success"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnConnectionMetadata() {
        // given
        final Processor processor = Mockito.mock(ComponentMockUtil.DummyProcessor.class);

        givenRootProcessGroup()
                .withProcessor(getProcessorNode("processor1", "name1", "", Optional.empty(), SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL,
                        ScheduledState.RUNNING, ValidationStatus.VALID, new HashSet<>(), "DummyProcessorForTest", processor,
                        new HashMap<>(), AUTHORIZED));

        // when
        whenExecuteSearch("dummy");

        // then
        Assert.assertEquals(1, results.getProcessorResults().size());

        final ComponentSearchResultDTO componentSearchResultDTO = results.getProcessorResults().get(0);
        Assert.assertEquals("processor1", componentSearchResultDTO.getId());
        Assert.assertEquals("name1", componentSearchResultDTO.getName());
        Assert.assertEquals(2, componentSearchResultDTO.getMatches().size());

        final String firstMatch = componentSearchResultDTO.getMatches().get(0);
        final String secondMatch = componentSearchResultDTO.getMatches().get(1);

        if ((!firstMatch.equals("Type: DummyProcessorForTest") || !secondMatch.startsWith("Type: ComponentMockUtil$DummyProcessor$MockitoMock$"))
                && (!secondMatch.equals("Type: DummyProcessorForTest") || !firstMatch.startsWith("Type: ComponentMockUtil$DummyProcessor$MockitoMock$"))) {
            Assert.fail();
        }
    }

    @Test
    public void testSearchBasedOnProperty() {
        // given
        final Map<PropertyDescriptor, String> rawProperties = new HashMap<>();
        final PropertyDescriptor descriptor1 = new PropertyDescriptor.Builder().name("property1").displayName("property1display").description("property1 description").sensitive(false).build();
        final PropertyDescriptor descriptor2 = new PropertyDescriptor.Builder().name("property2").displayName("property2display").description("property2 description").sensitive(true).build();
        rawProperties.put(descriptor1, "property1value");
        rawProperties.put(descriptor2, "property2value");

        final ProcessorNode processorNode = getProcessorNode("processor1", "name1", "", Optional.empty(), SchedulingStrategy.TIMER_DRIVEN,
                ExecutionNode.ALL, ScheduledState.RUNNING, ValidationStatus.VALID, new HashSet<>(), "Processor", Mockito.mock(Processor.class),
                rawProperties, AUTHORIZED);

        givenRootProcessGroup()
                .withProcessor(processorNode);

        // when
        whenExecuteSearch("property");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("processor1", "name1", "Property name: property1", "Property value: property1 - property1value",
                        "Property description: property1 description", "Property name: property2", "Property description: property2 description"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnProcessGroupAttribute() {
        // given
        givenRootProcessGroup();
        givenProcessGroup(getChildProcessGroup("groupA", "groupAName", "groupA comment", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL));
        givenProcessGroup(getChildProcessGroup("groupB", "groupBName", "groupB comment but contains groupA", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL));
        givenProcessGroup(getChildProcessGroup("groupC", "groupCName", "groupC comment", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), NOT_AUTHORIZED, NOT_UNDER_VERSION_CONTROL));
        givenProcessGroup(getChildProcessGroup("groupD", "groupDName", "groupD comment", getProcessGroup("groupA"), AUTHORIZED, NOT_UNDER_VERSION_CONTROL));

        // when
        whenExecuteSearch("groupA");

        // then
        thenResultConsists()
                .ofProcessGroup(getSimpleResultFromRoot("groupA","groupAName", "Id: groupA", "Name: groupAName", "Comments: groupA comment"))
                .ofProcessGroup(getSimpleResultFromRoot("groupB", "groupBName", "Comments: groupB comment but contains groupA"))
                .validate(results);


        // when
        whenExecuteSearch("name");

        // then
        thenResultConsists()
                .ofProcessGroup(getSimpleResultFromRoot("groupA","groupAName", "Name: groupAName"))
                .ofProcessGroup(getSimpleResultFromRoot("groupB","groupBName", "Name: groupBName"))
                .ofProcessGroup(getSimpleResult("groupD", "groupDName", "groupA", "groupA", "groupAName", "Name: groupDName"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnVariableRegistry() {
        // given
        givenRootProcessGroup();

        final Map<VariableDescriptor, String> variables = new HashMap<>();
        variables.put(new VariableDescriptor.Builder("variableName").build(), "variableValue");

        final ComponentVariableRegistry variableRegistry = Mockito.mock(ComponentVariableRegistry.class);
        Mockito.when(variableRegistry.getVariableMap()).thenReturn(variables);

        final ProcessGroup processGroup = getChildProcessGroup("childGroup", "childGroupName", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL);
        Mockito.when(processGroup.getVariableRegistry()).thenReturn(variableRegistry);

        givenProcessGroup(processGroup);

        // when
        whenExecuteSearch("variable");

        // then
        thenResultConsists()
                .ofProcessGroup(getSimpleResultFromRoot("childGroup", "childGroupName", "Variable Name: variableName", "Variable Value: variableValue"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnVariableRegistryInRoot() {
        // given
        givenRootProcessGroup();
        final ProcessGroup childProcessGroup = getChildProcessGroup("childGroup", "childGroupName", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL);
        givenProcessGroup(childProcessGroup);

        final Map<VariableDescriptor, String> variablesRoot = new HashMap<>();
        variablesRoot.put(new VariableDescriptor.Builder("variableName1").build(), "variableValue1");

        final ComponentVariableRegistry variableRegistryRoot = Mockito.mock(ComponentVariableRegistry.class);
        Mockito.when(variableRegistryRoot.getVariableMap()).thenReturn(variablesRoot);

        Mockito.when(getProcessGroup(ROOT_PROCESSOR_GROUP_ID).getVariableRegistry()).thenReturn(variableRegistryRoot);

        final Map<VariableDescriptor, String> variablesChild = new HashMap<>();
        variablesChild.put(new VariableDescriptor.Builder("variableName2").build(), "variableValue2");

        final ComponentVariableRegistry variableRegistryChild = Mockito.mock(ComponentVariableRegistry.class);
        Mockito.when(variableRegistryChild.getVariableMap()).thenReturn(variablesChild);

        Mockito.when(childProcessGroup.getVariableRegistry()).thenReturn(variableRegistryChild);

        // when
        whenExecuteSearch("variableValue");

        // then
        thenResultConsists()
                .ofProcessGroup(getSimpleResult(ROOT_PROCESSOR_GROUP_ID,
                        ROOT_PROCESSOR_GROUP_NAME,
                        ROOT_PROCESSOR_GROUP_ID,
                        null,
                        null,
                        "Variable Value: " + "variableValue1"))
                .ofProcessGroup(getSimpleResultFromRoot("childGroup", "childGroupName", "Variable Value: variableValue2"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnConnectionAttributes() {
        // given
        final ProcessorNode processor1 = getProcessorNode("processor1", "processor1Name", AUTHORIZED);
        final ProcessorNode processor2 = getProcessorNode("processor2", "processor2Name", AUTHORIZED);

        givenRootProcessGroup()
                .withProcessor(processor1)
                .withProcessor(processor2)
                .withConnection(getConnection("connection", "connectionName", getBasicRelationships(), processor1, processor2, AUTHORIZED));

        // when
        whenExecuteSearch("connection");

        // then
        thenResultConsists()
                .ofConnection(getSimpleResultFromRoot("connection", "connectionName", "Id: connection", "Name: connectionName"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnPriorities() {
        // given
        final ProcessorNode processor1 = getProcessorNode("processor1", "processor1Name", AUTHORIZED);
        final ProcessorNode processor2 = getProcessorNode("processor2", "processor2Name", AUTHORIZED);
        final Connection connection = getConnection("connection", "connectionName", getBasicRelationships(), processor1, processor2, AUTHORIZED);

        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        final List<FlowFilePrioritizer> prioritizers = new ArrayList<>();
        prioritizers.add(Mockito.mock(ComponentMockUtil.DummyFlowFilePrioritizer.class));
        Mockito.when(flowFileQueue.getPriorities()).thenReturn(prioritizers);
        Mockito.when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);

        givenRootProcessGroup()
                .withProcessor(processor1)
                .withProcessor(processor2)
                .withConnection(connection);

        // when
        whenExecuteSearch("dummy");

        // then
        Assert.assertEquals(1, results.getConnectionResults().size());
        Assert.assertEquals(1, results.getConnectionResults().get(0).getMatches().size());
        Assert.assertTrue(results.getConnectionResults().get(0).getMatches().get(0)
                .startsWith("Prioritizer: org.apache.nifi.web.controller.ComponentMockUtil$DummyFlowFilePrioritizer$"));
    }

    @Test
    public void testSearchBasedOnExpiration() {
        // given
        final ProcessorNode processor1 = getProcessorNode("processor1", "processor1Name", AUTHORIZED);
        final ProcessorNode processor2 = getProcessorNode("processor2", "processor2Name", AUTHORIZED);
        final Connection connection = getConnection("connection", "connectionName", getBasicRelationships(), processor1, processor2, AUTHORIZED);

        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        Mockito.when(flowFileQueue.getFlowFileExpiration(TimeUnit.MILLISECONDS)).thenReturn(5);
        Mockito.when(flowFileQueue.getFlowFileExpiration()).thenReturn("5");
        Mockito.when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);

        givenRootProcessGroup()
                .withProcessor(processor1)
                .withProcessor(processor2)
                .withConnection(connection);

        // when
        whenExecuteSearch("expire");

        // then
        thenResultConsists()
                .ofConnection(getSimpleResultFromRoot("connection", "connectionName", "FlowFile expiration: 5" ))
                .validate(results);


        // when
        whenExecuteSearch("expires");

        // then
        thenResultConsists()
                .ofConnection(getSimpleResultFromRoot("connection", "connectionName", "FlowFile expiration: 5" ))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnBackPressure() {
        // given
        final ProcessorNode processor1 = getProcessorNode("processor1", "processor1Name", AUTHORIZED);
        final ProcessorNode processor2 = getProcessorNode("processor2", "processor2Name", AUTHORIZED);
        final Connection connection = getConnection("connection", "connectionName", getBasicRelationships(), processor1, processor2, AUTHORIZED);

        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        Mockito.when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn("100 KB");
        Mockito.when(flowFileQueue.getBackPressureObjectThreshold()).thenReturn(5L);
        Mockito.when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);

        givenRootProcessGroup()
                .withProcessor(processor1)
                .withProcessor(processor2)
                .withConnection(connection);

        // when
        whenExecuteSearch("pressure");

        // then
        thenResultConsists()
                .ofConnection(getSimpleResultFromRoot("connection", "connectionName", "Back pressure data size: 100 KB", "Back pressure count: 5"))
                .validate(results);


        // when
        whenExecuteSearch("back pressure");

        // then
        thenResultConsists()
                .ofConnection(getSimpleResultFromRoot("connection", "connectionName", "Back pressure data size: 100 KB", "Back pressure count: 5"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnConnectivity() {
        // given
        final ProcessorNode processor1 = getProcessorNode("source", "sourceName", AUTHORIZED);
        final ProcessorNode processor2 = getProcessorNode("destination", "destinationName", AUTHORIZED);
        final Connection connection = getConnection("connection", "connectionName", getBasicRelationships(), processor1, processor2, AUTHORIZED);

        givenRootProcessGroup()
                .withProcessor(processor1)
                .withProcessor(processor2)
                .withConnection(connection);

        // when
        whenExecuteSearch("source");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("source", "sourceName", "Id: source", "Name: sourceName"))
                .ofConnection(getSimpleResultFromRoot("connection", "connectionName", "Source id: source", "Source name: sourceName"))
                .validate(results);


        // when
        whenExecuteSearch("destination");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("destination", "destinationName", "Id: destination", "Name: destinationName"))
                .ofConnection(getSimpleResultFromRoot("connection", "connectionName", "Destination id: destination", "Destination name: destinationName"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnRemoteProcessGroupAttributes() {
        // given
        givenRootProcessGroup()
                .withRemoteProcessGroup(getRemoteProcessGroup("remote", "remoteName", Optional.empty(), "", "localhost", true, AUTHORIZED));

        // when
        whenExecuteSearch("remote");

        // then
        thenResultConsists()
                .ofRemoteProcessGroup(getSimpleResultFromRoot("remote", "remoteName", "Id: remote", "Name: remoteName"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnRemoteProcessGroupURLs() {
        // given
        givenRootProcessGroup()
                .withRemoteProcessGroup(getRemoteProcessGroup("remote", "remoteName", Optional.empty(), "", "localhost", true, AUTHORIZED));

        // when
        whenExecuteSearch("localhost");

        // then
        thenResultConsists()
                .ofRemoteProcessGroup(getSimpleResultFromRoot("remote", "remoteName", "URLs: localhost"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnRemoteProcessTransmission() {
        // given
        givenRootProcessGroup()
                .withRemoteProcessGroup(getRemoteProcessGroup("remote1", "remoteName1", Optional.empty(), "", "localhost", true, AUTHORIZED))
                .withRemoteProcessGroup(getRemoteProcessGroup("remote2", "remoteName2", Optional.empty(), "", "localhost", false, AUTHORIZED));

        // when
        whenExecuteSearch("transmitting");

        // then
        thenResultConsists()
                .ofRemoteProcessGroup(getSimpleResultFromRoot("remote1", "remoteName1", "Transmission: On"))
                .validate(results);

        // when
        whenExecuteSearch("transmission enabled");

        // then
        thenResultConsists()
                .ofRemoteProcessGroup(getSimpleResultFromRoot("remote1", "remoteName1", "Transmission: On"))
                .validate(results);

        // when
        whenExecuteSearch("not transmitting");

        // then
        thenResultConsists()
                .ofRemoteProcessGroup(getSimpleResultFromRoot("remote2", "remoteName2", "Transmission: Off"))
                .validate(results);

        // when
        whenExecuteSearch("transmission disabled");

        // then
        thenResultConsists()
                .ofRemoteProcessGroup(getSimpleResultFromRoot("remote2", "remoteName2", "Transmission: Off"))
                .validate(results);
    }

    @Test
    public void testSearchBasedOnPortPublicity() {
        // given
        givenRootProcessGroup()
                .withInputPort(getPublicPort("port", "portName", "", ScheduledState.RUNNING, true, AUTHORIZED,
                        Arrays.asList("accessAllowed1"), Arrays.asList("accessAllowed2")));

        // when
        whenExecuteSearch("allowed");

        // then
        thenResultConsists()
                .ofInputPort(getSimpleResultFromRoot("port", "portName", "User access control: accessAllowed1", "Group access control: accessAllowed2"))
                .validate(results);
    }
}
