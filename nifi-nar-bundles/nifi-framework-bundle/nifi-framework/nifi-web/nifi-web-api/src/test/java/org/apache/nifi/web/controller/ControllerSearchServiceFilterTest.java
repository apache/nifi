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
 */package org.apache.nifi.web.controller;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.web.controller.ComponentMockUtil.getChildProcessGroup;
import static org.apache.nifi.web.controller.ComponentMockUtil.getProcessorNode;

public class ControllerSearchServiceFilterTest extends AbstractControllerSearchIntegrationTest {

    @Test
    public void testScopeWhenChild() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("workingProcessor1", "processor1Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child", "childName", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor2", "processor2Name", AUTHORIZED));

        // when
        whenExecuteSearch("scope:here workingProcessor", "child");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResult("workingProcessor2", "processor2Name", "child", "child", "childName", "Id: workingProcessor2"))
                .validate(results);
    }

    @Test
    public void testScopeWhenRoot() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("workingProcessor1", "processor1Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child", "childName", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor2", "processor2Name", AUTHORIZED));

        // when
        whenExecuteSearch("scope:here workingProcessor", ROOT_PROCESSOR_GROUP_ID);

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("workingProcessor1", "processor1Name","Id: workingProcessor1"))
                .ofProcessor(getSimpleResult("workingProcessor2", "processor2Name", "child", "child", "childName", "Id: workingProcessor2"))
                .validate(results);
    }

    @Test
    public void testScopeWhenInvalidFilter() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("workingProcessor1", "processor1Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child", "childName", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor2", "processor2Name", AUTHORIZED));

        // when
        whenExecuteSearch("scope:there workingProcessor", "child");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("workingProcessor1", "processor1Name","Id: workingProcessor1"))
                .ofProcessor(getSimpleResult("workingProcessor2", "processor2Name", "child", "child", "childName", "Id: workingProcessor2"))
                .validate(results);
    }

    @Test
    public void testGroupWhenRoot() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("workingProcessor1", "processor1Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child1", "child1Name", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor2", "processor1Name", AUTHORIZED));

        // when:
        // Cannot use "group:NiFi flow" as filter since the scope is only considered until the first space in the query
        whenExecuteSearch("group:NiFi processor1Name");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResult("workingProcessor1", "processor1Name", ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME, "Name: processor1Name"))
                .ofProcessor(getSimpleResult("workingProcessor2", "processor1Name", "child1", "child1", "child1Name", "Name: processor1Name"))
                .validate(results);
    }

    @Test
    public void testGroupWhenHasChildGroup() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("workingProcessor1", "processor1Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child1", "child1Name", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor2", "processor2Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child2", "child2Name", "", getProcessGroup("child1"), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor3", "processor3Name", AUTHORIZED));


        // when
        whenExecuteSearch("group:child1 workingProcessor");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResult("workingProcessor2", "processor2Name", "child1", "child1", "child1Name", "Id: workingProcessor2"))
                .ofProcessor(getSimpleResult("workingProcessor3", "processor3Name", "child2", "child2", "child2Name", "Id: workingProcessor3"))
                .validate(results);
    }

    @Test
    public void testGroupWhenUnknown() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("workingProcessor1", "processor1Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child1", "child1Name", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor2", "processor2Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child2", "child2Name", "", getProcessGroup("child1"), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor3", "processor3Name", AUTHORIZED));


        // when
        whenExecuteSearch("group:unknown workingProcessor");

        // then
        thenResultIsEmpty();
    }

    @Test
    public void testGroupWhenNotAuthorized() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("workingProcessor1", "processor1Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child1", "child1Name", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor2", "processor2Name", NOT_AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child2", "child2Name", "", getProcessGroup("child1"), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor3", "processor3Name", AUTHORIZED));


        // when
        whenExecuteSearch("group:child1 workingProcessor");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResult("workingProcessor3", "processor3Name", "child2", "child2", "child2Name", "Id: workingProcessor3"))
                .validate(results);
    }

    @Test
    public void testGroupWithScopeWhenOverlapping() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("workingProcessor1", "processor1Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child1", "child1Name", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor2", "processor2Name", NOT_AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child2", "child2Name", "", getProcessGroup("child1"), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor3", "processor3Name", AUTHORIZED));


        // when
        whenExecuteSearch("scope:here group:child2 workingProcessor", "child1");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResult("workingProcessor3", "processor3Name", "child2", "child2", "child2Name", "Id: workingProcessor3"))
                .validate(results);


        // when
        whenExecuteSearch("scope:here group:child1 workingProcessor", "child2");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResult("workingProcessor3", "processor3Name", "child2", "child2", "child2Name", "Id: workingProcessor3"))
                .validate(results);
    }

    @Test
    public void testGroupWithScopeWhenNotOverlapping() {
        // given
        givenRootProcessGroup()
                .withProcessor(getProcessorNode("workingProcessor1", "processor1Name", AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child1", "child1Name", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor2", "processor2Name", NOT_AUTHORIZED));

        givenProcessGroup(getChildProcessGroup("child2", "child2Name", "", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withProcessor(getProcessorNode("workingProcessor3", "processor3Name", AUTHORIZED));

        // when
        whenExecuteSearch("scope:here group:child2 workingProcessor", "child1");

        // then
        thenResultIsEmpty();
    }

    @Test
    public void testPropertiesAreExcluded() {
        // given
        final Map<PropertyDescriptor, String> rawProperties = new HashMap<>();
        final PropertyDescriptor descriptor = new PropertyDescriptor.Builder().name("property1").displayName("property1display").description("property1 description").sensitive(false).build();
        rawProperties.put(descriptor, "working");

        givenRootProcessGroup()
                .withProcessor(getProcessorNode("workingProcessor1", "processor1Name", "", Optional.empty(), SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING,
                        ValidationStatus.VALID, new HashSet<>(), "Processor", Mockito.mock(Processor.class), new HashMap<>(), AUTHORIZED))
                .withProcessor(getProcessorNode("processor2", "processor2Name", "", Optional.empty(), SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING,
                        ValidationStatus.VALID, new HashSet<>(), "Processor", Mockito.mock(Processor.class), rawProperties, AUTHORIZED));

        // when
        whenExecuteSearch("properties:exclude working");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("workingProcessor1", "processor1Name", "Id: workingProcessor1"))
                .validate(results);


        // when
        whenExecuteSearch("properties:invalid working");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("workingProcessor1", "processor1Name", "Id: workingProcessor1"))
                .ofProcessor(getSimpleResultFromRoot("processor2", "processor2Name", "Property value: property1 - working"))
                .validate(results);
    }
}