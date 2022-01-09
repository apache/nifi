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
package org.apache.nifi.integration.processgroup;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.queue.DropFlowFileState;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StandardProcessGroupIT extends FrameworkIntegrationTest {
    @Test
    public void testProcessGroupDefaults() {
        // Connect two processors with default settings of the root process group
        ProcessorNode sourceProcessor = createGenerateProcessor(1);
        ProcessorNode destinationProcessor = createProcessorNode((context, session) -> {});
        Connection connection = connect(sourceProcessor, destinationProcessor, Collections.singleton(REL_SUCCESS));


        // Verify all defaults are in place on the process group and the connection
        assertEquals("0 sec", getRootGroup().getDefaultFlowFileExpiration());
        assertEquals(NiFiProperties.DEFAULT_BACKPRESSURE_COUNT, (long) getRootGroup().getDefaultBackPressureObjectThreshold());
        assertEquals(NiFiProperties.DEFAULT_BACKPRESSURE_SIZE, getRootGroup().getDefaultBackPressureDataSizeThreshold());
        assertEquals("0 sec", connection.getFlowFileQueue().getFlowFileExpiration());
        assertEquals(NiFiProperties.DEFAULT_BACKPRESSURE_COUNT, (long) connection.getFlowFileQueue().getBackPressureObjectThreshold());
        assertEquals(NiFiProperties.DEFAULT_BACKPRESSURE_SIZE, connection.getFlowFileQueue().getBackPressureDataSizeThreshold());

        // Update default settings of the process group, and create a new connection
        getRootGroup().setDefaultFlowFileExpiration("99 min");
        getRootGroup().setDefaultBackPressureObjectThreshold(99L);
        getRootGroup().setDefaultBackPressureDataSizeThreshold("99 MB");

        ProcessorNode sourceProcessor1 = createGenerateProcessor(1);
        ProcessorNode destinationProcessor1 = createProcessorNode((context, session) -> {});
        Connection connection1 = connect(sourceProcessor1, destinationProcessor1, Collections.singleton(REL_SUCCESS));

        // Verify updated settings are in place on the process group and the connection
        assertEquals("99 min", getRootGroup().getDefaultFlowFileExpiration());
        assertEquals(99, (long) getRootGroup().getDefaultBackPressureObjectThreshold());
        assertEquals("99 MB", getRootGroup().getDefaultBackPressureDataSizeThreshold());
        assertEquals("99 min", connection1.getFlowFileQueue().getFlowFileExpiration());
        assertEquals(99, (long) connection1.getFlowFileQueue().getBackPressureObjectThreshold());
        assertEquals("99 MB", connection1.getFlowFileQueue().getBackPressureDataSizeThreshold());
    }

    @Test
    public void testDropAllFlowFilesFromOneConnection() throws Exception {
        ProcessorNode sourceProcessGroup = createGenerateProcessor(1);
        ProcessorNode destinationProcessGroup = createProcessorNode((context, session) -> {});

        Connection connection = connect(sourceProcessGroup, destinationProcessGroup, Collections.singleton(REL_SUCCESS));

        for (int i = 0; i < 5; i++) {
            triggerOnce(sourceProcessGroup);
        }
        assertEquals(5, connection.getFlowFileQueue().size().getObjectCount());

        // WHEN
        DropFlowFileStatus dropFlowFileStatus = getRootGroup().dropAllFlowFiles("requestId", "unimportant");
        while (dropFlowFileStatus.getState() != DropFlowFileState.COMPLETE) {
            TimeUnit.MILLISECONDS.sleep(10);
            dropFlowFileStatus = getRootGroup().getDropAllFlowFilesStatus("requestId");
        }

        // THEN
        assertEquals(0, connection.getFlowFileQueue().size().getObjectCount());
    }

    @Test
    public void testDropAllFlowFilesFromOneConnectionInChildProcessGroup() throws Exception {
        ProcessGroup childProcessGroup = getFlowController().getFlowManager().createProcessGroup("childProcessGroup");
        childProcessGroup.setName("ChildProcessGroup");
        getRootGroup().addProcessGroup(childProcessGroup);

        ProcessorNode sourceProcessGroup = createGenerateProcessor(1);
        moveProcessor(sourceProcessGroup, childProcessGroup);

        ProcessorNode destinationProcessGroup = createProcessorNode((context, session) -> {});
        moveProcessor(destinationProcessGroup, childProcessGroup);

        Connection connection = connect(childProcessGroup, sourceProcessGroup, destinationProcessGroup, Collections.singleton(REL_SUCCESS));

        for (int i = 0; i < 5; i++) {
            triggerOnce(sourceProcessGroup);
        }
        assertEquals(5, connection.getFlowFileQueue().size().getObjectCount());

        // WHEN
        DropFlowFileStatus dropFlowFileStatus = getRootGroup().dropAllFlowFiles("requestId", "unimportant");
        while (dropFlowFileStatus.getState() != DropFlowFileState.COMPLETE) {
            TimeUnit.MILLISECONDS.sleep(10);
            dropFlowFileStatus = childProcessGroup.getDropAllFlowFilesStatus("requestId");
        }

        // THEN
        assertEquals(0, connection.getFlowFileQueue().size().getObjectCount());
    }

    @Test
    public void testDropAllFlowFilesFromMultipleConnections() throws Exception {
        ProcessGroup childProcessGroup = getFlowController().getFlowManager().createProcessGroup("childProcessGroup");
        childProcessGroup.setName("ChildProcessGroup");
        getRootGroup().addProcessGroup(childProcessGroup);

        ProcessorNode sourceProcessGroup1 = createGenerateProcessor(4);
        moveProcessor(sourceProcessGroup1, childProcessGroup);

        ProcessorNode sourceProcessGroup2 = createGenerateProcessor(5);
        moveProcessor(sourceProcessGroup2, childProcessGroup);

        ProcessorNode destinationProcessGroup = createProcessorNode((context, session) -> {});
        moveProcessor(destinationProcessGroup, childProcessGroup);

        Connection connection1 = connect(childProcessGroup, sourceProcessGroup1, destinationProcessGroup, Collections.singleton(REL_SUCCESS));
        Connection connection2 = connect(childProcessGroup, sourceProcessGroup2, destinationProcessGroup, Collections.singleton(REL_SUCCESS));

        for (int i = 0; i < 5; i++) {
            triggerOnce(sourceProcessGroup1);
        }
        assertEquals(5, connection1.getFlowFileQueue().size().getObjectCount());

        for (int i = 0; i < 10; i++) {
            triggerOnce(sourceProcessGroup2);
        }
        assertEquals(10, connection2.getFlowFileQueue().size().getObjectCount());

        // WHEN
        DropFlowFileStatus dropFlowFileStatus = getRootGroup().dropAllFlowFiles("requestId", "unimportant");
        while (dropFlowFileStatus.getState() != DropFlowFileState.COMPLETE) {
            TimeUnit.MILLISECONDS.sleep(10);
            dropFlowFileStatus = childProcessGroup.getDropAllFlowFilesStatus("requestId");
        }

        // THEN
        assertEquals(5 + 10, dropFlowFileStatus.getOriginalSize().getObjectCount());
        assertEquals(20 + 50, dropFlowFileStatus.getOriginalSize().getByteCount());

        assertEquals(0, dropFlowFileStatus.getCurrentSize().getObjectCount());
        assertEquals(0, dropFlowFileStatus.getCurrentSize().getByteCount());

        assertEquals(5 + 10, dropFlowFileStatus.getDroppedSize().getObjectCount());
        assertEquals(20 + 50, dropFlowFileStatus.getDroppedSize().getByteCount());

        assertEquals(0, connection1.getFlowFileQueue().size().getObjectCount());
        assertEquals(0, connection2.getFlowFileQueue().size().getObjectCount());
    }

    @Test
    public void testComponentsAffectedByVariableOverridden() {
        final ProcessGroup child = getFlowController().getFlowManager().createProcessGroup("child");
        child.setName("Child");
        child.setVariables(Collections.singletonMap("number", "5"));

        getRootGroup().setVariables(Collections.singletonMap("number", "1"));
        getRootGroup().addProcessGroup(child);

        final ProcessorNode processor = createProcessorNode(NumberRefProcessor.class);
        processor.setProperties(Collections.singletonMap(NumberRefProcessor.NUMBER.getName(), "${number}"));
        moveProcessor(processor, child);

        final Set<ComponentNode> componentsAffected = child.getComponentsAffectedByVariable("number");
        assertEquals(1, componentsAffected.size());
        assertTrue(componentsAffected.contains(processor));

        final Set<ComponentNode> rootAffected = getRootGroup().getComponentsAffectedByVariable("number");
        assertTrue(rootAffected.isEmpty());

        processor.setScheduldingPeriod("1 hour");
        child.startProcessor(processor, false);

        getRootGroup().setVariables(Collections.singletonMap("number", "2"));

        try {
            child.setVariables(Collections.singletonMap("number", "10"));
            Assert.fail("Updated variable that is referenced by a running processor");
        } catch (final IllegalStateException ise) {
            // Expected
        }

        child.stopProcessor(processor);
    }


    public static class NumberRefProcessor extends AbstractProcessor {
        static final PropertyDescriptor NUMBER = new Builder()
            .name("Number")
            .displayName("Number")
            .description("A Number")
            .required(true)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("1")
            .build();

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Collections.singletonList(NUMBER);
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        }
    }
}
