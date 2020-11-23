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

package org.apache.nifi.stateless.controller.services;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StatelessControllerServiceSystemIT extends StatelessSystemIT {

    @Test
    public void testControllerServices() throws IOException, StatelessConfigurationException, InterruptedException {
        // Build the flow:
        // Root Input Port -> Child Input Port -> CountFlowFiles -> Child Output Port -> Root Output Port.
        // Controller Service at root group and at child group.
        // When Processor is triggered, it should call the Controller Service at the child group level, which should call the Controller Service as the Parent/root level.
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();

        final VersionedControllerService rootService = builder.createSimpleControllerService("StandardCountService", "CountService");
        final VersionedProcessGroup childGroup = builder.createProcessGroup("child");
        final VersionedControllerService childService = builder.createSimpleControllerService("StandardCountService", "CountService", childGroup);

        builder.addControllerServiceReference(childService, rootService, "Dependent Service");

        final VersionedProcessor processor = builder.createSimpleProcessor("CountFlowFiles", childGroup);
        builder.addControllerServiceReference(processor, childService, "Count Service");

        final VersionedPort rootIn = builder.createInputPort("Root In");
        final VersionedPort rootOut = builder.createOutputPort("Root Out");
        final VersionedPort childIn = builder.createInputPort("Child In", childGroup);
        final VersionedPort childOut = builder.createOutputPort("Child Out", childGroup);

        builder.createConnection(rootIn, childIn, Relationship.ANONYMOUS.getName());
        builder.createConnection(childIn, processor, Relationship.ANONYMOUS.getName(), childGroup);
        builder.createConnection(processor, childOut, "success", childGroup);
        builder.createConnection(childOut, rootOut, Relationship.ANONYMOUS.getName());

        // Create the flow
        final StatelessDataflow dataflow = loadDataflow(builder.getFlowSnapshot());

        // Enqueue FlowFile and trigger
        dataflow.enqueue(new byte[0], Collections.emptyMap(), "Root In");
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        final List<FlowFile> flowFilesOut = result.getOutputFlowFiles("Root Out");
        assertEquals(1, flowFilesOut.size());

        // Should be 2 because both the child Count Service and the Root-level Count Service got triggered
        final FlowFile out = flowFilesOut.get(0);
        assertEquals("2", out.getAttribute("count"));
    }
}
