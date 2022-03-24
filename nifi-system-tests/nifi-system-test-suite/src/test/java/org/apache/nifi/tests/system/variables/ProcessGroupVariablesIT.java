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
package org.apache.nifi.tests.system.variables;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ProcessGroupVariablesIT extends NiFiSystemIT {

    @Test
    public void testChangeVariableWhileProcessorRunning() throws NiFiClientException, IOException, InterruptedException {
        // Add variable abc=123 to the root group
        ProcessGroupEntity rootGroup = getNifiClient().getProcessGroupClient().getProcessGroup("root");
        getClientUtil().updateVariableRegistry(rootGroup, Collections.singletonMap("abc", "123"));

        // Create GenerateFlowFile that uses Variable to create FlowFile attribute. Connect to CountEvents processor.
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generateFlowFile, Collections.singletonMap("Value", "Hello ${abc}"));
        getClientUtil().updateProcessorSchedulingPeriod(generateFlowFile, "10 mins");

        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents");
        final ConnectionEntity connection = getClientUtil().createConnection(generateFlowFile, countEvents, "success");
        getClientUtil().setAutoTerminatedRelationships(countEvents, "success");

        // Wait for processor to be valid
        getClientUtil().waitForValidProcessor(generateFlowFile.getId());

        // Start Processor, wait for 1 FlowFile to be queued up, then stop processor
        getNifiClient().getProcessorClient().startProcessor(generateFlowFile);
        waitForQueueCount(connection.getId(), 1);

        final FlowFileEntity flowFile = getClientUtil().getQueueFlowFile(connection.getId(), 0);
        assertEquals("Hello 123", flowFile.getFlowFile().getAttributes().get("Value"));

        getClientUtil().emptyQueue(connection.getId());
        waitForQueueCount(connection.getId(), 0);

        rootGroup = getNifiClient().getProcessGroupClient().getProcessGroup("root");
        getClientUtil().updateVariableRegistry(rootGroup, Collections.singletonMap("abc", "xyz"));
        waitForQueueCount(connection.getId(), 1);

        final FlowFileEntity secondFlowFile = getClientUtil().getQueueFlowFile(connection.getId(), 0);
        assertEquals("Hello xyz", secondFlowFile.getFlowFile().getAttributes().get("Value"));
    }


    @Test
    public void testChangeHigherLevelVariableWhileProcessorRunning() throws NiFiClientException, IOException, InterruptedException {
        // Add variable abc=123 to the root group
        ProcessGroupEntity rootGroup = getNifiClient().getProcessGroupClient().getProcessGroup("root");
        getClientUtil().updateVariableRegistry(rootGroup, Collections.singletonMap("abc", "123"));

        final ProcessGroupEntity childGroup = getClientUtil().createProcessGroup("child", "root");

        // Create GenerateFlowFile that uses Variable to create FlowFile attribute. Connect to CountEvents processor.
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile", childGroup.getId());
        getClientUtil().updateProcessorProperties(generateFlowFile, Collections.singletonMap("Value", "Hello ${abc}"));
        getClientUtil().updateProcessorSchedulingPeriod(generateFlowFile, "10 mins");

        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents", childGroup.getId());
        final ConnectionEntity connection = getClientUtil().createConnection(generateFlowFile, countEvents, "success");
        getClientUtil().setAutoTerminatedRelationships(countEvents, "success");

        // Wait for processor to be valid
        getClientUtil().waitForValidProcessor(generateFlowFile.getId());

        // Start Processor, wait for 1 FlowFile to be queued up, then stop processor
        getNifiClient().getProcessorClient().startProcessor(generateFlowFile);
        waitForQueueCount(connection.getId(), 1);

        final FlowFileEntity flowFile = getClientUtil().getQueueFlowFile(connection.getId(), 0);
        assertEquals("Hello 123", flowFile.getFlowFile().getAttributes().get("Value"));

        getClientUtil().emptyQueue(connection.getId());
        waitForQueueCount(connection.getId(), 0);

        rootGroup = getNifiClient().getProcessGroupClient().getProcessGroup("root");
        getClientUtil().updateVariableRegistry(rootGroup, Collections.singletonMap("abc", "xyz"));
        waitForQueueCount(connection.getId(), 1);

        final FlowFileEntity secondFlowFile = getClientUtil().getQueueFlowFile(connection.getId(), 0);
        assertEquals("Hello xyz", secondFlowFile.getFlowFile().getAttributes().get("Value"));
    }


    @Test
    public void testChangeVariableThatIsOverridden() throws NiFiClientException, IOException, InterruptedException {
        // Add variable abc=123 to the root group
        ProcessGroupEntity rootGroup = getNifiClient().getProcessGroupClient().getProcessGroup("root");
        getClientUtil().updateVariableRegistry(rootGroup, Collections.singletonMap("abc", "123"));

        final ProcessGroupEntity childGroup = getClientUtil().createProcessGroup("child", "root");
        getClientUtil().updateVariableRegistry(childGroup, Collections.singletonMap("abc", "123"));

        // Create GenerateFlowFile that uses Variable to create FlowFile attribute. Connect to CountEvents processor.
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile", childGroup.getId());
        getClientUtil().updateProcessorProperties(generateFlowFile, Collections.singletonMap("Value", "Hello ${abc}"));
        getClientUtil().updateProcessorSchedulingPeriod(generateFlowFile, "10 mins");

        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents", childGroup.getId());
        final ConnectionEntity connection = getClientUtil().createConnection(generateFlowFile, countEvents, "success");
        getClientUtil().setAutoTerminatedRelationships(countEvents, "success");

        // Wait for processor to be valid
        getClientUtil().waitForValidProcessor(generateFlowFile.getId());

        // Start Processor, wait for 1 FlowFile to be queued up, then stop processor
        getNifiClient().getProcessorClient().startProcessor(generateFlowFile);
        waitForQueueCount(connection.getId(), 1);

        final FlowFileEntity flowFile = getClientUtil().getQueueFlowFile(connection.getId(), 0);
        assertEquals("Hello 123", flowFile.getFlowFile().getAttributes().get("Value"));

        getClientUtil().emptyQueue(connection.getId());
        waitForQueueCount(connection.getId(), 0);

        rootGroup = getNifiClient().getProcessGroupClient().getProcessGroup("root");
        getClientUtil().updateVariableRegistry(rootGroup, Collections.singletonMap("abc", "xyz"));

        // Wait a bit and ensure that the queue is still empty.
        Thread.sleep(2000L);

        assertEquals(0, getConnectionQueueSize(connection.getId()));
    }
}
