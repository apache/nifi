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
package org.apache.nifi.tests.system.processor;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ClassloaderIsolationWithControllerServicePropertyIT extends NiFiSystemIT {

    @Test
    void testChangeClassloaderWhenControllerServicePropertyChange() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generateFlowFileProcessor = getClientUtil().createProcessor("GenerateFlowFile");
        ProcessorEntity classloaderIsolationProcessor = getClientUtil().createProcessor("ClassloaderIsolationWithServiceProperty");
        final ProcessorEntity terminateSuccess = getClientUtil().createProcessor("TerminateFlowFile");
        final ControllerServiceEntity service = getClientUtil().createControllerService("ClassloaderIsolationKeyProviderService");

        classloaderIsolationProcessor = getClientUtil().updateProcessorProperties(classloaderIsolationProcessor, Collections.singletonMap("Key Provider Service", service.getComponent().getId()));
        getClientUtil().updateControllerServiceProperties(service, Collections.singletonMap("Key Field", "Original value"));
        getClientUtil().enableControllerService(service);

        final ConnectionEntity classloaderIsolationConnection = getClientUtil().createConnection(classloaderIsolationProcessor, terminateSuccess, "success");
        final ConnectionEntity generateFlowFileConnection = getClientUtil().createConnection(generateFlowFileProcessor, classloaderIsolationProcessor, "success");

        // Create a FlowFile
        startProcessor(generateFlowFileProcessor, generateFlowFileConnection);

        // Wait for a FlowFile to be routed to success
        startProcessor(classloaderIsolationProcessor, classloaderIsolationConnection);

        final Map<String, String> firstFlowFileAttributes = getClientUtil().getQueueFlowFile(classloaderIsolationConnection.getId(), 0).getFlowFile().getAttributes();
        final String classloaderInstance1 = firstFlowFileAttributes.get("classloader.instance");

        // Stop processors and empty queues
        stopProcessorAndEmptyQueue(generateFlowFileProcessor, generateFlowFileConnection);
        stopProcessorAndEmptyQueue(classloaderIsolationProcessor, classloaderIsolationConnection);

        // Create a FlowFile
        startProcessor(generateFlowFileProcessor, generateFlowFileConnection);

        // Wait for a FlowFile to be routed to success
        startProcessor(classloaderIsolationProcessor, classloaderIsolationConnection);

        final Map<String, String> secondFlowFileAttributes = getClientUtil().getQueueFlowFile(classloaderIsolationConnection.getId(), 0).getFlowFile().getAttributes();
        final String classloaderInstance2 = secondFlowFileAttributes.get("classloader.instance");

        // The classloader instances should be the same in the two run since the service property didn't change
        assertEquals(classloaderInstance1, classloaderInstance2);

        // Stop processors and empty queues
        stopProcessorAndEmptyQueue(generateFlowFileProcessor, generateFlowFileConnection);
        stopProcessorAndEmptyQueue(classloaderIsolationProcessor, classloaderIsolationConnection);

        getClientUtil().disableControllerService(service);
        getClientUtil().updateControllerServiceProperties(service, Collections.singletonMap("Key Field", "Updated value"));
        getClientUtil().enableControllerService(service);

        // Create a FlowFile
        startProcessor(generateFlowFileProcessor, generateFlowFileConnection);

        // Wait for a FlowFile to be routed to success
        startProcessor(classloaderIsolationProcessor, classloaderIsolationConnection);

        final Map<String, String> thirdFlowFileAttributes = getClientUtil().getQueueFlowFile(classloaderIsolationConnection.getId(), 0).getFlowFile().getAttributes();
        final String classloaderInstance3 = thirdFlowFileAttributes.get("classloader.instance");

        // The classloader instances should be different since the service property changed
        assertNotEquals(classloaderInstance1, classloaderInstance3);
    }

    private void stopProcessorAndEmptyQueue(ProcessorEntity processor, ConnectionEntity connection) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().stopProcessor(processor);
        getClientUtil().emptyQueue(connection.getId());
    }

    private void startProcessor(ProcessorEntity processor, ConnectionEntity connection) throws InterruptedException, IOException, NiFiClientException {
        getClientUtil().waitForValidProcessor(processor.getId());
        getClientUtil().startProcessor(processor);
        waitForQueueCount(connection.getId(), 1);
    }
}
