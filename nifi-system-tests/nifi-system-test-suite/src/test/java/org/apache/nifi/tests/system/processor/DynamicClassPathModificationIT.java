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
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamicClassPathModificationIT extends NiFiSystemIT {

    private ProcessorEntity generate;
    private ProcessorEntity modify;

    private ConnectionEntity modifyInputConnection;
    private ConnectionEntity successConnection;
    private ConnectionEntity failureConnection;


    @Test
    public void testLoadsClassOnBaseClasspath() throws NiFiClientException, IOException, InterruptedException {
        createFlow();

        // Configure with a class that is always on the classpath
        final Map<String, String> propertyMap = new HashMap<>();
        propertyMap.put("Class to Load", "org.apache.nifi.flowfile.FlowFile");
        getClientUtil().updateProcessorProperties(modify, propertyMap);
        getClientUtil().waitForValidProcessor(modify.getId());

        // Let Generate create a FlowFile
        getClientUtil().startProcessor(generate);
        waitForQueueCount(modifyInputConnection.getId(), 1);

        // Start the processor and make sure that the FlowFile is routed to success
        getClientUtil().startProcessor(modify);
        waitForQueueCount(successConnection.getId(), 1);
        assertEquals(0, getConnectionQueueSize(failureConnection.getId()));
    }

    @Test
    public void testLoadsClassFromDynamicModification() throws NiFiClientException, IOException, InterruptedException {
        createFlow();

        final Map<String, String> propertyMap = new HashMap<>();
        propertyMap.put("Class to Load", "org.apache.commons.lang3.StringUtils");
        getClientUtil().updateProcessorProperties(modify, propertyMap);
        getClientUtil().waitForValidProcessor(modify.getId());

        // Let Generate create a FlowFile
        getClientUtil().startProcessor(generate);
        waitForQueueCount(modifyInputConnection.getId(), 1);

        // Start the processor and we expect the FlowFile to go to failure because the StringUtils class should not be available
        getClientUtil().startProcessor(modify);
        waitForQueueCount(failureConnection.getId(), 1);
        assertEquals(0, getConnectionQueueSize(successConnection.getId()));

        getClientUtil().stopProcessor(modify);
        getClientUtil().stopProcessor(generate);

        getClientUtil().waitForStoppedProcessor(modify.getId());
        getClientUtil().waitForStoppedProcessor(generate.getId());

        // Update modify to have the appropriate URL
        propertyMap.put("URLs to Load", getCommonsLangJar().toURI().toURL().toString());
        getClientUtil().updateProcessorProperties(modify, propertyMap);
        getClientUtil().waitForValidProcessor(modify.getId());

        // Let Generate create another FlowFile
        getClientUtil().startProcessor(generate);
        waitForQueueCount(modifyInputConnection.getId(), 1);

        // Wait for a FlowFile to be routed to success
        getClientUtil().startProcessor(modify);
        waitForQueueCount(successConnection.getId(), 1);

        getClientUtil().stopProcessor(generate);
        getClientUtil().waitForStoppedProcessor(generate.getId());

        // Restart and ensure that everything works as expected after restart
        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        // Feed another FlowFile through. Upon restart, in order to modify, we need to get the most up-to-date revision so will first fetch the Processor
        final ProcessorEntity generateAfterRestart = getNifiClient().getProcessorClient().getProcessor(generate.getId());
        getClientUtil().startProcessor(generateAfterRestart);

        // Depending on whether or not the flow was written out with the processor running, the Modify processor may or may not be running. Ensure that it is running.
        getClientUtil().waitForValidationCompleted(modify);
        final ProcessorEntity modifyAfterRestart = getNifiClient().getProcessorClient().getProcessor(modify.getId());
        final String modifyRunStatus = modifyAfterRestart.getStatus().getRunStatus();
        if (!"Running".equalsIgnoreCase(modifyRunStatus)) {
            getClientUtil().startProcessor(modifyAfterRestart);
        }

        // We now expect 2 FlowFiles to be in the success route
        waitForQueueCount(successConnection.getId(), 2);
    }


    @Test
    public void testSuccessAfterTerminate() throws NiFiClientException, IOException, InterruptedException {
        createFlow();

        // Configure so that the processor should succeed but sleep for 5 mins so that we can terminate it
        final Map<String, String> propertyMap = new HashMap<>();
        propertyMap.put("Class to Load", "org.apache.commons.lang3.StringUtils");
        propertyMap.put("URLs to Load", getCommonsLangJar().toURI().toURL().toString());
        propertyMap.put("Sleep Duration", "${sleep}");
        getClientUtil().updateProcessorProperties(modify, propertyMap);
        getClientUtil().waitForValidProcessor(modify.getId());

        // Tell Generate Processor to add an attribute named 'sleep' with 5 mins as the value
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("sleep", "5 mins"));
        getClientUtil().waitForValidProcessor(generate.getId());

        // Let Generate create a FlowFile
        getClientUtil().startProcessor(generate);
        waitForQueueCount(modifyInputConnection.getId(), 1);

        // Start the processor, wait a bit, stop it and terminate it.
        getClientUtil().startProcessor(modify);
        Thread.sleep(2000L);
        getNifiClient().getProcessorClient().stopProcessor(modify);
        getNifiClient().getProcessorClient().terminateProcessor(modify.getId());
        getClientUtil().waitForStoppedProcessor(modify.getId());

        // Empty the queue. Because the processor was terminated, it may still hold the FlowFile for a bit until the terminate completes.
        // Because of that we'll wait until the queue is empty, attempting to empty it if it is not.
        getClientUtil().emptyQueue(modifyInputConnection.getId());
        waitFor(() -> {
            final int queueSize = getConnectionQueueSize(modifyInputConnection.getId());
            if (queueSize == 0) {
                return true;
            }

            getClientUtil().emptyQueue(modifyInputConnection.getId());
            return false;
        });

        // Generate another FlowFile with a sleep of 0 sec
        getClientUtil().stopProcessor(generate);
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("sleep", "0 sec"));
        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().startProcessor(generate);

        // Start processor and expect data to go to 'success'. This time the processor will not sleep in onTrigger because
        // it is configured to sleep only on the first iteration after an update.
        getClientUtil().startProcessor(modify);
        waitForQueueCount(successConnection.getId(), 1);
    }

    private File getCommonsLangJar() {
        final File bootstrapLib = new File(getNiFiInstance().getInstanceDirectory(), "lib/bootstrap");
        final File[] commonsLangJars = bootstrapLib.listFiles(file -> file.getName().startsWith("commons-lang"));
        if (commonsLangJars == null || commonsLangJars.length == 0) {
            throw new IllegalStateException("Could not find commons-lang jar in bootstrap lib directory");
        }

        if (commonsLangJars.length > 1) {
            throw new IllegalStateException("Found multiple commons-lang jars in bootstrap lib directory");
        }

        return commonsLangJars[0];
    }

    // We have several tests running the same flow but with different configuration. Since we need to reference the ProcessorEntities and ConnectionEntities, we have a method
    // that creates the flow and stores the entities are member variables
    private void createFlow() throws NiFiClientException, IOException {
        generate = getClientUtil().createProcessor("GenerateFlowFile");
        modify = getClientUtil().createProcessor("DynamicallyModifyClasspath");
        ProcessorEntity terminateSuccess = getClientUtil().createProcessor("TerminateFlowFile");
        ProcessorEntity terminateFailure = getClientUtil().createProcessor("TerminateFlowFile");

        modifyInputConnection = getClientUtil().createConnection(generate, modify, "success");
        successConnection = getClientUtil().createConnection(modify, terminateSuccess, "success");
        failureConnection = getClientUtil().createConnection(modify, terminateFailure, "failure");
    }
}
