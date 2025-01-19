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

class DefaultedDynamicClassPathModificationIT extends NiFiSystemIT {

    private ProcessorEntity generateFlowFileProcessor;
    private ProcessorEntity defaultedModifyClasspathProcessor;

    private ConnectionEntity defaultedModifyClasspathInputConnection;
    private ConnectionEntity successConnection;

    @Test
    void testLoadsClassFromDefaultedDynamicModification() throws NiFiClientException, IOException, InterruptedException {
        createFlow();

        // Update modify to have the appropriate URL, don't update URL to load to let it on default value
        final Map<String, String> propertyMap = new HashMap<>();
        propertyMap.put("Class to Load", "org.apache.commons.lang3.StringUtils");
        getClientUtil().updateProcessorProperties(defaultedModifyClasspathProcessor, propertyMap);
        getClientUtil().waitForValidProcessor(defaultedModifyClasspathProcessor.getId());

        // Create a FlowFile
        getClientUtil().waitForValidProcessor(generateFlowFileProcessor.getId());
        getClientUtil().startProcessor(generateFlowFileProcessor);
        waitForQueueCount(defaultedModifyClasspathInputConnection.getId(), 1);

        // Wait for a FlowFile to be routed to success
        getClientUtil().startProcessor(defaultedModifyClasspathProcessor);
        waitForQueueCount(successConnection.getId(), 1);

        getClientUtil().stopProcessor(generateFlowFileProcessor);
        getClientUtil().waitForStoppedProcessor(generateFlowFileProcessor.getId());

        // Restart and ensure that everything works as expected after restart
        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        // Feed another FlowFile through. Upon restart, in order to modify, we need to get the most up-to-date revision so will first fetch the Processor
        final ProcessorEntity generateAfterRestart = getNifiClient().getProcessorClient().getProcessor(generateFlowFileProcessor.getId());
        getClientUtil().waitForValidProcessor(generateAfterRestart.getId());
        getClientUtil().startProcessor(generateAfterRestart);

        // Depending on whether or not the flow was written out with the processor running, the Modify processor may or may not be running. Ensure that it is running.
        getClientUtil().waitForValidationCompleted(defaultedModifyClasspathProcessor);
        final ProcessorEntity modifyAfterRestart = getNifiClient().getProcessorClient().getProcessor(defaultedModifyClasspathProcessor.getId());
        final String modifyRunStatus = modifyAfterRestart.getStatus().getRunStatus();
        if (!"Running".equalsIgnoreCase(modifyRunStatus)) {
            getClientUtil().startProcessor(modifyAfterRestart);
        }

        // We now expect 2 FlowFiles to be in the success route
        waitForQueueCount(successConnection.getId(), 2);
    }

    // We have several tests running the same flow but with different configuration. Since we need to reference the ProcessorEntities and ConnectionEntities, we have a method
    // that creates the flow and stores the entities are member variables
    private void createFlow() throws NiFiClientException, IOException {
        generateFlowFileProcessor = getClientUtil().createProcessor("GenerateFlowFile");
        defaultedModifyClasspathProcessor = getClientUtil().createProcessor("DefaultedDynamicallyModifyClasspath");
        ProcessorEntity terminateSuccess = getClientUtil().createProcessor("TerminateFlowFile");
        ProcessorEntity terminateFailure = getClientUtil().createProcessor("TerminateFlowFile");

        // Find the commons-lang3 jar that is in the bootstrap directory and set the processor to use it.
        final File lib = new File(getNiFiInstance().getInstanceDirectory(), "lib");
        final File bootstrapLib = new File(lib, "bootstrap");
        final File[] listing = bootstrapLib.listFiles(file -> file.getName().endsWith(".jar") && file.getName().startsWith("commons-lang3"));
        if (listing != null && listing.length >= 1) {
            final File lang3Jar = listing[0];
            defaultedModifyClasspathProcessor = getClientUtil().updateProcessorProperties(defaultedModifyClasspathProcessor, Collections.singletonMap("URLs to Load", lang3Jar.getAbsolutePath()));
        }

        defaultedModifyClasspathInputConnection = getClientUtil().createConnection(generateFlowFileProcessor, defaultedModifyClasspathProcessor, "success");
        successConnection = getClientUtil().createConnection(defaultedModifyClasspathProcessor, terminateSuccess, "success");
        getClientUtil().createConnection(defaultedModifyClasspathProcessor, terminateFailure, "failure");
    }
}
