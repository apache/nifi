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
package org.apache.nifi.tests.system.parameters;

import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * System test that verifies parameter updates succeed when a referencing processor is ghosted.
 *
 * When a processor is ghosted (its NAR is missing), all of its properties are treated as sensitive because
 * NiFi does not know the actual property descriptors. This means a non-sensitive parameter that was originally
 * referenced from a non-sensitive property now appears to be referenced from a sensitive property, which would
 * normally block any update to that parameter. The fix in StandardParameterContext skips validation for ghosted
 * components entirely, allowing the parameter to be updated.
 *
 * This test exercises the full lifecycle:
 * 1. A processor references a non-sensitive parameter via a non-sensitive property.
 * 2. The processor's NAR is removed and NiFi is restarted, causing the processor to become ghosted.
 * 3. While the processor is ghosted, the parameter value is updated (remaining non-sensitive).
 * 4. The NAR is restored and NiFi is restarted.
 * 5. The processor is no longer ghosted and can still be used with the updated parameter value.
 */
public class ParameterSensitivityWithGhostedComponentIT extends NiFiSystemIT {
    private static final Logger logger = LoggerFactory.getLogger(ParameterSensitivityWithGhostedComponentIT.class);

    private static final String TEST_EXTENSIONS_NAR_PREFIX = "nifi-system-test-extensions-nar";

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Test
    public void testParameterUpdateWhileProcessorGhosted() throws NiFiClientException, IOException, InterruptedException {
        // Create a parameter context with a non-sensitive parameter
        final ParameterContextEntity paramContext = getClientUtil().createParameterContext("TestContext", "myParam", "hello", false);
        final String paramContextId = paramContext.getId();

        // Create a CountEvents processor and set its non-sensitive "Name" property to reference the parameter
        final ProcessorEntity processor = getClientUtil().createProcessor("CountEvents");
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Name", "#{myParam}"));
        final String processorId = processor.getId();

        // Bind parameter context to root process group
        getClientUtil().setParameterContext("root", paramContext);

        // Verify processor is VALID
        getClientUtil().waitForValidProcessor(processorId);
        logger.info("Processor {} is VALID with non-sensitive parameter value 'hello'", processorId);

        // Stop NiFi and remove the test extensions NAR to ghost the processor
        final NiFiInstance nifiInstance = getNiFiInstance();
        nifiInstance.stop();

        final File instanceDir = nifiInstance.getInstanceDirectory();
        final File libDir = new File(instanceDir, "lib");
        final File narFile = findTestExtensionsNar(libDir);
        assertNotNull(narFile, "Could not find test extensions NAR in " + libDir.getAbsolutePath());

        final Path movedNarPath = instanceDir.toPath().resolve(narFile.getName() + ".removed");
        Files.move(narFile.toPath(), movedNarPath, StandardCopyOption.REPLACE_EXISTING);
        logger.info("Moved NAR {} to {} to simulate missing extension", narFile.getName(), movedNarPath);

        // Restart NiFi without the NAR
        nifiInstance.start();
        setupClient();

        // Verify the processor is now ghosted
        waitFor(() -> {
            final ProcessorEntity processorEntity = getNifiClient().getProcessorClient().getProcessor(processorId);
            return processorEntity.getComponent().getExtensionMissing();
        });
        logger.info("Processor {} is ghosted (extension missing)", processorId);

        // Update the parameter value while the processor is ghosted. Because the processor is ghosted, all of its
        // properties are treated as sensitive. Without the fix in StandardParameterContext, this update would fail
        // because the ghosted processor would appear to have a sensitive property referencing a non-sensitive parameter.
        final ParameterContextEntity currentParamContext = getNifiClient().getParamContextClient().getParamContext(paramContextId, false);
        final ParameterContextUpdateRequestEntity updateRequest = getClientUtil().updateParameterContext(currentParamContext, "myParam", "world");
        getClientUtil().waitForParameterContextRequestToComplete(paramContextId, updateRequest.getRequest().getRequestId());
        logger.info("Successfully updated parameter 'myParam' from 'hello' to 'world' while processor is ghosted");

        // Stop NiFi, restore the NAR, and restart
        nifiInstance.stop();
        Files.move(movedNarPath, narFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        logger.info("Restored NAR {} to {}", narFile.getName(), libDir.getAbsolutePath());

        nifiInstance.start();
        setupClient();

        // Verify the processor is no longer ghosted
        waitFor(() -> {
            final ProcessorEntity processorEntity = getNifiClient().getProcessorClient().getProcessor(processorId);
            return !processorEntity.getComponent().getExtensionMissing();
        });
        logger.info("Processor {} is no longer ghosted after NAR restore", processorId);

        // Verify the processor is VALID with the updated parameter value
        getClientUtil().waitForValidProcessor(processorId);
        final ProcessorEntity validProcessor = getNifiClient().getProcessorClient().getProcessor(processorId);
        assertFalse(validProcessor.getComponent().getExtensionMissing(), "Processor should not be ghosted");
        logger.info("Processor {} is VALID after NAR restore", processorId);

        // Verify we can still update the parameter now that the processor is no longer ghosted
        final ParameterContextEntity restoredParamContext = getNifiClient().getParamContextClient().getParamContext(paramContextId, false);
        final ParameterContextUpdateRequestEntity secondUpdate = getClientUtil().updateParameterContext(restoredParamContext, "myParam", "final-value");
        getClientUtil().waitForParameterContextRequestToComplete(paramContextId, secondUpdate.getRequest().getRequestId());
        logger.info("Successfully updated parameter 'myParam' to 'final-value' after processor is no longer ghosted");

        // Verify the processor is still VALID after the second parameter update
        getClientUtil().waitForValidProcessor(processorId);
    }

    private File findTestExtensionsNar(final File libDir) {
        final File[] narFiles = libDir.listFiles((dir, name) -> name.startsWith(TEST_EXTENSIONS_NAR_PREFIX) && name.endsWith(".nar"));
        if (narFiles == null || narFiles.length == 0) {
            return null;
        }
        return narFiles[0];
    }
}
