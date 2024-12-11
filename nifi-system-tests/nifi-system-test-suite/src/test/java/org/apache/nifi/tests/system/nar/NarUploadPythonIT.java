/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.tests.system.nar;

import org.apache.nifi.nar.NarState;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NarUploadPythonIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(NarUploadPythonIT.class);

    private static final String PYTHON_TEXT_EXTENSIONS_NAR_ID = "nifi-python-test-extensions-nar";
    private static final String PYTHON_WRITE_BECH_32_CHARSET = "WriteBech32Charset";
    private static final String EXPECTED_FLOW_FILE_CONTENT = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createPythonicInstanceFactory();
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Test
    public void testUploadPythonNar() throws NiFiClientException, IOException, InterruptedException {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());

        final File nifiHome = getNiFiInstance().getInstanceDirectory();
        final File lib = new File(nifiHome, "lib");
        final File pythonNars = new File(lib, "python-nars");
        final File[] narFiles = pythonNars.listFiles((dir, name) -> name.endsWith(".nar") && name.startsWith(PYTHON_TEXT_EXTENSIONS_NAR_ID));
        assertNotNull(narFiles);
        assertEquals(1, narFiles.length);

        // Upload Python NAR
        final File pythonTestExtensionsNar = narFiles[0];
        final NarSummaryDTO uploadedNarSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(uploadedNarSummary.getIdentifier(), NarState.INSTALLED));

        // Verify all NARs are now installed
        final NarSummariesEntity narSummaries = narUploadUtil.verifyNarSummaries(1);
        for (final NarSummaryEntity narSummaryEntity : narSummaries.getNarSummaries()) {
            assertEquals(NarState.INSTALLED.getValue(), narSummaryEntity.getNarSummary().getState());
        }

        // Verify the Python NAR is in the listing
        final NarSummaryEntity matchingSummary = narSummaries.getNarSummaries().stream()
                .filter(summary -> summary.getNarSummary().getIdentifier().equals(uploadedNarSummary.getIdentifier()))
                .findAny()
                .orElse(null);
        assertNotNull(matchingSummary);

        // Verify processor from the uploaded NAR is loaded and available to use
        final DocumentedTypeDTO processorTypeFromUploadedNar = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
        assertNotNull(processorTypeFromUploadedNar);

        final BundleDTO processorBundle = processorTypeFromUploadedNar.getBundle();
        logger.info("{} - {} - {}", processorBundle.getGroup(), processorBundle.getArtifact(), processorBundle.getVersion());

        // Create instance of the WriteBech32Charset processor, and connect a GenerateFlowFile to it
        final ProcessorEntity generateProcessor = getClientUtil().createProcessor("GenerateFlowFile");
        ProcessorEntity pythonProcessor = getClientUtil().createProcessor(PYTHON_WRITE_BECH_32_CHARSET, processorBundle.getGroup(), processorBundle.getArtifact(), processorBundle.getVersion());
        pythonProcessor = getClientUtil().setAutoTerminatedRelationships(pythonProcessor, "failure");
        final ConnectionEntity generateToWriteVersion = getClientUtil().createConnection(generateProcessor, pythonProcessor, "success");

        // Create a TerminateFlowFile processor and connect the WriteNumpyVersionBundledDependency processor to it
        final ProcessorEntity terminateProcessor = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity writeVersionToTerminate = getClientUtil().createConnection(pythonProcessor, terminateProcessor, "success");

        // Wait for processor validation to complete
        getClientUtil().waitForValidProcessor(generateProcessor.getId());
        getClientUtil().waitForValidProcessor(pythonProcessor.getId());

        // Run the flow
        getClientUtil().startProcessor(generateProcessor);
        waitForQueueCount(generateToWriteVersion.getId(), 1);
        getClientUtil().startProcessor(pythonProcessor);
        waitForQueueCount(writeVersionToTerminate.getId(), 1);

        // Verify the output
        final String contents = getClientUtil().getFlowFileContentAsUtf8(writeVersionToTerminate.getId(), 0);
        // Ensure that the contents written to the FlowFile are the 32 characters used by the bech32 encoding
        assertEquals(EXPECTED_FLOW_FILE_CONTENT, contents);

        // Verify can't delete Python NAR when component exists without force
        assertThrows(NiFiClientException.class, () -> getNifiClient().getControllerClient().deleteNar(uploadedNarSummary.getIdentifier(), false));

        // Delete the Python NAR
        narUploadUtil.deleteNar(uploadedNarSummary);
        narUploadUtil.verifyNarSummaries(0);

        // Verify type is no longer available
        assertNull(getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET));

        // Verify existing processor is ghosted
        final String pythonProcessorId = pythonProcessor.getId();
        waitFor(() -> {
            final ProcessorEntity pythonProcessorAfterDelete = getNifiClient().getProcessorClient().getProcessor(pythonProcessorId);
            logger.info("Waiting for processor {} to be considered missing", pythonProcessorId);
            return pythonProcessorAfterDelete.getComponent().getExtensionMissing();
        });

        // Restore NAR
        final NarSummaryDTO restoredNarSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(restoredNarSummary.getIdentifier(), NarState.INSTALLED));

        // Verify existing processor is un-ghosted
        final ProcessorEntity pythonProcessorAfterRestore = getNifiClient().getProcessorClient().getProcessor(pythonProcessor.getId());
        assertNotNull(pythonProcessorAfterRestore );
        assertNotNull(pythonProcessorAfterRestore .getComponent());
        assertFalse(pythonProcessorAfterRestore .getComponent().getExtensionMissing());

        // Run the flow again
        getClientUtil().waitForValidProcessor(pythonProcessor.getId());
        getClientUtil().startProcessor(generateProcessor);
        getClientUtil().startProcessor(pythonProcessor);
        waitForQueueCount(writeVersionToTerminate.getId(), 1);

        // Verify the output
        final String contentsAfterRestore = getClientUtil().getFlowFileContentAsUtf8(writeVersionToTerminate.getId(), 0);
        assertEquals(EXPECTED_FLOW_FILE_CONTENT, contentsAfterRestore);
    }

    private DocumentedTypeDTO getDocumentedTypeDTO(final String type) throws NiFiClientException, IOException {
        final ProcessorTypesEntity allProcessorTypes = getNifiClient().getFlowClient().getProcessorTypes();
        assertNotNull(allProcessorTypes);
        assertNotNull(allProcessorTypes.getProcessorTypes());

        return allProcessorTypes.getProcessorTypes().stream()
                .filter(processorType -> processorType.getType().equals(type))
                .findAny()
                .orElse(null);
    }
}
