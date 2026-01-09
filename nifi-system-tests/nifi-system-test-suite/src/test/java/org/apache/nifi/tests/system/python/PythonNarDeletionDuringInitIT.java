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

package org.apache.nifi.tests.system.python;

import org.apache.nifi.nar.NarState;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.nar.NarUploadUtil;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for Python NAR deletion during processor initialization.
 * These tests verify that:
 * 1. Force-deleting a NAR while a Python processor is initializing does not block indefinitely
 * 2. The processor initialization is properly cancelled when the NAR is deleted
 * 3. The system remains in a consistent state after the operation
 */
public class PythonNarDeletionDuringInitIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(PythonNarDeletionDuringInitIT.class);

    private static final String PYTHON_TEXT_EXTENSIONS_NAR_ID = "nifi-python-test-extensions-nar";
    private static final String PYTHON_WRITE_BECH_32_CHARSET = "WriteBech32Charset";

    // Maximum time to wait for NAR deletion - should complete quickly with interruptibility fix
    private static final int NAR_DELETE_TIMEOUT_SECONDS = 30;

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createPythonicInstanceFactory();
    }


    /**
     * Tests that force-deleting a NAR immediately after creating a processor
     * does not block indefinitely. The deletion should complete within a reasonable time
     * even if the processor is still initializing (creating venv, downloading dependencies, etc.).
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testForceDeleteNarDuringProcessorInitialization() throws NiFiClientException, IOException, InterruptedException {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());

        // Get the Python NAR file
        final File pythonTestExtensionsNar = getPythonTestExtensionsNar();
        assertNotNull(pythonTestExtensionsNar, "Python test extensions NAR not found");

        // Upload the NAR and wait for it to be installed
        final NarSummaryDTO uploadedNarSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(uploadedNarSummary.getIdentifier(), NarState.INSTALLED));
        logger.info("NAR uploaded and installed: {}", uploadedNarSummary.getIdentifier());

        // Get the processor type info
        final DocumentedTypeDTO processorTypeDTO = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
        assertNotNull(processorTypeDTO, "Processor type not found after NAR installation");
        final BundleDTO processorBundle = processorTypeDTO.getBundle();

        // Create a Python processor - this triggers async initialization
        final ProcessorEntity pythonProcessor = getClientUtil().createProcessor(
                PYTHON_WRITE_BECH_32_CHARSET,
                processorBundle.getGroup(),
                processorBundle.getArtifact(),
                processorBundle.getVersion());
        logger.info("Created processor: {}", pythonProcessor.getId());

        // Immediately force-delete the NAR while the processor is still initializing
        // This should NOT block indefinitely - the initialization should be interruptible
        logger.info("Force-deleting NAR while processor is initializing...");

        final CompletableFuture<NarSummaryDTO> deleteFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return narUploadUtil.deleteNar(uploadedNarSummary);
            } catch (final Exception e) {
                throw new RuntimeException("Failed to delete NAR", e);
            }
        });

        // The deletion should complete within the timeout - this is the key assertion
        // Before the fix, this would block indefinitely waiting for venv creation to complete
        assertDoesNotThrow(() -> {
            try {
                final NarSummaryDTO deletedNar = deleteFuture.get(NAR_DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                logger.info("NAR deleted successfully: {}", deletedNar.getIdentifier());
            } catch (final TimeoutException e) {
                throw new AssertionError(
                        "NAR deletion blocked for more than " + NAR_DELETE_TIMEOUT_SECONDS + " seconds. " +
                        "This indicates the initialization was not properly interrupted.", e);
            } catch (final ExecutionException e) {
                throw new RuntimeException("NAR deletion failed with exception", e.getCause());
            }
        }, "NAR deletion should complete within " + NAR_DELETE_TIMEOUT_SECONDS + " seconds");

        // Verify the NAR is gone
        narUploadUtil.verifyNarSummaries(0);

        // Verify the processor type is no longer available
        assertNull(getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET),
                "Processor type should not be available after NAR deletion");

        // Verify the processor is now ghosted (extension missing)
        final String pythonProcessorId = pythonProcessor.getId();
        waitFor(() -> {
            final ProcessorEntity processorAfterDelete = getNifiClient().getProcessorClient().getProcessor(pythonProcessorId);
            logger.info("Waiting for processor {} to be considered missing", pythonProcessorId);
            return processorAfterDelete.getComponent().getExtensionMissing();
        });

        logger.info("Test completed successfully - NAR deletion did not block during processor initialization");
    }

    /**
     * Tests that after a NAR is force-deleted during initialization and then re-uploaded,
     * a new processor can be created and functions correctly.
     */
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testNarReuploadAfterForceDeleteDuringInit() throws NiFiClientException, IOException, InterruptedException {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());

        // Get the Python NAR file
        final File pythonTestExtensionsNar = getPythonTestExtensionsNar();
        assertNotNull(pythonTestExtensionsNar);

        // Phase 1: Upload NAR, create processor, force-delete NAR during init
        logger.info("Phase 1: Upload NAR, create processor, force-delete during init");

        final NarSummaryDTO uploadedNarSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(uploadedNarSummary.getIdentifier(), NarState.INSTALLED));

        final DocumentedTypeDTO processorTypeDTO = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
        final BundleDTO processorBundle = processorTypeDTO.getBundle();

        // Create processor (triggers initialization)
        final ProcessorEntity firstProcessor = getClientUtil().createProcessor(
                PYTHON_WRITE_BECH_32_CHARSET,
                processorBundle.getGroup(),
                processorBundle.getArtifact(),
                processorBundle.getVersion());

        // Immediately force-delete NAR
        final CompletableFuture<NarSummaryDTO> deleteFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return narUploadUtil.deleteNar(uploadedNarSummary);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Wait for deletion (should not timeout)
        try {
            deleteFuture.get(NAR_DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final TimeoutException | ExecutionException e) {
            throw new AssertionError("NAR deletion should not block during processor initialization", e);
        }

        // Wait for the first processor to become ghosted
        final String firstProcessorId = firstProcessor.getId();
        waitFor(() -> {
            final ProcessorEntity proc = getNifiClient().getProcessorClient().getProcessor(firstProcessorId);
            return proc.getComponent().getExtensionMissing();
        });

        // Delete the ghosted processor to clean up
        getNifiClient().getProcessorClient().deleteProcessor(firstProcessor);

        // Phase 2: Re-upload NAR and verify new processor works
        logger.info("Phase 2: Re-upload NAR and verify new processor works");

        final NarSummaryDTO reuploadedNarSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(reuploadedNarSummary.getIdentifier(), NarState.INSTALLED));

        // Verify processor type is available again
        final DocumentedTypeDTO reloadedProcessorType = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
        assertNotNull(reloadedProcessorType, "Processor type should be available after NAR re-upload");

        // Create a new processor and verify it initializes successfully
        final ProcessorEntity secondProcessor = getClientUtil().createProcessor(
                PYTHON_WRITE_BECH_32_CHARSET,
                reloadedProcessorType.getBundle().getGroup(),
                reloadedProcessorType.getBundle().getArtifact(),
                reloadedProcessorType.getBundle().getVersion());

        // Wait for the processor to be loaded (not ghosted) - we don't need it to be fully valid
        // since that would require wiring up connections
        final String secondProcessorId = secondProcessor.getId();
        waitFor(() -> {
            final ProcessorEntity proc = getNifiClient().getProcessorClient().getProcessor(secondProcessorId);
            // Processor is loaded when it's not marked as extension missing
            return !proc.getComponent().getExtensionMissing();
        });

        // Verify the processor is not ghosted
        final ProcessorEntity loadedProcessor = getNifiClient().getProcessorClient().getProcessor(secondProcessorId);
        assertFalse(loadedProcessor.getComponent().getExtensionMissing(),
                "Processor should not be marked as extension missing after NAR re-upload");

        // Clean up
        getNifiClient().getProcessorClient().deleteProcessor(loadedProcessor);
        narUploadUtil.deleteNar(reuploadedNarSummary);

        logger.info("Test completed successfully - NAR re-upload after force-delete works correctly");
    }

    /**
     * Tests rapid NAR upload/delete cycles to verify the system remains stable
     * and doesn't accumulate orphaned resources.
     */
    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    public void testRapidNarUploadDeleteCycles() throws NiFiClientException, IOException, InterruptedException {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());
        final File pythonTestExtensionsNar = getPythonTestExtensionsNar();
        assertNotNull(pythonTestExtensionsNar);

        final int cycles = 3;
        logger.info("Running {} rapid NAR upload/delete cycles", cycles);

        for (int i = 0; i < cycles; i++) {
            logger.info("Cycle {} of {}", i + 1, cycles);

            // Upload NAR
            final NarSummaryDTO narSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
            waitFor(narUploadUtil.getWaitForNarStateSupplier(narSummary.getIdentifier(), NarState.INSTALLED));

            final DocumentedTypeDTO processorType = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
            assertNotNull(processorType, "Processor type should be available");

            // Create processor
            final ProcessorEntity processor = getClientUtil().createProcessor(
                    PYTHON_WRITE_BECH_32_CHARSET,
                    processorType.getBundle().getGroup(),
                    processorType.getBundle().getArtifact(),
                    processorType.getBundle().getVersion());

            // Force-delete NAR immediately
            final CompletableFuture<NarSummaryDTO> deleteFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return narUploadUtil.deleteNar(narSummary);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            });

            // Verify deletion completes without blocking
            try {
                deleteFuture.get(NAR_DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final TimeoutException e) {
                throw new AssertionError("Cycle " + (i + 1) + ": NAR deletion blocked", e);
            } catch (final ExecutionException e) {
                throw new RuntimeException("Cycle " + (i + 1) + ": NAR deletion failed", e.getCause());
            }

            // Wait for processor to become ghosted
            final String processorId = processor.getId();
            waitFor(() -> {
                final ProcessorEntity proc = getNifiClient().getProcessorClient().getProcessor(processorId);
                return proc.getComponent().getExtensionMissing();
            });

            // Clean up the ghosted processor
            getNifiClient().getProcessorClient().deleteProcessor(processor);

            // Verify no NARs remain
            assertEquals(0, narUploadUtil.verifyNarSummaries(0).getNarSummaries().size());
        }

        logger.info("All {} cycles completed successfully", cycles);
    }

    /**
     * Tests that the processor load state transitions correctly when initialization is cancelled.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testProcessorLoadStateDuringCancellation() throws NiFiClientException, IOException, InterruptedException {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());
        final File pythonTestExtensionsNar = getPythonTestExtensionsNar();
        assertNotNull(pythonTestExtensionsNar);

        // Upload NAR
        final NarSummaryDTO narSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(narSummary.getIdentifier(), NarState.INSTALLED));

        final DocumentedTypeDTO processorType = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
        final ProcessorEntity processor = getClientUtil().createProcessor(
                PYTHON_WRITE_BECH_32_CHARSET,
                processorType.getBundle().getGroup(),
                processorType.getBundle().getArtifact(),
                processorType.getBundle().getVersion());

        // Check initial state - should be in one of the initialization states
        final ProcessorEntity processorBeforeDelete = getNifiClient().getProcessorClient().getProcessor(processor.getId());
        final String validationStatus = processorBeforeDelete.getComponent().getValidationStatus();
        logger.info("Processor validation status before NAR delete: {}", validationStatus);

        // The processor should be VALIDATING or INVALID while initializing
        assertTrue(validationStatus.equals("VALIDATING") || validationStatus.equals("INVALID"),
                "Expected VALIDATING or INVALID status during initialization, got: " + validationStatus);

        // Force-delete the NAR
        final CompletableFuture<NarSummaryDTO> deleteFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return narUploadUtil.deleteNar(narSummary);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            deleteFuture.get(NAR_DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final TimeoutException | ExecutionException e) {
            throw new AssertionError("NAR deletion should complete promptly", e);
        }

        // After NAR deletion, the processor should be ghosted
        waitFor(() -> {
            final ProcessorEntity proc = getNifiClient().getProcessorClient().getProcessor(processor.getId());
            return proc.getComponent().getExtensionMissing();
        });

        final ProcessorEntity processorAfterDelete = getNifiClient().getProcessorClient().getProcessor(processor.getId());
        assertTrue(processorAfterDelete.getComponent().getExtensionMissing(),
                "Processor should be marked as extension missing after NAR deletion");

        logger.info("Test completed - processor transitioned to ghosted state correctly");
    }

    private File getPythonTestExtensionsNar() {
        final File nifiHome = getNiFiInstance().getInstanceDirectory();
        final File lib = new File(nifiHome, "lib");
        final File pythonNars = new File(lib, "python-nars");
        final File[] narFiles = pythonNars.listFiles((dir, name) ->
                name.endsWith(".nar") && name.startsWith(PYTHON_TEXT_EXTENSIONS_NAR_ID));

        if (narFiles == null || narFiles.length == 0) {
            return null;
        }
        return narFiles[0];
    }

    private DocumentedTypeDTO getDocumentedTypeDTO(final String type) throws NiFiClientException, IOException {
        final ProcessorTypesEntity allProcessorTypes = getNifiClient().getFlowClient().getProcessorTypes();
        if (allProcessorTypes == null || allProcessorTypes.getProcessorTypes() == null) {
            return null;
        }

        return allProcessorTypes.getProcessorTypes().stream()
                .filter(processorType -> processorType.getType().equals(type))
                .findAny()
                .orElse(null);
    }
}

