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

public class PythonNarDeletionDuringInitIT extends NiFiSystemIT {

    private static final String PYTHON_TEXT_EXTENSIONS_NAR_ID = "nifi-python-test-extensions-nar";
    private static final String PYTHON_WRITE_BECH_32_CHARSET = "WriteBech32Charset";

    private static final int NAR_DELETE_TIMEOUT_SECONDS = 30;

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createPythonicInstanceFactory();
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testForceDeleteNarDuringProcessorInitialization() throws NiFiClientException, IOException, InterruptedException {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());

        final File pythonTestExtensionsNar = getPythonTestExtensionsNar();
        assertNotNull(pythonTestExtensionsNar);

        final NarSummaryDTO uploadedNarSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(uploadedNarSummary.getIdentifier(), NarState.INSTALLED));

        final DocumentedTypeDTO processorTypeDTO = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
        assertNotNull(processorTypeDTO);
        final BundleDTO processorBundle = processorTypeDTO.getBundle();

        final ProcessorEntity pythonProcessor = getClientUtil().createProcessor(
                PYTHON_WRITE_BECH_32_CHARSET,
                processorBundle.getGroup(),
                processorBundle.getArtifact(),
                processorBundle.getVersion());

        final CompletableFuture<NarSummaryDTO> deleteFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return narUploadUtil.deleteNar(uploadedNarSummary);
            } catch (final Exception e) {
                throw new RuntimeException("Failed to delete NAR", e);
            }
        });

        assertDoesNotThrow(() -> {
            try {
                deleteFuture.get(NAR_DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final TimeoutException e) {
                throw new AssertionError(
                        "NAR deletion blocked for more than %d seconds".formatted(NAR_DELETE_TIMEOUT_SECONDS), e);
            } catch (final ExecutionException e) {
                throw new RuntimeException("NAR deletion failed with exception", e.getCause());
            }
        });

        narUploadUtil.verifyNarSummaries(0);

        assertNull(getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET));

        final String pythonProcessorId = pythonProcessor.getId();
        waitFor(() -> {
            final ProcessorEntity processorAfterDelete = getNifiClient().getProcessorClient().getProcessor(pythonProcessorId);
            return processorAfterDelete.getComponent().getExtensionMissing();
        });
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    public void testNarReuploadAfterForceDeleteDuringInit() throws NiFiClientException, IOException, InterruptedException {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());

        final File pythonTestExtensionsNar = getPythonTestExtensionsNar();
        assertNotNull(pythonTestExtensionsNar);

        final NarSummaryDTO uploadedNarSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(uploadedNarSummary.getIdentifier(), NarState.INSTALLED));

        final DocumentedTypeDTO processorTypeDTO = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
        final BundleDTO processorBundle = processorTypeDTO.getBundle();

        final ProcessorEntity firstProcessor = getClientUtil().createProcessor(
                PYTHON_WRITE_BECH_32_CHARSET,
                processorBundle.getGroup(),
                processorBundle.getArtifact(),
                processorBundle.getVersion());

        final CompletableFuture<NarSummaryDTO> deleteFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return narUploadUtil.deleteNar(uploadedNarSummary);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });

        try {
            deleteFuture.get(NAR_DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final TimeoutException | ExecutionException e) {
            throw new AssertionError("NAR deletion should not block during processor initialization", e);
        }

        final String firstProcessorId = firstProcessor.getId();
        waitFor(() -> {
            final ProcessorEntity proc = getNifiClient().getProcessorClient().getProcessor(firstProcessorId);
            return proc.getComponent().getExtensionMissing();
        });

        getNifiClient().getProcessorClient().deleteProcessor(firstProcessor);

        final NarSummaryDTO reuploadedNarSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(reuploadedNarSummary.getIdentifier(), NarState.INSTALLED));

        final DocumentedTypeDTO reloadedProcessorType = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
        assertNotNull(reloadedProcessorType);

        final ProcessorEntity secondProcessor = getClientUtil().createProcessor(
                PYTHON_WRITE_BECH_32_CHARSET,
                reloadedProcessorType.getBundle().getGroup(),
                reloadedProcessorType.getBundle().getArtifact(),
                reloadedProcessorType.getBundle().getVersion());

        final String secondProcessorId = secondProcessor.getId();
        waitFor(() -> {
            final ProcessorEntity proc = getNifiClient().getProcessorClient().getProcessor(secondProcessorId);
            return !proc.getComponent().getExtensionMissing();
        });

        final ProcessorEntity loadedProcessor = getNifiClient().getProcessorClient().getProcessor(secondProcessorId);
        assertFalse(loadedProcessor.getComponent().getExtensionMissing());

        getNifiClient().getProcessorClient().deleteProcessor(loadedProcessor);
        narUploadUtil.deleteNar(reuploadedNarSummary);
    }

    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    public void testRapidNarUploadDeleteCycles() throws NiFiClientException, IOException, InterruptedException {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());
        final File pythonTestExtensionsNar = getPythonTestExtensionsNar();
        assertNotNull(pythonTestExtensionsNar);

        final int cycles = 3;

        for (int i = 0; i < cycles; i++) {
            final NarSummaryDTO narSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
            waitFor(narUploadUtil.getWaitForNarStateSupplier(narSummary.getIdentifier(), NarState.INSTALLED));

            final DocumentedTypeDTO processorType = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
            assertNotNull(processorType);

            final ProcessorEntity processor = getClientUtil().createProcessor(
                    PYTHON_WRITE_BECH_32_CHARSET,
                    processorType.getBundle().getGroup(),
                    processorType.getBundle().getArtifact(),
                    processorType.getBundle().getVersion());

            final CompletableFuture<NarSummaryDTO> deleteFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return narUploadUtil.deleteNar(narSummary);
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            });

            try {
                deleteFuture.get(NAR_DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final TimeoutException e) {
                throw new AssertionError("Cycle %d: NAR deletion blocked".formatted(i + 1), e);
            } catch (final ExecutionException e) {
                throw new RuntimeException("Cycle %d: NAR deletion failed".formatted(i + 1), e.getCause());
            }

            final String processorId = processor.getId();
            waitFor(() -> {
                final ProcessorEntity proc = getNifiClient().getProcessorClient().getProcessor(processorId);
                return proc.getComponent().getExtensionMissing();
            });

            getNifiClient().getProcessorClient().deleteProcessor(processor);

            assertEquals(0, narUploadUtil.verifyNarSummaries(0).getNarSummaries().size());
        }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    public void testProcessorLoadStateDuringCancellation() throws NiFiClientException, IOException, InterruptedException {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());
        final File pythonTestExtensionsNar = getPythonTestExtensionsNar();
        assertNotNull(pythonTestExtensionsNar);

        final NarSummaryDTO narSummary = narUploadUtil.uploadNar(pythonTestExtensionsNar);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(narSummary.getIdentifier(), NarState.INSTALLED));

        final DocumentedTypeDTO processorType = getDocumentedTypeDTO(PYTHON_WRITE_BECH_32_CHARSET);
        final ProcessorEntity processor = getClientUtil().createProcessor(
                PYTHON_WRITE_BECH_32_CHARSET,
                processorType.getBundle().getGroup(),
                processorType.getBundle().getArtifact(),
                processorType.getBundle().getVersion());

        final ProcessorEntity processorBeforeDelete = getNifiClient().getProcessorClient().getProcessor(processor.getId());
        final String validationStatus = processorBeforeDelete.getComponent().getValidationStatus();

        assertTrue(validationStatus.equals("VALIDATING") || validationStatus.equals("INVALID"));

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

        waitFor(() -> {
            final ProcessorEntity proc = getNifiClient().getProcessorClient().getProcessor(processor.getId());
            return proc.getComponent().getExtensionMissing();
        });

        final ProcessorEntity processorAfterDelete = getNifiClient().getProcessorClient().getProcessor(processor.getId());
        assertTrue(processorAfterDelete.getComponent().getExtensionMissing());
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

