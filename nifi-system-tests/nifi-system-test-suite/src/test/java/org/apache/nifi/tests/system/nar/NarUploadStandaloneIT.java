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
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.NarCoordinateDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.ControllerServiceTypesEntity;
import org.apache.nifi.web.api.entity.NarDetailsEntity;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NarUploadStandaloneIT extends NiFiSystemIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(NarUploadStandaloneIT.class);

    private static final String NARS_LOCATION = "target/nifi-nar-provider-nars";
    private static final String PROCESSORS_NAR_ID = "nifi-nar-provider-processors-nar";
    private static final String CONTROLLER_SERVICE_API_NAR_ID = "nifi-nar-provider-service-api-nar";
    private static final String CONTROLLER_SERVICE_NAR_ID = "nifi-nar-provider-service-nar";

    @Test
    public void testUploadNars() throws NiFiClientException, IOException, InterruptedException {
        verifyNarSummaries(0);

        final File narsLocation = new File(NARS_LOCATION);
        if (!narsLocation.exists()) {
            throw new IllegalStateException("NARs location does not exist at: " + narsLocation.getAbsolutePath());
        }

        // Upload processor NAR and wait until is reaches Missing Dependency
        final NarSummaryDTO uploadedProcessorsNar = uploadNar(narsLocation, PROCESSORS_NAR_ID);
        waitForNarState(uploadedProcessorsNar.getIdentifier(), NarState.MISSING_DEPENDENCY);

        // Upload controller service NAR and wait until is reaches Missing Dependency
        final NarSummaryDTO uploadedControllerServiceNar = uploadNar(narsLocation, CONTROLLER_SERVICE_NAR_ID);
        waitForNarState(uploadedControllerServiceNar.getIdentifier(), NarState.MISSING_DEPENDENCY);

        // Upload controller service API NAR and wait until is reaches Installed, will cause previous two NARs to also install
        final NarSummaryDTO uploadedControllerServiceApiNar = uploadNar(narsLocation, CONTROLLER_SERVICE_API_NAR_ID);
        waitForNarState(uploadedControllerServiceApiNar.getIdentifier(), NarState.INSTALLED);

        // Verify all NARs are now installed
        final NarSummariesEntity narSummaries = verifyNarSummaries(3);
        for (final NarSummaryEntity narSummaryEntity : narSummaries.getNarSummaries()) {
            assertEquals(NarState.INSTALLED.getValue(), narSummaryEntity.getNarSummary().getState());
        }

        // Verify processors from the uploaded NAR are loaded and available to use
        final ProcessorTypesEntity allProcessorTypes = getNifiClient().getFlowClient().getProcessorTypes();
        assertNotNull(allProcessorTypes);
        assertNotNull(allProcessorTypes.getProcessorTypes());

        final Set<DocumentedTypeDTO> processorsFromUploadedNar = allProcessorTypes.getProcessorTypes().stream()
                .filter(processorType -> matchingBundles(processorType.getBundle(), uploadedProcessorsNar.getCoordinate()))
                .collect(Collectors.toSet());
        assertNotNull(processorsFromUploadedNar);
        assertFalse(processorsFromUploadedNar.isEmpty());

        // Verify controller services from the uploaded NAR are loaded and available to use
        final ControllerServiceTypesEntity allControllerServiceTypes = getNifiClient().getFlowClient().getControllerServiceTypes();
        assertNotNull(allControllerServiceTypes);
        assertNotNull(allControllerServiceTypes.getControllerServiceTypes());

        final Set<DocumentedTypeDTO> controllerServicesFromUploadedNar = allControllerServiceTypes.getControllerServiceTypes().stream()
                .filter(serviceType -> matchingBundles(serviceType.getBundle(), uploadedControllerServiceNar.getCoordinate()))
                .collect(Collectors.toSet());
        assertNotNull(controllerServicesFromUploadedNar);
        assertFalse(controllerServicesFromUploadedNar.isEmpty());

        // Verify retrieving NAR details
        final NarDetailsEntity narDetails = getNifiClient().getControllerClient().getNarDetails(uploadedProcessorsNar.getIdentifier());
        assertNotNull(narDetails);
        assertNotNull(narDetails.getNarSummary());
        assertNotNull(narDetails.getProcessorTypes());
        assertEquals(1, narDetails.getProcessorTypes().size());

        // Verify service API NAR can't be replaced while other NARs depend on it
        assertThrows(NiFiClientException.class, () -> uploadNar(narsLocation, CONTROLLER_SERVICE_API_NAR_ID));

        // Verify processors NAR can be replaced
        final NarSummaryDTO replacedProcessorsNar = uploadNar(narsLocation, PROCESSORS_NAR_ID);
        waitForNarState(replacedProcessorsNar.getIdentifier(), NarState.INSTALLED);

        // Verify controller services NAR can be replaced
        final NarSummaryDTO replacedControllerServiceNar = uploadNar(narsLocation, CONTROLLER_SERVICE_NAR_ID);
        waitForNarState(replacedControllerServiceNar.getIdentifier(), NarState.INSTALLED);

        // Verify service API NAR can't be deleted while other NARs depend on it
        assertThrows(NiFiClientException.class, () -> getNifiClient().getControllerClient().deleteNar(uploadedControllerServiceApiNar.getIdentifier(), true));

        // Delete other NARs first
        deleteNar(uploadedProcessorsNar);
        deleteNar(uploadedControllerServiceNar);
        deleteNar(uploadedControllerServiceApiNar);

        // Verify no NARs exist
        verifyNarSummaries(0);
    }

    private boolean matchingBundles(final BundleDTO bundleDTO, final NarCoordinateDTO narCoordinateDTO) {
        return bundleDTO.getGroup().equals(narCoordinateDTO.getGroup())
                && bundleDTO.getArtifact().equals(narCoordinateDTO.getArtifact())
                && bundleDTO.getVersion().equals(narCoordinateDTO.getVersion());
    }

    private NarSummariesEntity verifyNarSummaries(final int expectedCount) throws NiFiClientException, IOException {
        final NarSummariesEntity narSummariesEntity = getNifiClient().getControllerClient().getNarSummaries();
        assertNotNull(narSummariesEntity);
        assertNotNull(narSummariesEntity.getNarSummaries());
        assertNotNull(narSummariesEntity.getCurrentTime());
        assertEquals(expectedCount, narSummariesEntity.getNarSummaries().size());
        return narSummariesEntity;
    }

    private NarSummaryDTO uploadNar(final File narsLocation, final String narId) throws IOException, NiFiClientException {
        final File narFile = Arrays.stream(Objects.requireNonNull(narsLocation.listFiles(file -> file.getName().startsWith(narId) && file.getName().endsWith(".nar"))))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Unable to find NAR with id " + narId + " in " + narsLocation.getAbsolutePath()));
        assertNotNull(narFile);

        try (final InputStream inputStream = new FileInputStream(narFile)) {
            final NarSummaryEntity narSummaryEntity = getNifiClient().getControllerClient().uploadNar(narFile.getName(), inputStream);
            assertNotNull(narSummaryEntity);

            final NarSummaryDTO narSummaryDTO = narSummaryEntity.getNarSummary();
            assertNotNull(narSummaryDTO);
            assertNotNull(narSummaryDTO.getIdentifier());
            assertNotNull(narSummaryDTO.getCoordinate());
            assertNotNull(narSummaryDTO.getState());
            return narSummaryEntity.getNarSummary();
        }
    }

    private void waitForNarState(final String identifier, final NarState narState) throws InterruptedException {
        waitFor(() -> {
            LOGGER.info("Waiting for NAR [{}] to be in state [{}]]", identifier, narState.getValue());
            final NarSummaryEntity narSummaryEntity = getNifiClient().getControllerClient().getNarSummary(identifier);
            return narSummaryEntity != null && narSummaryEntity.getNarSummary() != null && narState.getValue().equals(narSummaryEntity.getNarSummary().getState());
        });
    }

    private NarSummaryDTO deleteNar(final NarSummaryDTO narSummary) throws NiFiClientException, IOException {
        final NarSummaryEntity deletedNar = getNifiClient().getControllerClient().deleteNar(narSummary.getIdentifier(), true);
        assertNotNull(deletedNar);
        assertNotNull(deletedNar.getNarSummary());
        assertEquals(narSummary.getIdentifier(), deletedNar.getNarSummary().getIdentifier());
        return deletedNar.getNarSummary();
    }
}
