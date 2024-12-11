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
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.NarCoordinateDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.ControllerServiceTypesEntity;
import org.apache.nifi.web.api.entity.NarDetailsEntity;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NarUploadStandaloneIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(NarUploadStandaloneIT.class);

    private static final String NAR_PROVIDER_NARS_LOCATION = "target/nifi-nar-provider-nars";
    private static final String PROCESSORS_NAR_ID = "nifi-nar-provider-processors-nar";
    private static final String PROCESSOR_CLASS_NAME = "org.apache.nifi.nar.provider.GetClassLoaderInfo";
    private static final String CONTROLLER_SERVICE_API_NAR_ID = "nifi-nar-provider-service-api-nar";
    private static final String CONTROLLER_SERVICE_NAR_ID = "nifi-nar-provider-service-nar";

    @Test
    public void testUploadStandardNars() throws NiFiClientException, IOException, InterruptedException {
        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());
        narUploadUtil.verifyNarSummaries(0);

        final File narsLocation = new File(NAR_PROVIDER_NARS_LOCATION);
        if (!narsLocation.exists()) {
            throw new IllegalStateException("NARs location does not exist at: " + narsLocation.getAbsolutePath());
        }

        // Upload processor NAR and wait until is reaches Missing Dependency
        final NarSummaryDTO uploadedProcessorsNar = narUploadUtil.uploadNar(narsLocation, PROCESSORS_NAR_ID);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(uploadedProcessorsNar.getIdentifier(), NarState.MISSING_DEPENDENCY));

        // Upload controller service NAR and wait until is reaches Missing Dependency
        final NarSummaryDTO uploadedControllerServiceNar = narUploadUtil.uploadNar(narsLocation, CONTROLLER_SERVICE_NAR_ID);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(uploadedControllerServiceNar.getIdentifier(), NarState.MISSING_DEPENDENCY));

        // Upload controller service API NAR and wait until is reaches Installed, will cause previous two NARs to also install
        final NarSummaryDTO uploadedControllerServiceApiNar = narUploadUtil.uploadNar(narsLocation, CONTROLLER_SERVICE_API_NAR_ID);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(uploadedControllerServiceApiNar.getIdentifier(), NarState.INSTALLED));

        // Verify all NARs are now installed
        final NarSummariesEntity narSummaries = narUploadUtil.verifyNarSummaries(3);
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

        // Verify retrieving the processor NAR details
        final NarDetailsEntity processorNarDetails = getNifiClient().getControllerClient().getNarDetails(uploadedProcessorsNar.getIdentifier());
        assertNotNull(processorNarDetails);
        assertNotNull(processorNarDetails.getNarSummary());
        assertNotNull(processorNarDetails.getProcessorTypes());
        assertEquals(1, processorNarDetails.getProcessorTypes().size());
        assertNotNull(processorNarDetails.getDependentCoordinates());
        assertEquals(0, processorNarDetails.getDependentCoordinates().size());

        // Verify retrieving the service API NAR details
        final NarDetailsEntity serviceApiNarDetails = getNifiClient().getControllerClient().getNarDetails(uploadedControllerServiceApiNar.getIdentifier());
        assertNotNull(serviceApiNarDetails);
        assertNotNull(serviceApiNarDetails.getNarSummary());
        assertNotNull(serviceApiNarDetails.getProcessorTypes());
        assertEquals(0, serviceApiNarDetails.getProcessorTypes().size());
        assertNotNull(serviceApiNarDetails.getDependentCoordinates());
        assertEquals(2, serviceApiNarDetails.getDependentCoordinates().size());

        // Create instance of the custom processor
        final NarCoordinateDTO uploadedProcessorCoordinate = uploadedProcessorsNar.getCoordinate();
        final ProcessorEntity customProcessor = getClientUtil().createProcessor(PROCESSOR_CLASS_NAME, uploadedProcessorCoordinate.getGroup(),
                uploadedProcessorCoordinate.getArtifact(), uploadedProcessorCoordinate.getVersion());
        assertNotNull(customProcessor.getComponent());
        assertNotNull(customProcessor.getComponent().getExtensionMissing());
        assertFalse(customProcessor.getComponent().getExtensionMissing());

        // Verify service API NAR can be replaced while other NARs depend on it
        final NarSummaryDTO replacedServiceApiNar = narUploadUtil.uploadNar(narsLocation, CONTROLLER_SERVICE_API_NAR_ID);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(replacedServiceApiNar.getIdentifier(), NarState.INSTALLED));

        // Verify processors NAR can be replaced
        final NarSummaryDTO replacedProcessorsNar = narUploadUtil.uploadNar(narsLocation, PROCESSORS_NAR_ID);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(replacedProcessorsNar.getIdentifier(), NarState.INSTALLED));

        // Verify controller services NAR can be replaced
        final NarSummaryDTO replacedControllerServiceNar = narUploadUtil.uploadNar(narsLocation, CONTROLLER_SERVICE_NAR_ID);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(replacedControllerServiceNar.getIdentifier(), NarState.INSTALLED));

        // Verify service API NAR can't be deleted while other NARs depend on it
        assertThrows(NiFiClientException.class, () -> getNifiClient().getControllerClient().deleteNar(uploadedControllerServiceApiNar.getIdentifier(), true));

        // Delete other NARs first
        narUploadUtil.deleteNar(uploadedProcessorsNar);
        narUploadUtil.deleteNar(uploadedControllerServiceNar);
        narUploadUtil.deleteNar(uploadedControllerServiceApiNar);

        // Verify no NARs exist
        narUploadUtil.verifyNarSummaries(0);

        // Verify custom processor is ghosted
        final String customProcessorId = customProcessor.getId();
        waitFor(() -> {
            final ProcessorEntity customProcessorAfterDelete = getNifiClient().getProcessorClient().getProcessor(customProcessorId);
            logger.info("Waiting for processor {} to be considered missing", customProcessorId);
            return customProcessorAfterDelete.getComponent().getExtensionMissing();
        });

        // Restore NARs
        narUploadUtil.uploadNar(narsLocation, CONTROLLER_SERVICE_API_NAR_ID);
        narUploadUtil.uploadNar(narsLocation, CONTROLLER_SERVICE_NAR_ID);
        final NarSummaryDTO restoredProcessorsNar = narUploadUtil.uploadNar(narsLocation, PROCESSORS_NAR_ID);
        waitFor(narUploadUtil.getWaitForNarStateSupplier(restoredProcessorsNar.getIdentifier(), NarState.INSTALLED));

        // Verify processor is un-ghosted
        waitFor(() -> {
            final ProcessorEntity customProcessorAfterDelete = getNifiClient().getProcessorClient().getProcessor(customProcessorId);
            logger.info("Waiting for processor {} to be considered not missing", customProcessorId);
            return !customProcessorAfterDelete.getComponent().getExtensionMissing();
        });
    }

    private boolean matchingBundles(final BundleDTO bundleDTO, final NarCoordinateDTO narCoordinateDTO) {
        return bundleDTO.getGroup().equals(narCoordinateDTO.getGroup())
                && bundleDTO.getArtifact().equals(narCoordinateDTO.getArtifact())
                && bundleDTO.getVersion().equals(narCoordinateDTO.getVersion());
    }

}
