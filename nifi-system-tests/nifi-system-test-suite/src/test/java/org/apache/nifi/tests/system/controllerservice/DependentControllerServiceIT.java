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
package org.apache.nifi.tests.system.controllerservice;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.ControllerServiceRunStatusEntity;
import org.apache.nifi.web.api.entity.UpdateControllerServiceReferenceRequestEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DependentControllerServiceIT extends NiFiSystemIT {

    @Test
    public void testEnableDisableServiceWithReferences() throws NiFiClientException, IOException {
        final ControllerServiceEntity sleepOnValidation = getClientUtil().createControllerService("StandardSleepService");
        sleepOnValidation.getComponent().setProperties(Collections.singletonMap("Validate Sleep Time", "10 secs"));
        getNifiClient().getControllerServicesClient().updateControllerService(sleepOnValidation);

        final ControllerServiceEntity referencingService = getClientUtil().createControllerService("StandardSleepService");
        referencingService.getComponent().setProperties(Collections.singletonMap("Dependent Service", sleepOnValidation.getId()));
        getNifiClient().getControllerServicesClient().updateControllerService(referencingService);

        // Enable Sleep on Validate
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(sleepOnValidation.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(sleepOnValidation.getId(), runStatusEntity);

        // Enable Referencing Services
        final ControllerServiceReferencingComponentsEntity referencingComponentsEntity = getNifiClient().getControllerServicesClient().getControllerServiceReferences(sleepOnValidation.getId());

        final Map<String, RevisionDTO> referenceRevisions = new HashMap<>();
        for (final ControllerServiceReferencingComponentEntity componentEntity : referencingComponentsEntity.getControllerServiceReferencingComponents()) {
            referenceRevisions.put(componentEntity.getId(), componentEntity.getRevision());
        }

        final UpdateControllerServiceReferenceRequestEntity updateReferencesEntity = new UpdateControllerServiceReferenceRequestEntity();
        updateReferencesEntity.setId(sleepOnValidation.getId());
        updateReferencesEntity.setReferencingComponentRevisions(referenceRevisions);
        updateReferencesEntity.setState("ENABLED");
        getNifiClient().getControllerServicesClient().updateControllerServiceReferences(updateReferencesEntity);

        getClientUtil().waitForControllerSerivcesEnabled("root");

        // Disable the referencing services.
        updateReferencesEntity.setState("DISABLED");
        getNifiClient().getControllerServicesClient().updateControllerServiceReferences(updateReferencesEntity);
        getClientUtil().waitForControllerSerivcesDisabled("root", referencingService.getId());

        // Disable the Sleep On Validation Service.
        runStatusEntity.setState("DISABLED");
        getNifiClient().getControllerServicesClient().activateControllerService(sleepOnValidation.getId(), runStatusEntity);

        // Wait for all services to become disabled.
        getClientUtil().waitForControllerSerivcesDisabled("root");
    }
}
