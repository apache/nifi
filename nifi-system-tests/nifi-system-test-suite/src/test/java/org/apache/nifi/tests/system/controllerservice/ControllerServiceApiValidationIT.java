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
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceRunStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ControllerServiceApiValidationIT extends NiFiSystemIT {
    @Test
    public void testMatchingControllerService() throws NiFiClientException, IOException {
        final ControllerServiceEntity fakeServiceEntity = getClientUtil().createControllerService("FakeControllerService1");
        final ProcessorEntity fakeProcessorEntity = getClientUtil().createProcessor("FakeProcessor");
        fakeProcessorEntity.getComponent().getConfig().setProperties(Collections.singletonMap("Fake Service", fakeServiceEntity.getId()));
        getNifiClient().getProcessorClient().updateProcessor(fakeProcessorEntity);
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(fakeServiceEntity.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(fakeServiceEntity.getId(), runStatusEntity);
        getClientUtil().waitForControllerSerivcesEnabled("root");
        String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(fakeServiceEntity.getId()).getStatus().getRunStatus();
        String processorStatus = getNifiClient().getProcessorClient().getProcessor(fakeProcessorEntity.getId()).getStatus().getRunStatus();

        assertEquals("ENABLED", controllerStatus);
        assertEquals("Stopped", processorStatus);
    }

    @Test
    public void testMatchingDynamicPropertyControllerService() throws NiFiClientException, IOException {
        final ControllerServiceEntity fakeServiceEntity = getClientUtil().createControllerService("FakeControllerService1");
        final ProcessorEntity fakeProcessorEntity = getClientUtil().createProcessor("FakeDynamicPropertiesProcessor");
        fakeProcessorEntity.getComponent().getConfig().setProperties(Collections.singletonMap("FCS.fakeControllerService", fakeServiceEntity.getId()));
        getNifiClient().getProcessorClient().updateProcessor(fakeProcessorEntity);
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(fakeServiceEntity.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(fakeServiceEntity.getId(), runStatusEntity);
        getClientUtil().waitForControllerSerivcesEnabled("root");
        String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(fakeServiceEntity.getId()).getStatus().getRunStatus();
        String processorStatus = getNifiClient().getProcessorClient().getProcessor(fakeProcessorEntity.getId()).getStatus().getRunStatus();

        assertEquals("ENABLED", controllerStatus);
        assertEquals("Stopped", processorStatus);
    }

    @Test
    public void testNonMatchingControllerService() throws NiFiClientException, IOException, InterruptedException {
        final ControllerServiceEntity controllerService = getClientUtil().createControllerService(
                NiFiSystemIT.TEST_CS_PACKAGE + ".FakeControllerService2",
                "root",
                NiFiSystemIT.NIFI_GROUP_ID,
                "nifi-system-test-extensions2-nar",
                getNiFiVersion());

        final ProcessorEntity processor = getClientUtil().createProcessor("FakeProcessor");
        getClientUtil().updateProcessorProperties(processor, Collections.singletonMap("Fake Service", controllerService.getId()));
        getClientUtil().enableControllerService(controllerService);

        getClientUtil().waitForControllerSerivcesEnabled("root");

        final String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(controllerService.getId()).getStatus().getRunStatus();
        assertEquals("ENABLED", controllerStatus);

        getClientUtil().waitForInvalidProcessor(processor.getId());
    }

    @Test
    public void testNonMatchingDynamicPropertyControllerService() throws NiFiClientException, IOException {
        final ControllerServiceEntity fakeServiceEntity = getClientUtil().createControllerService(
                NiFiSystemIT.TEST_CS_PACKAGE + ".FakeControllerService2",
                "root",
                NiFiSystemIT.NIFI_GROUP_ID,
                "nifi-system-test-extensions2-nar",
                getNiFiVersion());
        final ProcessorEntity fakeProcessorEntity = getClientUtil().createProcessor("FakeDynamicPropertiesProcessor");
        fakeProcessorEntity.getComponent().getConfig().setProperties(Collections.singletonMap("FCS.fakeControllerService", fakeServiceEntity.getId()));
        getNifiClient().getProcessorClient().updateProcessor(fakeProcessorEntity);
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(fakeServiceEntity.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(fakeServiceEntity.getId(), runStatusEntity);
        getClientUtil().waitForControllerSerivcesEnabled("root");
        String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(fakeServiceEntity.getId()).getStatus().getRunStatus();
        String processorStatus = getNifiClient().getProcessorClient().getProcessor(fakeProcessorEntity.getId()).getStatus().getRunStatus();

        assertEquals("ENABLED", controllerStatus);
        assertEquals("Invalid", processorStatus);
    }

    @Test
    public void testMatchingGenericControllerService() throws NiFiClientException, IOException {
        final ControllerServiceEntity fakeServiceEntity = getClientUtil().createControllerService("FakeControllerService1");
        final ProcessorEntity fakeProcessorEntity = getClientUtil().createProcessor(
                NiFiSystemIT.TEST_PROCESSORS_PACKAGE + ".FakeGenericProcessor",
                "root",
                NiFiSystemIT.NIFI_GROUP_ID,
                "nifi-system-test-extensions2-nar",
                getNiFiVersion());
        fakeProcessorEntity.getComponent().getConfig().setProperties(Collections.singletonMap("Fake Service", fakeServiceEntity.getId()));
        getNifiClient().getProcessorClient().updateProcessor(fakeProcessorEntity);
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(fakeServiceEntity.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(fakeServiceEntity.getId(), runStatusEntity);
        getClientUtil().waitForControllerSerivcesEnabled("root");
        String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(fakeServiceEntity.getId()).getStatus().getRunStatus();
        String processorStatus = getNifiClient().getProcessorClient().getProcessor(fakeProcessorEntity.getId()).getStatus().getRunStatus();

        assertEquals("ENABLED", controllerStatus);
        assertEquals("Stopped", processorStatus);
    }

    @Test
    public void testMatchingGenericDynamicPropertyControllerService() throws NiFiClientException, IOException {
        final ControllerServiceEntity fakeServiceEntity = getClientUtil().createControllerService("FakeControllerService1");
        final ProcessorEntity fakeProcessorEntity = getClientUtil().createProcessor("FakeDynamicPropertiesProcessor");
        fakeProcessorEntity.getComponent().getConfig().setProperties(Collections.singletonMap("CS.fakeControllerService", fakeServiceEntity.getId()));
        getNifiClient().getProcessorClient().updateProcessor(fakeProcessorEntity);
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(fakeServiceEntity.getRevision());
        getNifiClient().getControllerServicesClient().activateControllerService(fakeServiceEntity.getId(), runStatusEntity);
        getClientUtil().waitForControllerSerivcesEnabled("root");
        String controllerStatus = getNifiClient().getControllerServicesClient().getControllerService(fakeServiceEntity.getId()).getStatus().getRunStatus();
        String processorStatus = getNifiClient().getProcessorClient().getProcessor(fakeProcessorEntity.getId()).getStatus().getRunStatus();

        assertEquals("ENABLED", controllerStatus);
        assertEquals("Stopped", processorStatus);
    }
}
