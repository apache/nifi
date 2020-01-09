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
    public void testNonMatchingControllerService() throws NiFiClientException, IOException {
        final ControllerServiceEntity fakeServiceEntity = getClientUtil().createControllerService(
                NiFiSystemIT.TEST_CS_PACKAGE + ".FakeControllerService2",
                "root",
                NiFiSystemIT.NIFI_GROUP_ID,
                "nifi-system-test-extensions2-nar",
                getNiFiVersion());
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
        assertEquals("Invalid", processorStatus);
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
