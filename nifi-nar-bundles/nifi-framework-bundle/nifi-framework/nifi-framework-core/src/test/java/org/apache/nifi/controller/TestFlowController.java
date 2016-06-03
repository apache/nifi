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

package org.apache.nifi.controller;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.admin.service.KeyService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.MockProvenanceEventRepository;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFlowController {

    private FlowController controller;

    @Before
    public void setup() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");

        final FlowFileEventRepository flowFileEventRepo = Mockito.mock(FlowFileEventRepository.class);
        final KeyService keyService = Mockito.mock(KeyService.class);
        final AuditService auditService = Mockito.mock(AuditService.class);
        final StringEncryptor encryptor = StringEncryptor.createEncryptor();
        final NiFiProperties properties = NiFiProperties.getInstance();
        properties.setProperty(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceEventRepository.class.getName());
        properties.setProperty("nifi.remote.input.socket.port", "");
        properties.setProperty("nifi.remote.input.secure", "");

        final BulletinRepository bulletinRepo = Mockito.mock(BulletinRepository.class);
        controller = FlowController.createStandaloneInstance(flowFileEventRepo, properties, keyService, auditService, encryptor, bulletinRepo);
    }

    @After
    public void cleanup() {
        controller.shutdown(true);
    }

    @Test
    public void testCreateMissingProcessor() throws ProcessorInstantiationException {
        final ProcessorNode procNode = controller.createProcessor("org.apache.nifi.NonExistingProcessor", "1234-Processor");
        assertNotNull(procNode);
        assertEquals("org.apache.nifi.NonExistingProcessor", procNode.getCanonicalClassName());
        assertEquals("(Missing) NonExistingProcessor", procNode.getComponentType());

        final PropertyDescriptor descriptor = procNode.getPropertyDescriptor("my descriptor");
        assertNotNull(descriptor);
        assertEquals("my descriptor", descriptor.getName());
        assertTrue(descriptor.isRequired());
        assertTrue(descriptor.isSensitive());

        final Relationship relationship = procNode.getRelationship("my relationship");
        assertEquals("my relationship", relationship.getName());
    }

    @Test
    public void testCreateMissingReportingTask() throws ReportingTaskInstantiationException {
        final ReportingTaskNode taskNode = controller.createReportingTask("org.apache.nifi.NonExistingReportingTask", "1234-Reporting-Task", true);
        assertNotNull(taskNode);
        assertEquals("org.apache.nifi.NonExistingReportingTask", taskNode.getCanonicalClassName());
        assertEquals("(Missing) NonExistingReportingTask", taskNode.getComponentType());

        final PropertyDescriptor descriptor = taskNode.getReportingTask().getPropertyDescriptor("my descriptor");
        assertNotNull(descriptor);
        assertEquals("my descriptor", descriptor.getName());
        assertTrue(descriptor.isRequired());
        assertTrue(descriptor.isSensitive());
    }

    @Test
    public void testCreateMissingControllerService() throws ProcessorInstantiationException {
        final ControllerServiceNode serviceNode = controller.createControllerService("org.apache.nifi.NonExistingControllerService", "1234-Controller-Service", false);
        assertNotNull(serviceNode);
        assertEquals("org.apache.nifi.NonExistingControllerService", serviceNode.getCanonicalClassName());
        assertEquals("(Missing) NonExistingControllerService", serviceNode.getComponentType());

        final ControllerService service = serviceNode.getControllerServiceImplementation();
        final PropertyDescriptor descriptor = service.getPropertyDescriptor("my descriptor");
        assertNotNull(descriptor);
        assertEquals("my descriptor", descriptor.getName());
        assertTrue(descriptor.isRequired());
        assertTrue(descriptor.isSensitive());
        assertEquals("GhostControllerService[id=1234-Controller-Service, type=org.apache.nifi.NonExistingControllerService]", service.toString());
        service.hashCode(); // just make sure that an Exception is not thrown
        assertTrue(service.equals(service));
        assertFalse(service.equals(serviceNode));
    }

}
