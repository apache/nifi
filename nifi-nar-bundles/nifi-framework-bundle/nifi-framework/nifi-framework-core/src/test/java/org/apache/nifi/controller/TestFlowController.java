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

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.AbstractPolicyBasedAuthorizer;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.MockPolicyBasedAuthorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.User;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.VariableRegistryUtils;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class TestFlowController {

    private FlowController controller;
    private AbstractPolicyBasedAuthorizer authorizer;
    private StandardFlowSynchronizer standardFlowSynchronizer;
    private FlowFileEventRepository flowFileEventRepo;
    private AuditService auditService;
    private StringEncryptor encryptor;
    private NiFiProperties properties;
    private BulletinRepository bulletinRepo;
    private VariableRegistry variableRegistry;

    @Before
    public void setup() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");

        flowFileEventRepo = Mockito.mock(FlowFileEventRepository.class);
        auditService = Mockito.mock(AuditService.class);
        encryptor = StringEncryptor.createEncryptor();
        properties = NiFiProperties.getInstance();
        properties.setProperty(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceRepository.class.getName());
        properties.setProperty("nifi.remote.input.socket.port", "");
        properties.setProperty("nifi.remote.input.secure", "");

        User user1 = new User.Builder().identifier("user-id-1").identity("user-1").build();
        User user2 = new User.Builder().identifier("user-id-2").identity("user-2").build();

        Group group1 = new Group.Builder().identifier("group-id-1").name("group-1").addUser(user1.getIdentifier()).build();
        Group group2 = new Group.Builder().identifier("group-id-2").name("group-2").build();

        AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-id-1")
                .resource("resource1")
                .action(RequestAction.READ)
                .addUser(user1.getIdentifier())
                .addUser(user2.getIdentifier())
                .build();

        AccessPolicy policy2 = new AccessPolicy.Builder()
                .identifier("policy-id-2")
                .resource("resource2")
                .action(RequestAction.READ)
                .addGroup(group1.getIdentifier())
                .addGroup(group2.getIdentifier())
                .addUser(user1.getIdentifier())
                .addUser(user2.getIdentifier())
                .build();

        Set<Group> groups1 = new LinkedHashSet<>();
        groups1.add(group1);
        groups1.add(group2);

        Set<User> users1 = new LinkedHashSet<>();
        users1.add(user1);
        users1.add(user2);

        Set<AccessPolicy> policies1 = new LinkedHashSet<>();
        policies1.add(policy1);
        policies1.add(policy2);

        authorizer = new MockPolicyBasedAuthorizer(groups1, users1, policies1);
        variableRegistry = VariableRegistryUtils.createCustomVariableRegistry(properties.getVariableRegistryPropertiesPaths());

        bulletinRepo = Mockito.mock(BulletinRepository.class);
        controller = FlowController.createStandaloneInstance(flowFileEventRepo, properties, authorizer, auditService, encryptor, bulletinRepo,variableRegistry);

        standardFlowSynchronizer = new StandardFlowSynchronizer(StringEncryptor.createEncryptor());
    }

    @After
    public void cleanup() {
        controller.shutdown(true);
    }

    @Test
    public void testSynchronizeFlowWhenAuthorizationsAreEqual() {
        // create a mock proposed data flow with the same auth fingerprint as the current authorizer
        final String authFingerprint = authorizer.getFingerprint();
        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getAuthorizerFingerprint()).thenReturn(authFingerprint.getBytes(StandardCharsets.UTF_8));

        controller.synchronize(standardFlowSynchronizer, proposedDataFlow);

        assertEquals(authFingerprint, authorizer.getFingerprint());
    }

    @Test(expected = UninheritableFlowException.class)
    public void testSynchronizeFlowWhenAuthorizationsAreDifferent() {
        // create a mock proposed data flow with different auth fingerprint as the current authorizer
        final String authFingerprint = "<authorizations></authorizations>";
        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getAuthorizerFingerprint()).thenReturn(authFingerprint.getBytes(StandardCharsets.UTF_8));

        controller.synchronize(standardFlowSynchronizer, proposedDataFlow);
        assertNotEquals(authFingerprint, authorizer.getFingerprint());
    }

    @Test(expected = UninheritableFlowException.class)
    public void testSynchronizeFlowWhenProposedAuthorizationsAreNull() {
        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getAuthorizerFingerprint()).thenReturn(null);

        controller.synchronize(standardFlowSynchronizer, proposedDataFlow);
    }

    @Test
    public void testSynchronizeFlowWhenCurrentAuthorizationsAreEmptyAndProposedAreNot() {
        // create a mock proposed data flow with the same auth fingerprint as the current authorizer
        final String authFingerprint = authorizer.getFingerprint();
        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getAuthorizerFingerprint()).thenReturn(authFingerprint.getBytes(StandardCharsets.UTF_8));

        authorizer = new MockPolicyBasedAuthorizer();
        assertNotEquals(authFingerprint, authorizer.getFingerprint());

        controller.shutdown(true);
        controller = FlowController.createStandaloneInstance(flowFileEventRepo, properties, authorizer, auditService, encryptor, bulletinRepo,variableRegistry);
        controller.synchronize(standardFlowSynchronizer, proposedDataFlow);
        assertEquals(authFingerprint, authorizer.getFingerprint());
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
