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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.authorization.AbstractPolicyBasedAuthorizer;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.MockPolicyBasedAuthorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.User;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.scheduling.StandardProcessScheduler;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.mock.DummyProcessor;
import org.apache.nifi.controller.service.mock.DummyReportingTask;
import org.apache.nifi.controller.service.mock.ServiceA;
import org.apache.nifi.controller.service.mock.ServiceB;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepository;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.variable.FileBasedVariableRegistry;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFlowController {

    private FlowController controller;
    private AbstractPolicyBasedAuthorizer authorizer;
    private FlowFileEventRepository flowFileEventRepo;
    private AuditService auditService;
    private StringEncryptor encryptor;
    private NiFiProperties nifiProperties;
    private Bundle systemBundle;
    private BulletinRepository bulletinRepo;
    private VariableRegistry variableRegistry;
    private volatile String propsFile = "src/test/resources/flowcontrollertest.nifi.properties";
    private ExtensionDiscoveringManager extensionManager;

    /**
     * Utility method which accepts {@link NiFiProperties} object but calls {@link StringEncryptor#createEncryptor(String, String, String)} with extracted properties.
     *
     * @param nifiProperties the NiFiProperties object
     * @return the StringEncryptor
     */
    private StringEncryptor createEncryptorFromProperties(NiFiProperties nifiProperties) {
        final String algorithm = nifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM);
        final String provider = nifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_PROVIDER);
        final String password = nifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY);
        return StringEncryptor.createEncryptor(algorithm, provider, password);
    }


    @Before
    public void setup() {

        flowFileEventRepo = Mockito.mock(FlowFileEventRepository.class);
        auditService = Mockito.mock(AuditService.class);
        final Map<String, String> otherProps = new HashMap<>();
        otherProps.put(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceRepository.class.getName());
        otherProps.put("nifi.remote.input.socket.port", "");
        otherProps.put("nifi.remote.input.secure", "");
        nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, otherProps);
        encryptor = createEncryptorFromProperties(nifiProperties);

        // use the system bundle
        systemBundle = SystemBundle.create(nifiProperties);
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());

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
        variableRegistry = new FileBasedVariableRegistry(nifiProperties.getVariableRegistryPropertiesPaths());

        bulletinRepo = Mockito.mock(BulletinRepository.class);
        controller = FlowController.createStandaloneInstance(flowFileEventRepo, nifiProperties, authorizer,
            auditService, encryptor, bulletinRepo, variableRegistry, Mockito.mock(FlowRegistryClient.class), extensionManager);
    }

    @After
    public void cleanup() throws Exception {
        controller.shutdown(true);
        FileUtils.deleteDirectory(new File("./target/flowcontrollertest"));
    }

    @Test
    public void testSynchronizeFlowWithReportingTaskAndProcessorReferencingControllerService() throws IOException {
        final FlowSynchronizer standardFlowSynchronizer = new StandardFlowSynchronizer(
                createEncryptorFromProperties(nifiProperties), nifiProperties, extensionManager);

        // create a mock proposed data flow with the same auth fingerprint as the current authorizer
        final String authFingerprint = authorizer.getFingerprint();
        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getAuthorizerFingerprint()).thenReturn(authFingerprint.getBytes(StandardCharsets.UTF_8));

        final File flowFile = new File("src/test/resources/conf/reporting-task-with-cs-flow-0.7.0.xml");
        final String flow = IOUtils.toString(new FileInputStream(flowFile), StandardCharsets.UTF_8);
        when(proposedDataFlow.getFlow()).thenReturn(flow.getBytes(StandardCharsets.UTF_8));

        controller.synchronize(standardFlowSynchronizer, proposedDataFlow);

        // should be two controller services
        final Set<ControllerServiceNode> controllerServiceNodes = controller.getFlowManager().getAllControllerServices();
        assertNotNull(controllerServiceNodes);
        assertEquals(2, controllerServiceNodes.size());

        // find the controller service that was moved to the root group
        final ControllerServiceNode rootGroupCs = controllerServiceNodes.stream().filter(c -> c.getProcessGroup() != null).findFirst().get();
        assertNotNull(rootGroupCs);

        // find the controller service that was not moved to the root group
        final ControllerServiceNode controllerCs = controllerServiceNodes.stream().filter(c -> c.getProcessGroup() == null).findFirst().get();
        assertNotNull(controllerCs);

        // should be same class (not Ghost), different ids, and same properties
        assertEquals(rootGroupCs.getCanonicalClassName(), controllerCs.getCanonicalClassName());
        assertFalse(rootGroupCs.getCanonicalClassName().contains("Ghost"));
        assertNotEquals(rootGroupCs.getIdentifier(), controllerCs.getIdentifier());
        assertEquals(rootGroupCs.getProperties(), controllerCs.getProperties());

        // should be one processor
        final Collection<ProcessorNode> processorNodes = controller.getFlowManager().getGroup(controller.getFlowManager().getRootGroupId()).getProcessors();
        assertNotNull(processorNodes);
        assertEquals(1, processorNodes.size());

        // verify the processor is still pointing at the controller service that got moved to the root group
        final ProcessorNode processorNode = processorNodes.stream().findFirst().get();
        final PropertyDescriptor procControllerServiceProp = processorNode.getProperties().entrySet().stream()
                .filter(e -> e.getValue().equals(rootGroupCs.getIdentifier()))
                .map(e -> e.getKey())
                .findFirst()
                .get();
        assertNotNull(procControllerServiceProp);

        // should be one reporting task
        final Set<ReportingTaskNode> reportingTaskNodes = controller.getAllReportingTasks();
        assertNotNull(reportingTaskNodes);
        assertEquals(1, reportingTaskNodes.size());

        // verify that the reporting task is pointing at the controller service at the controller level
        final ReportingTaskNode reportingTaskNode = reportingTaskNodes.stream().findFirst().get();
        final PropertyDescriptor reportingTaskControllerServiceProp = reportingTaskNode.getProperties().entrySet().stream()
                .filter(e -> e.getValue().equals(controllerCs.getIdentifier()))
                .map(e -> e.getKey())
                .findFirst()
                .get();
        assertNotNull(reportingTaskControllerServiceProp);
    }

    @Test
    public void testSynchronizeFlowWithProcessorReferencingControllerService() throws IOException {
        final FlowSynchronizer standardFlowSynchronizer = new StandardFlowSynchronizer(
                createEncryptorFromProperties(nifiProperties), nifiProperties, extensionManager);

        // create a mock proposed data flow with the same auth fingerprint as the current authorizer
        final String authFingerprint = authorizer.getFingerprint();
        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getAuthorizerFingerprint()).thenReturn(authFingerprint.getBytes(StandardCharsets.UTF_8));

        final File flowFile = new File("src/test/resources/conf/processor-with-cs-flow-0.7.0.xml");
        final String flow = IOUtils.toString(new FileInputStream(flowFile), StandardCharsets.UTF_8);
        when(proposedDataFlow.getFlow()).thenReturn(flow.getBytes(StandardCharsets.UTF_8));

        controller.synchronize(standardFlowSynchronizer, proposedDataFlow);

        // should be two controller services
        final Set<ControllerServiceNode> controllerServiceNodes = controller.getFlowManager().getAllControllerServices();
        assertNotNull(controllerServiceNodes);
        assertEquals(1, controllerServiceNodes.size());

        // find the controller service that was moved to the root group
        final ControllerServiceNode rootGroupCs = controllerServiceNodes.stream().filter(c -> c.getProcessGroup() != null).findFirst().get();
        assertNotNull(rootGroupCs);

        // should be one processor
        final Collection<ProcessorNode> processorNodes = controller.getFlowManager().getRootGroup().getProcessors();
        assertNotNull(processorNodes);
        assertEquals(1, processorNodes.size());

        // verify the processor is still pointing at the controller service that got moved to the root group
        final ProcessorNode processorNode = processorNodes.stream().findFirst().get();
        final PropertyDescriptor procControllerServiceProp = processorNode.getProperties().entrySet().stream()
                .filter(e -> e.getValue().equals(rootGroupCs.getIdentifier()))
                .map(e -> e.getKey())
                .findFirst()
                .get();
        assertNotNull(procControllerServiceProp);
    }

    @Test
    public void testSynchronizeFlowWhenAuthorizationsAreEqual() {
        final FlowSynchronizer standardFlowSynchronizer = new StandardFlowSynchronizer(
                createEncryptorFromProperties(nifiProperties), nifiProperties, extensionManager);

        // create a mock proposed data flow with the same auth fingerprint as the current authorizer
        final String authFingerprint = authorizer.getFingerprint();
        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getAuthorizerFingerprint()).thenReturn(authFingerprint.getBytes(StandardCharsets.UTF_8));

        controller.synchronize(standardFlowSynchronizer, proposedDataFlow);

        assertEquals(authFingerprint, authorizer.getFingerprint());
    }

    @Test(expected = UninheritableFlowException.class)
    public void testSynchronizeFlowWhenAuthorizationsAreDifferent() {
        final FlowSynchronizer standardFlowSynchronizer = new StandardFlowSynchronizer(
                createEncryptorFromProperties(nifiProperties), nifiProperties, extensionManager);

        // create a mock proposed data flow with different auth fingerprint as the current authorizer
        final String authFingerprint = "<authorizations></authorizations>";
        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getAuthorizerFingerprint()).thenReturn(authFingerprint.getBytes(StandardCharsets.UTF_8));

        controller.synchronize(standardFlowSynchronizer, proposedDataFlow);
        assertNotEquals(authFingerprint, authorizer.getFingerprint());
    }

    @Test(expected = UninheritableFlowException.class)
    public void testSynchronizeFlowWhenProposedAuthorizationsAreNull() {
        final FlowSynchronizer standardFlowSynchronizer = new StandardFlowSynchronizer(
                createEncryptorFromProperties(nifiProperties), nifiProperties, extensionManager);

        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getAuthorizerFingerprint()).thenReturn(null);

        controller.synchronize(standardFlowSynchronizer, proposedDataFlow);
    }

    /**
     * StandardProcessScheduler is created by FlowController. The StandardProcessScheduler needs access to the Controller Service Provider,
     * but the Controller Service Provider needs the ProcessScheduler in its constructor. So the StandardProcessScheduler obtains the Controller Service
     * Provider by making a call back to FlowController.getControllerServiceProvider. This test exists to ensure that we always have access to the
     * Controller Service Provider in the Process Scheduler, and that we don't inadvertently start storing away the result of calling
     * FlowController.getControllerServiceProvider() before the service provider has been fully initialized.
     */
    @Test
    public void testProcessSchedulerHasAccessToControllerServiceProvider() {
        final StandardProcessScheduler scheduler = controller.getProcessScheduler();
        assertNotNull(scheduler);

        final ControllerServiceProvider serviceProvider = scheduler.getControllerServiceProvider();
        assertNotNull(serviceProvider);
        assertSame(serviceProvider, controller.getControllerServiceProvider());
    }

    @Test
    public void testSynchronizeFlowWhenCurrentAuthorizationsAreEmptyAndProposedAreNot() {
        final FlowSynchronizer standardFlowSynchronizer = new StandardFlowSynchronizer(
                createEncryptorFromProperties(nifiProperties), nifiProperties, extensionManager);

        // create a mock proposed data flow with the same auth fingerprint as the current authorizer
        final String authFingerprint = authorizer.getFingerprint();
        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getAuthorizerFingerprint()).thenReturn(authFingerprint.getBytes(StandardCharsets.UTF_8));

        authorizer = new MockPolicyBasedAuthorizer();
        assertNotEquals(authFingerprint, authorizer.getFingerprint());

        controller.shutdown(true);
        controller = FlowController.createStandaloneInstance(flowFileEventRepo, nifiProperties, authorizer,
            auditService, encryptor, bulletinRepo, variableRegistry, Mockito.mock(FlowRegistryClient.class), extensionManager);
        controller.synchronize(standardFlowSynchronizer, proposedDataFlow);
        assertEquals(authFingerprint, authorizer.getFingerprint());
    }

    @Test
    public void testSynchronizeFlowWhenProposedMissingComponentsAreDifferent() {
        final FlowSynchronizer standardFlowSynchronizer = new StandardFlowSynchronizer(
                createEncryptorFromProperties(nifiProperties), nifiProperties, extensionManager);

        final Set<String> missingComponents = new HashSet<>();
        missingComponents.add("1");
        missingComponents.add("2");

        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getMissingComponents()).thenReturn(missingComponents);

        try {
            controller.synchronize(standardFlowSynchronizer, proposedDataFlow);
            Assert.fail("Should have thrown exception");
        } catch (UninheritableFlowException e) {
            assertTrue(e.getMessage().contains("Proposed flow has missing components that are not considered missing in the current flow (1,2)"));
        }
    }

    @Test
    public void testSynchronizeFlowWhenExistingMissingComponentsAreDifferent() throws IOException {
        final StringEncryptor stringEncryptor = createEncryptorFromProperties(nifiProperties);
        final FlowSynchronizer standardFlowSynchronizer = new StandardFlowSynchronizer(stringEncryptor, nifiProperties, extensionManager);

        final ProcessorNode mockProcessorNode = mock(ProcessorNode.class);
        when(mockProcessorNode.getIdentifier()).thenReturn("1");
        when(mockProcessorNode.isExtensionMissing()).thenReturn(true);

        final ControllerServiceNode mockControllerServiceNode = mock(ControllerServiceNode.class);
        when(mockControllerServiceNode.getIdentifier()).thenReturn("2");
        when(mockControllerServiceNode.isExtensionMissing()).thenReturn(true);

        final ReportingTaskNode mockReportingTaskNode = mock(ReportingTaskNode.class);
        when(mockReportingTaskNode.getIdentifier()).thenReturn("3");
        when(mockReportingTaskNode.isExtensionMissing()).thenReturn(true);

        final ProcessGroup mockRootGroup = mock(ProcessGroup.class);
        when(mockRootGroup.findAllProcessors()).thenReturn(Collections.singletonList(mockProcessorNode));

        final SnippetManager mockSnippetManager = mock(SnippetManager.class);
        when(mockSnippetManager.export()).thenReturn(new byte[0]);

        final FlowManager flowManager = Mockito.mock(FlowManager.class);

        final FlowController mockFlowController = mock(FlowController.class);
        when(mockFlowController.getFlowManager()).thenReturn(flowManager);

        when(flowManager.getRootGroup()).thenReturn(mockRootGroup);
        when(flowManager.getAllControllerServices()).thenReturn(new HashSet<>(Arrays.asList(mockControllerServiceNode)));
        when(flowManager.getAllReportingTasks()).thenReturn(new HashSet<>(Arrays.asList(mockReportingTaskNode)));
        when(mockFlowController.getAuthorizer()).thenReturn(authorizer);
        when(mockFlowController.getSnippetManager()).thenReturn(mockSnippetManager);

        final DataFlow proposedDataFlow = Mockito.mock(DataFlow.class);
        when(proposedDataFlow.getMissingComponents()).thenReturn(new HashSet<>());

        try {
            standardFlowSynchronizer.sync(mockFlowController, proposedDataFlow, stringEncryptor);
            Assert.fail("Should have thrown exception");
        } catch (UninheritableFlowException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Current flow has missing components that are not considered missing in the proposed flow (1,2,3)"));
        }
    }

    @Test
    public void testSynchronizeFlowWhenBundlesAreSame() throws IOException {
        final FlowSynchronizer standardFlowSynchronizer = new StandardFlowSynchronizer(
                createEncryptorFromProperties(nifiProperties), nifiProperties, extensionManager);

        final LogRepository logRepository = LogRepositoryFactory.getRepository("d89ada5d-35fb-44ff-83f1-4cc00b48b2df");
        logRepository.removeAllObservers();

        syncFlow("src/test/resources/nifi/fingerprint/flow4.xml", standardFlowSynchronizer);
        syncFlow("src/test/resources/nifi/fingerprint/flow4.xml", standardFlowSynchronizer);
    }

    @Test
    public void testSynchronizeFlowWhenBundlesAreDifferent() throws IOException {
        final FlowSynchronizer standardFlowSynchronizer = new StandardFlowSynchronizer(
                createEncryptorFromProperties(nifiProperties), nifiProperties, extensionManager);

        final LogRepository logRepository = LogRepositoryFactory.getRepository("d89ada5d-35fb-44ff-83f1-4cc00b48b2df");
        logRepository.removeAllObservers();

        // first sync should work because we are syncing to an empty flow controller
        syncFlow("src/test/resources/nifi/fingerprint/flow4.xml", standardFlowSynchronizer);

        // second sync should fail because the bundle of the processor is different
        try {
            syncFlow("src/test/resources/nifi/fingerprint/flow4-with-different-bundle.xml", standardFlowSynchronizer);
            Assert.fail("Should have thrown UninheritableFlowException");
        } catch (UninheritableFlowException e) {
            //e.printStackTrace();
        }
    }

    private void syncFlow(String flowXmlFile, FlowSynchronizer standardFlowSynchronizer) throws IOException {
        String flowString = null;
        try (final InputStream in = new FileInputStream(flowXmlFile)) {
            flowString = IOUtils.toString(in, StandardCharsets.UTF_8);
        }
        assertNotNull(flowString);

        final DataFlow proposedDataFlow1 = Mockito.mock(DataFlow.class);
        when(proposedDataFlow1.getFlow()).thenReturn(flowString.getBytes(StandardCharsets.UTF_8));

        final String authFingerprint = authorizer.getFingerprint();
        when(proposedDataFlow1.getAuthorizerFingerprint()).thenReturn(authFingerprint.getBytes(StandardCharsets.UTF_8));

        controller.synchronize(standardFlowSynchronizer, proposedDataFlow1);
    }

    @Test
    public void testCreateMissingProcessor() throws ProcessorInstantiationException {
        final ProcessorNode procNode = controller.getFlowManager().createProcessor("org.apache.nifi.NonExistingProcessor", "1234-Processor",
                systemBundle.getBundleDetails().getCoordinate());
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
        final ReportingTaskNode taskNode = controller.createReportingTask("org.apache.nifi.NonExistingReportingTask", "1234-Reporting-Task",
                systemBundle.getBundleDetails().getCoordinate(), true);
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
        final ControllerServiceNode serviceNode = controller.getFlowManager().createControllerService("org.apache.nifi.NonExistingControllerService", "1234-Controller-Service",
                systemBundle.getBundleDetails().getCoordinate(), null, false, true);
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

    @Test
    public void testProcessorDefaultScheduleAnnotation() throws ProcessorInstantiationException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        ProcessorNode p_scheduled = controller.getFlowManager().createProcessor(DummyScheduledProcessor.class.getName(), "1234-ScheduledProcessor",
                systemBundle.getBundleDetails().getCoordinate());
        assertEquals(5, p_scheduled.getMaxConcurrentTasks());
        assertEquals(SchedulingStrategy.CRON_DRIVEN, p_scheduled.getSchedulingStrategy());
        assertEquals("0 0 0 1/1 * ?", p_scheduled.getSchedulingPeriod());
        assertEquals("1 sec", p_scheduled.getYieldPeriod());
        assertEquals("30 sec", p_scheduled.getPenalizationPeriod());
        assertEquals(LogLevel.WARN, p_scheduled.getBulletinLevel());
    }

    @Test
    public void testReportingTaskDefaultScheduleAnnotation() throws ReportingTaskInstantiationException {
        ReportingTaskNode p_scheduled = controller.getFlowManager().createReportingTask(DummyScheduledReportingTask.class.getName(), systemBundle.getBundleDetails().getCoordinate());
        assertEquals(SchedulingStrategy.CRON_DRIVEN, p_scheduled.getSchedulingStrategy());
        assertEquals("0 0 0 1/1 * ?", p_scheduled.getSchedulingPeriod());
    }

    @Test
    public void testProcessorDefaultSettingsAnnotation() throws ProcessorInstantiationException, ClassNotFoundException {

        ProcessorNode p_settings = controller.getFlowManager().createProcessor(DummySettingsProcessor.class.getName(), "1234-SettingsProcessor", systemBundle.getBundleDetails().getCoordinate());
        assertEquals("5 sec", p_settings.getYieldPeriod());
        assertEquals("1 min", p_settings.getPenalizationPeriod());
        assertEquals(LogLevel.DEBUG, p_settings.getBulletinLevel());
        assertEquals(1, p_settings.getMaxConcurrentTasks());
        assertEquals(SchedulingStrategy.TIMER_DRIVEN, p_settings.getSchedulingStrategy());
        assertEquals("0 sec", p_settings.getSchedulingPeriod());
    }

    @Test
    public void testPrimaryNodeOnlyAnnotation() throws ProcessorInstantiationException {
        String id = UUID.randomUUID().toString();
        ProcessorNode processorNode = controller.getFlowManager().createProcessor(DummyPrimaryNodeOnlyProcessor.class.getName(), id, systemBundle.getBundleDetails().getCoordinate());
        assertEquals(ExecutionNode.PRIMARY, processorNode.getExecutionNode());
    }

    @Test
    public void testDeleteProcessGroup() {
        ProcessGroup pg = controller.getFlowManager().createProcessGroup("my-process-group");
        pg.setName("my-process-group");
        ControllerServiceNode cs = controller.getFlowManager().createControllerService("org.apache.nifi.NonExistingControllerService", "my-controller-service",
                systemBundle.getBundleDetails().getCoordinate(), null, false, true);
        pg.addControllerService(cs);
        controller.getFlowManager().getRootGroup().addProcessGroup(pg);
        controller.getFlowManager().getRootGroup().removeProcessGroup(pg);
        pg.getControllerServices(true);
        assertTrue(pg.getControllerServices(true).isEmpty());
    }

    @Test
    public void testReloadProcessor() throws ProcessorInstantiationException {
        final String id = "1234-ScheduledProcessor" + System.currentTimeMillis();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ProcessorNode processorNode = controller.getFlowManager().createProcessor(DummyScheduledProcessor.class.getName(), id, coordinate);
        final String originalName = processorNode.getName();

        assertEquals(id, processorNode.getIdentifier());
        assertEquals(id, processorNode.getComponent().getIdentifier());
        assertEquals(coordinate.getCoordinate(), processorNode.getBundleCoordinate().getCoordinate());
        assertEquals(DummyScheduledProcessor.class.getCanonicalName(), processorNode.getCanonicalClassName());
        assertEquals(DummyScheduledProcessor.class.getSimpleName(), processorNode.getComponentType());
        assertEquals(DummyScheduledProcessor.class.getCanonicalName(), processorNode.getComponent().getClass().getCanonicalName());

        assertEquals(5, processorNode.getMaxConcurrentTasks());
        assertEquals(SchedulingStrategy.CRON_DRIVEN, processorNode.getSchedulingStrategy());
        assertEquals("0 0 0 1/1 * ?", processorNode.getSchedulingPeriod());
        assertEquals("1 sec", processorNode.getYieldPeriod());
        assertEquals("30 sec", processorNode.getPenalizationPeriod());
        assertEquals(LogLevel.WARN, processorNode.getBulletinLevel());

        // now change the type of the processor from DummyScheduledProcessor to DummySettingsProcessor
        controller.getReloadComponent().reload(processorNode, DummySettingsProcessor.class.getName(), coordinate, Collections.emptySet());

        // ids and coordinate should stay the same
        assertEquals(id, processorNode.getIdentifier());
        assertEquals(id, processorNode.getComponent().getIdentifier());
        assertEquals(coordinate.getCoordinate(), processorNode.getBundleCoordinate().getCoordinate());

        // in this test we happened to change between two processors that have different canonical class names
        // but in the running application the DAO layer would call verifyCanUpdateBundle and would prevent this so
        // for the sake of this test it is ok that the canonical class name hasn't changed
        assertEquals(originalName, processorNode.getName());
        assertEquals(DummyScheduledProcessor.class.getCanonicalName(), processorNode.getCanonicalClassName());
        assertEquals(DummyScheduledProcessor.class.getSimpleName(), processorNode.getComponentType());
        assertEquals(DummySettingsProcessor.class.getCanonicalName(), processorNode.getComponent().getClass().getCanonicalName());

        // all these settings should have stayed the same
        assertEquals(5, processorNode.getMaxConcurrentTasks());
        assertEquals(SchedulingStrategy.CRON_DRIVEN, processorNode.getSchedulingStrategy());
        assertEquals("0 0 0 1/1 * ?", processorNode.getSchedulingPeriod());
        assertEquals("1 sec", processorNode.getYieldPeriod());
        assertEquals("30 sec", processorNode.getPenalizationPeriod());
        assertEquals(LogLevel.WARN, processorNode.getBulletinLevel());
    }

    @Test
    public void testReloadProcessorWithAdditionalResources() throws ProcessorInstantiationException, MalformedURLException {
        final URL resource1 = new File("src/test/resources/TestClasspathResources/resource1.txt").toURI().toURL();
        final URL resource2 = new File("src/test/resources/TestClasspathResources/resource2.txt").toURI().toURL();
        final URL resource3 = new File("src/test/resources/TestClasspathResources/resource3.txt").toURI().toURL();
        final Set<URL> additionalUrls = new LinkedHashSet<>(Arrays.asList(resource1, resource2, resource3));

        final String id = "1234-ScheduledProcessor" + System.currentTimeMillis();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ProcessorNode processorNode = controller.getFlowManager().createProcessor(DummyScheduledProcessor.class.getName(), id, coordinate);
        final String originalName = processorNode.getName();

        // the instance class loader shouldn't have any of the resources yet
        InstanceClassLoader instanceClassLoader = extensionManager.getInstanceClassLoader(id);
        assertNotNull(instanceClassLoader);
        assertFalse(containsResource(instanceClassLoader.getURLs(), resource1));
        assertFalse(containsResource(instanceClassLoader.getURLs(), resource2));
        assertFalse(containsResource(instanceClassLoader.getURLs(), resource3));
        assertTrue(instanceClassLoader.getAdditionalResourceUrls().isEmpty());

        // now change the type of the processor from DummyScheduledProcessor to DummySettingsProcessor
        controller.getReloadComponent().reload(processorNode, DummySettingsProcessor.class.getName(), coordinate, additionalUrls);

        // the instance class loader shouldn't have any of the resources yet
        instanceClassLoader = extensionManager.getInstanceClassLoader(id);
        assertNotNull(instanceClassLoader);
        assertTrue(containsResource(instanceClassLoader.getURLs(), resource1));
        assertTrue(containsResource(instanceClassLoader.getURLs(), resource2));
        assertTrue(containsResource(instanceClassLoader.getURLs(), resource3));
        assertEquals(3, instanceClassLoader.getAdditionalResourceUrls().size());
    }

    @Test
    public void testReloadControllerService() {
        final String id = "ServiceA" + System.currentTimeMillis();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ControllerServiceNode controllerServiceNode = controller.getFlowManager().createControllerService(ServiceA.class.getName(), id, coordinate, null, true, true);
        final String originalName = controllerServiceNode.getName();

        assertEquals(id, controllerServiceNode.getIdentifier());
        assertEquals(id, controllerServiceNode.getComponent().getIdentifier());
        assertEquals(coordinate.getCoordinate(), controllerServiceNode.getBundleCoordinate().getCoordinate());
        assertEquals(ServiceA.class.getCanonicalName(), controllerServiceNode.getCanonicalClassName());
        assertEquals(ServiceA.class.getSimpleName(), controllerServiceNode.getComponentType());
        assertEquals(ServiceA.class.getCanonicalName(), controllerServiceNode.getComponent().getClass().getCanonicalName());

        controller.getReloadComponent().reload(controllerServiceNode, ServiceB.class.getName(), coordinate, Collections.emptySet());

        // ids and coordinate should stay the same
        assertEquals(id, controllerServiceNode.getIdentifier());
        assertEquals(id, controllerServiceNode.getComponent().getIdentifier());
        assertEquals(coordinate.getCoordinate(), controllerServiceNode.getBundleCoordinate().getCoordinate());

        // in this test we happened to change between two services that have different canonical class names
        // but in the running application the DAO layer would call verifyCanUpdateBundle and would prevent this so
        // for the sake of this test it is ok that the canonical class name hasn't changed
        assertEquals(originalName, controllerServiceNode.getName());
        assertEquals(ServiceA.class.getCanonicalName(), controllerServiceNode.getCanonicalClassName());
        assertEquals(ServiceA.class.getSimpleName(), controllerServiceNode.getComponentType());
        assertEquals(ServiceB.class.getCanonicalName(), controllerServiceNode.getComponent().getClass().getCanonicalName());
    }

    @Test
    public void testReloadControllerServiceWithAdditionalResources() throws MalformedURLException {
        final URL resource1 = new File("src/test/resources/TestClasspathResources/resource1.txt").toURI().toURL();
        final URL resource2 = new File("src/test/resources/TestClasspathResources/resource2.txt").toURI().toURL();
        final URL resource3 = new File("src/test/resources/TestClasspathResources/resource3.txt").toURI().toURL();
        final Set<URL> additionalUrls = new LinkedHashSet<>(Arrays.asList(resource1, resource2, resource3));

        final String id = "ServiceA" + System.currentTimeMillis();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ControllerServiceNode controllerServiceNode = controller.getFlowManager().createControllerService(ServiceA.class.getName(), id, coordinate, null, true, true);

        // the instance class loader shouldn't have any of the resources yet
        URLClassLoader instanceClassLoader = extensionManager.getInstanceClassLoader(id);
        assertNotNull(instanceClassLoader);
        assertFalse(containsResource(instanceClassLoader.getURLs(), resource1));
        assertFalse(containsResource(instanceClassLoader.getURLs(), resource2));
        assertFalse(containsResource(instanceClassLoader.getURLs(), resource3));
        assertTrue(instanceClassLoader instanceof InstanceClassLoader);
        assertTrue(((InstanceClassLoader) instanceClassLoader).getAdditionalResourceUrls().isEmpty());

        controller.getReloadComponent().reload(controllerServiceNode, ServiceB.class.getName(), coordinate, additionalUrls);

        // the instance class loader shouldn't have any of the resources yet
        instanceClassLoader = extensionManager.getInstanceClassLoader(id);
        assertNotNull(instanceClassLoader);
        assertTrue(containsResource(instanceClassLoader.getURLs(), resource1));
        assertTrue(containsResource(instanceClassLoader.getURLs(), resource2));
        assertTrue(containsResource(instanceClassLoader.getURLs(), resource3));
        assertTrue(instanceClassLoader instanceof InstanceClassLoader);
        assertEquals(3, ((InstanceClassLoader) instanceClassLoader).getAdditionalResourceUrls().size());
    }

    @Test
    public void testReloadReportingTask() throws ReportingTaskInstantiationException {
        final String id = "ReportingTask" + System.currentTimeMillis();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ReportingTaskNode node = controller.createReportingTask(DummyReportingTask.class.getName(), id, coordinate, true);
        final String originalName = node.getName();

        assertEquals(id, node.getIdentifier());
        assertEquals(id, node.getComponent().getIdentifier());
        assertEquals(coordinate.getCoordinate(), node.getBundleCoordinate().getCoordinate());
        assertEquals(DummyReportingTask.class.getCanonicalName(), node.getCanonicalClassName());
        assertEquals(DummyReportingTask.class.getSimpleName(), node.getComponentType());
        assertEquals(DummyReportingTask.class.getCanonicalName(), node.getComponent().getClass().getCanonicalName());

        controller.getReloadComponent().reload(node, DummyScheduledReportingTask.class.getName(), coordinate, Collections.emptySet());

        // ids and coordinate should stay the same
        assertEquals(id, node.getIdentifier());
        assertEquals(id, node.getComponent().getIdentifier());
        assertEquals(coordinate.getCoordinate(), node.getBundleCoordinate().getCoordinate());

        // in this test we happened to change between two services that have different canonical class names
        // but in the running application the DAO layer would call verifyCanUpdateBundle and would prevent this so
        // for the sake of this test it is ok that the canonical class name hasn't changed
        assertEquals(originalName, node.getName());
        assertEquals(DummyReportingTask.class.getCanonicalName(), node.getCanonicalClassName());
        assertEquals(DummyReportingTask.class.getSimpleName(), node.getComponentType());
        assertEquals(DummyScheduledReportingTask.class.getCanonicalName(), node.getComponent().getClass().getCanonicalName());
    }

    @Test
    public void testReloadReportingTaskWithAdditionalResources() throws ReportingTaskInstantiationException, MalformedURLException {
        final URL resource1 = new File("src/test/resources/TestClasspathResources/resource1.txt").toURI().toURL();
        final URL resource2 = new File("src/test/resources/TestClasspathResources/resource2.txt").toURI().toURL();
        final URL resource3 = new File("src/test/resources/TestClasspathResources/resource3.txt").toURI().toURL();
        final Set<URL> additionalUrls = new LinkedHashSet<>(Arrays.asList(resource1, resource2, resource3));

        final String id = "ReportingTask" + System.currentTimeMillis();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ReportingTaskNode node = controller.createReportingTask(DummyReportingTask.class.getName(), id, coordinate, true);

        // the instance class loader shouldn't have any of the resources yet
        InstanceClassLoader instanceClassLoader = extensionManager.getInstanceClassLoader(id);
        assertNotNull(instanceClassLoader);
        assertFalse(containsResource(instanceClassLoader.getURLs(), resource1));
        assertFalse(containsResource(instanceClassLoader.getURLs(), resource2));
        assertFalse(containsResource(instanceClassLoader.getURLs(), resource3));
        assertTrue(instanceClassLoader.getAdditionalResourceUrls().isEmpty());

        controller.getReloadComponent().reload(node, DummyScheduledReportingTask.class.getName(), coordinate, additionalUrls);

        // the instance class loader shouldn't have any of the resources yet
        instanceClassLoader = extensionManager.getInstanceClassLoader(id);
        assertNotNull(instanceClassLoader);
        assertTrue(containsResource(instanceClassLoader.getURLs(), resource1));
        assertTrue(containsResource(instanceClassLoader.getURLs(), resource2));
        assertTrue(containsResource(instanceClassLoader.getURLs(), resource3));
        assertEquals(3, instanceClassLoader.getAdditionalResourceUrls().size());
    }

    private boolean containsResource(URL[] resources, URL resourceToFind) {
        for (URL resource : resources) {
            if (resourceToFind.getPath().equals(resource.getPath())) {
                return true;
            }
        }
        return false;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInstantiateSnippetWhenProcessorMissingBundle() throws Exception {
        final String id = UUID.randomUUID().toString();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ProcessorNode processorNode = controller.getFlowManager().createProcessor(DummyProcessor.class.getName(), id, coordinate);

        // create a processor dto
        final ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId(UUID.randomUUID().toString()); // use a different id here
        processorDTO.setPosition(new PositionDTO(new Double(0), new Double(0)));
        processorDTO.setStyle(processorNode.getStyle());
        processorDTO.setParentGroupId("1234");
        processorDTO.setInputRequirement(processorNode.getInputRequirement().name());
        processorDTO.setPersistsState(processorNode.getProcessor().getClass().isAnnotationPresent(Stateful.class));
        processorDTO.setRestricted(processorNode.isRestricted());
        processorDTO.setExecutionNodeRestricted(processorNode.isExecutionNodeRestricted());
        processorDTO.setExtensionMissing(processorNode.isExtensionMissing());

        processorDTO.setType(processorNode.getCanonicalClassName());
        processorDTO.setBundle(null); // missing bundle
        processorDTO.setName(processorNode.getName());
        processorDTO.setState(processorNode.getScheduledState().toString());

        processorDTO.setRelationships(new ArrayList<>());

        processorDTO.setDescription("description");
        processorDTO.setSupportsParallelProcessing(!processorNode.isTriggeredSerially());
        processorDTO.setSupportsEventDriven(processorNode.isEventDrivenSupported());
        processorDTO.setSupportsBatching(processorNode.isSessionBatchingSupported());

        ProcessorConfigDTO configDTO = new ProcessorConfigDTO();
        configDTO.setSchedulingPeriod(processorNode.getSchedulingPeriod());
        configDTO.setPenaltyDuration(processorNode.getPenalizationPeriod());
        configDTO.setYieldDuration(processorNode.getYieldPeriod());
        configDTO.setRunDurationMillis(processorNode.getRunDuration(TimeUnit.MILLISECONDS));
        configDTO.setConcurrentlySchedulableTaskCount(processorNode.getMaxConcurrentTasks());
        configDTO.setLossTolerant(processorNode.isLossTolerant());
        configDTO.setComments(processorNode.getComments());
        configDTO.setBulletinLevel(processorNode.getBulletinLevel().name());
        configDTO.setSchedulingStrategy(processorNode.getSchedulingStrategy().name());
        configDTO.setExecutionNode(processorNode.getExecutionNode().name());
        configDTO.setAnnotationData(processorNode.getAnnotationData());

        processorDTO.setConfig(configDTO);

        // create the snippet with the processor
        final FlowSnippetDTO flowSnippetDTO = new FlowSnippetDTO();
        flowSnippetDTO.setProcessors(Collections.singleton(processorDTO));

        // instantiate the snippet
        assertEquals(0, controller.getFlowManager().getRootGroup().getProcessors().size());
        controller.getFlowManager().instantiateSnippet(controller.getFlowManager().getRootGroup(), flowSnippetDTO);
    }

    @Test
    public void testInstantiateSnippetWithProcessor() throws ProcessorInstantiationException {
        final String id = UUID.randomUUID().toString();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ProcessorNode processorNode = controller.getFlowManager().createProcessor(DummyProcessor.class.getName(), id, coordinate);

        // create a processor dto
        final ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId(UUID.randomUUID().toString()); // use a different id here
        processorDTO.setPosition(new PositionDTO(new Double(0), new Double(0)));
        processorDTO.setStyle(processorNode.getStyle());
        processorDTO.setParentGroupId("1234");
        processorDTO.setInputRequirement(processorNode.getInputRequirement().name());
        processorDTO.setPersistsState(processorNode.getProcessor().getClass().isAnnotationPresent(Stateful.class));
        processorDTO.setRestricted(processorNode.isRestricted());
        processorDTO.setExecutionNodeRestricted(processorNode.isExecutionNodeRestricted());
        processorDTO.setExtensionMissing(processorNode.isExtensionMissing());

        processorDTO.setType(processorNode.getCanonicalClassName());
        processorDTO.setBundle(new BundleDTO(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion()));
        processorDTO.setName(processorNode.getName());
        processorDTO.setState(processorNode.getScheduledState().toString());

        processorDTO.setRelationships(new ArrayList<>());

        processorDTO.setDescription("description");
        processorDTO.setSupportsParallelProcessing(!processorNode.isTriggeredSerially());
        processorDTO.setSupportsEventDriven(processorNode.isEventDrivenSupported());
        processorDTO.setSupportsBatching(processorNode.isSessionBatchingSupported());

        ProcessorConfigDTO configDTO = new ProcessorConfigDTO();
        configDTO.setSchedulingPeriod(processorNode.getSchedulingPeriod());
        configDTO.setPenaltyDuration(processorNode.getPenalizationPeriod());
        configDTO.setYieldDuration(processorNode.getYieldPeriod());
        configDTO.setRunDurationMillis(processorNode.getRunDuration(TimeUnit.MILLISECONDS));
        configDTO.setConcurrentlySchedulableTaskCount(processorNode.getMaxConcurrentTasks());
        configDTO.setLossTolerant(processorNode.isLossTolerant());
        configDTO.setComments(processorNode.getComments());
        configDTO.setBulletinLevel(processorNode.getBulletinLevel().name());
        configDTO.setSchedulingStrategy(processorNode.getSchedulingStrategy().name());
        configDTO.setExecutionNode(processorNode.getExecutionNode().name());
        configDTO.setAnnotationData(processorNode.getAnnotationData());

        processorDTO.setConfig(configDTO);

        // create the snippet with the processor
        final FlowSnippetDTO flowSnippetDTO = new FlowSnippetDTO();
        flowSnippetDTO.setProcessors(Collections.singleton(processorDTO));

        // instantiate the snippet
        assertEquals(0, controller.getFlowManager().getRootGroup().getProcessors().size());
        controller.getFlowManager().instantiateSnippet(controller.getFlowManager().getRootGroup(), flowSnippetDTO);
        assertEquals(1, controller.getFlowManager().getRootGroup().getProcessors().size());
    }

    @Test
    public void testInstantiateSnippetWithDisabledProcessor() throws ProcessorInstantiationException {
        final String id = UUID.randomUUID().toString();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ProcessorNode processorNode = controller.getFlowManager().createProcessor(DummyProcessor.class.getName(), id, coordinate);
        processorNode.disable();

        // create a processor dto
        final ProcessorDTO processorDTO = new ProcessorDTO();
        processorDTO.setId(UUID.randomUUID().toString()); // use a different id here
        processorDTO.setPosition(new PositionDTO(new Double(0), new Double(0)));
        processorDTO.setStyle(processorNode.getStyle());
        processorDTO.setParentGroupId("1234");
        processorDTO.setInputRequirement(processorNode.getInputRequirement().name());
        processorDTO.setPersistsState(processorNode.getProcessor().getClass().isAnnotationPresent(Stateful.class));
        processorDTO.setRestricted(processorNode.isRestricted());
        processorDTO.setExecutionNodeRestricted(processorNode.isExecutionNodeRestricted());
        processorDTO.setExtensionMissing(processorNode.isExtensionMissing());

        processorDTO.setType(processorNode.getCanonicalClassName());
        processorDTO.setBundle(new BundleDTO(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion()));
        processorDTO.setName(processorNode.getName());
        processorDTO.setState(processorNode.getScheduledState().toString());

        processorDTO.setRelationships(new ArrayList<>());

        processorDTO.setDescription("description");
        processorDTO.setSupportsParallelProcessing(!processorNode.isTriggeredSerially());
        processorDTO.setSupportsEventDriven(processorNode.isEventDrivenSupported());
        processorDTO.setSupportsBatching(processorNode.isSessionBatchingSupported());

        ProcessorConfigDTO configDTO = new ProcessorConfigDTO();
        configDTO.setSchedulingPeriod(processorNode.getSchedulingPeriod());
        configDTO.setPenaltyDuration(processorNode.getPenalizationPeriod());
        configDTO.setYieldDuration(processorNode.getYieldPeriod());
        configDTO.setRunDurationMillis(processorNode.getRunDuration(TimeUnit.MILLISECONDS));
        configDTO.setConcurrentlySchedulableTaskCount(processorNode.getMaxConcurrentTasks());
        configDTO.setLossTolerant(processorNode.isLossTolerant());
        configDTO.setComments(processorNode.getComments());
        configDTO.setBulletinLevel(processorNode.getBulletinLevel().name());
        configDTO.setSchedulingStrategy(processorNode.getSchedulingStrategy().name());
        configDTO.setExecutionNode(processorNode.getExecutionNode().name());
        configDTO.setAnnotationData(processorNode.getAnnotationData());

        processorDTO.setConfig(configDTO);

        // create the snippet with the processor
        final FlowSnippetDTO flowSnippetDTO = new FlowSnippetDTO();
        flowSnippetDTO.setProcessors(Collections.singleton(processorDTO));

        // instantiate the snippet
        assertEquals(0, controller.getFlowManager().getRootGroup().getProcessors().size());
        controller.getFlowManager().instantiateSnippet(controller.getFlowManager().getRootGroup(), flowSnippetDTO);
        assertEquals(1, controller.getFlowManager().getRootGroup().getProcessors().size());
        assertTrue(controller.getFlowManager().getRootGroup().getProcessors().iterator().next().getScheduledState().equals(ScheduledState.DISABLED));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInstantiateSnippetWhenControllerServiceMissingBundle() throws ProcessorInstantiationException {
        final String id = UUID.randomUUID().toString();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ControllerServiceNode controllerServiceNode = controller.getFlowManager().createControllerService(ServiceA.class.getName(), id, coordinate, null, true, true);

        // create the controller service dto
        final ControllerServiceDTO csDto = new ControllerServiceDTO();
        csDto.setId(UUID.randomUUID().toString()); // use a different id
        csDto.setParentGroupId(controllerServiceNode.getProcessGroup() == null ? null : controllerServiceNode.getProcessGroup().getIdentifier());
        csDto.setName(controllerServiceNode.getName());
        csDto.setType(controllerServiceNode.getCanonicalClassName());
        csDto.setBundle(null); // missing bundle
        csDto.setState(controllerServiceNode.getState().name());
        csDto.setAnnotationData(controllerServiceNode.getAnnotationData());
        csDto.setComments(controllerServiceNode.getComments());
        csDto.setPersistsState(controllerServiceNode.getControllerServiceImplementation().getClass().isAnnotationPresent(Stateful.class));
        csDto.setRestricted(controllerServiceNode.isRestricted());
        csDto.setExtensionMissing(controllerServiceNode.isExtensionMissing());
        csDto.setDescriptors(new LinkedHashMap<>());
        csDto.setProperties(new LinkedHashMap<>());

        // create the snippet with the controller service
        final FlowSnippetDTO flowSnippetDTO = new FlowSnippetDTO();
        flowSnippetDTO.setControllerServices(Collections.singleton(csDto));

        // instantiate the snippet
        assertEquals(0, controller.getFlowManager().getRootGroup().getControllerServices(false).size());
        controller.getFlowManager().instantiateSnippet(controller.getFlowManager().getRootGroup(), flowSnippetDTO);
    }

    @Test
    public void testInstantiateSnippetWithControllerService() throws ProcessorInstantiationException {
        final String id = UUID.randomUUID().toString();
        final BundleCoordinate coordinate = systemBundle.getBundleDetails().getCoordinate();
        final ControllerServiceNode controllerServiceNode = controller.getFlowManager().createControllerService(ServiceA.class.getName(), id, coordinate, null, true, true);

        // create the controller service dto
        final ControllerServiceDTO csDto = new ControllerServiceDTO();
        csDto.setId(UUID.randomUUID().toString()); // use a different id
        csDto.setParentGroupId(controllerServiceNode.getProcessGroup() == null ? null : controllerServiceNode.getProcessGroup().getIdentifier());
        csDto.setName(controllerServiceNode.getName());
        csDto.setType(controllerServiceNode.getCanonicalClassName());
        csDto.setBundle(new BundleDTO(coordinate.getGroup(), coordinate.getId(), coordinate.getVersion()));
        csDto.setState(controllerServiceNode.getState().name());
        csDto.setAnnotationData(controllerServiceNode.getAnnotationData());
        csDto.setComments(controllerServiceNode.getComments());
        csDto.setPersistsState(controllerServiceNode.getControllerServiceImplementation().getClass().isAnnotationPresent(Stateful.class));
        csDto.setRestricted(controllerServiceNode.isRestricted());
        csDto.setExtensionMissing(controllerServiceNode.isExtensionMissing());
        csDto.setDescriptors(new LinkedHashMap<>());
        csDto.setProperties(new LinkedHashMap<>());

        // create the snippet with the controller service
        final FlowSnippetDTO flowSnippetDTO = new FlowSnippetDTO();
        flowSnippetDTO.setControllerServices(Collections.singleton(csDto));

        // instantiate the snippet
        assertEquals(0, controller.getFlowManager().getRootGroup().getControllerServices(false).size());
        controller.getFlowManager().instantiateSnippet(controller.getFlowManager().getRootGroup(), flowSnippetDTO);
        assertEquals(1, controller.getFlowManager().getRootGroup().getControllerServices(false).size());
    }

}
