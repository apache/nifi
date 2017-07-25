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
package org.apache.nifi.controller.reporting;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.AbstractPolicyBasedAuthorizer;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.MockPolicyBasedAuthorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.User;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.DummyScheduledReportingTask;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.variable.FileBasedVariableRegistry;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestStandardReportingContext {

    private FlowController controller;
    private AbstractPolicyBasedAuthorizer authorizer;
    private FlowFileEventRepository flowFileEventRepo;
    private AuditService auditService;
    private StringEncryptor encryptor;
    private NiFiProperties nifiProperties;
    private Bundle systemBundle;
    private BulletinRepository bulletinRepo;
    private VariableRegistry variableRegistry;
    private volatile String propsFile = TestStandardReportingContext.class.getResource("/flowcontrollertest.nifi.properties").getFile();

    @Before
    public void setup() {

        flowFileEventRepo = Mockito.mock(FlowFileEventRepository.class);
        auditService = Mockito.mock(AuditService.class);
        final Map<String, String> otherProps = new HashMap<>();
        otherProps.put(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceRepository.class.getName());
        otherProps.put("nifi.remote.input.socket.port", "");
        otherProps.put("nifi.remote.input.secure", "");
        nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, otherProps);
        encryptor = StringEncryptor.createEncryptor(nifiProperties);

        // use the system bundle
        systemBundle = SystemBundle.create(nifiProperties);
        ExtensionManager.discoverExtensions(systemBundle, Collections.emptySet());

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
        controller = FlowController.createStandaloneInstance(flowFileEventRepo, nifiProperties, authorizer, auditService, encryptor, bulletinRepo, variableRegistry);
    }

    @After
    public void cleanup() throws Exception {
        controller.shutdown(true);
        FileUtils.deleteDirectory(new File("./target/flowcontrollertest"));
    }

    @Test
    public void testGetPropertyReportingTask() throws ReportingTaskInstantiationException {
        ReportingTaskNode reportingTask = controller.createReportingTask(DummyScheduledReportingTask.class.getName(), systemBundle.getBundleDetails().getCoordinate());
        PropertyDescriptor TEST_WITHOUT_DEFAULT_VALUE = new PropertyDescriptor.Builder().name("Test without default value").build();
        PropertyDescriptor TEST_WITH_DEFAULT_VALUE = new PropertyDescriptor.Builder().name("Test with default value").build();

        PropertyValue defaultValue = reportingTask.getReportingContext().getProperty(TEST_WITH_DEFAULT_VALUE);
        assertEquals("nifi", defaultValue.getValue());

        PropertyValue value = reportingTask.getReportingContext().getProperty(TEST_WITHOUT_DEFAULT_VALUE);
        assertEquals(null, value.getValue());
    }

}
