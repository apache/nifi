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
package org.apache.nifi.controller.serialization;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.AbstractPolicyBasedAuthorizer;
import org.apache.nifi.authorization.MockPolicyBasedAuthorizer;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.controller.DummyScheduledProcessor;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.variable.FileBasedVariableRegistry;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StandardFlowSerializerTest {

    private static final String RAW_COMMENTS
            = "<tagName> \"This\" is an ' example with many characters that need to be filtered and escaped \u0002 in it. \u007f \u0086 " + Character.MIN_SURROGATE;
    private static final String SERIALIZED_COMMENTS
            = "&lt;tagName&gt; \"This\" is an ' example with many characters that need to be filtered and escaped  in it. &#127; &#134; ";
    private volatile String propsFile = StandardFlowSerializerTest.class.getResource("/standardflowserializertest.nifi.properties").getFile();

    private FlowController controller;
    private Bundle systemBundle;
    private ExtensionDiscoveringManager extensionManager;
    private StandardFlowSerializer serializer;

    @Before
    public void setUp() throws Exception {
        final FlowFileEventRepository flowFileEventRepo = Mockito.mock(FlowFileEventRepository.class);
        final AuditService auditService = Mockito.mock(AuditService.class);
        final Map<String, String> otherProps = new HashMap<>();
        otherProps.put(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceRepository.class.getName());
        otherProps.put("nifi.remote.input.socket.port", "");
        otherProps.put("nifi.remote.input.secure", "");
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(propsFile, otherProps);
        final String algorithm = nifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM);
        final String provider = nifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_PROVIDER);
        final String password = nifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY);
        final StringEncryptor encryptor = StringEncryptor.createEncryptor(algorithm, provider, password);

        // use the system bundle
        systemBundle = SystemBundle.create(nifiProperties);
        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, Collections.emptySet());

        final AbstractPolicyBasedAuthorizer authorizer = new MockPolicyBasedAuthorizer();
        final VariableRegistry variableRegistry = new FileBasedVariableRegistry(nifiProperties.getVariableRegistryPropertiesPaths());

        final BulletinRepository bulletinRepo = Mockito.mock(BulletinRepository.class);
        controller = FlowController.createStandaloneInstance(flowFileEventRepo, nifiProperties, authorizer,
            auditService, encryptor, bulletinRepo, variableRegistry, Mockito.mock(FlowRegistryClient.class), extensionManager);

        serializer = new StandardFlowSerializer(encryptor);
    }

    @After
    public void after() throws Exception {
        controller.shutdown(true);
        FileUtils.deleteDirectory(new File("./target/standardflowserializertest"));
    }

    @Test
    public void testSerializationEscapingAndFiltering() throws Exception {
        final ProcessorNode dummy = controller.getFlowManager().createProcessor(DummyScheduledProcessor.class.getName(),
            UUID.randomUUID().toString(), systemBundle.getBundleDetails().getCoordinate());

        dummy.setComments(RAW_COMMENTS);
        controller.getFlowManager().getRootGroup().addProcessor(dummy);

        // serialize the controller
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final Document doc = serializer.transform(controller, ScheduledStateLookup.IDENTITY_LOOKUP);
        serializer.serialize(doc, os);

        // verify the results contain the serialized string
        final String serializedFlow = os.toString(StandardCharsets.UTF_8.name());
        assertTrue(serializedFlow.contains(SERIALIZED_COMMENTS));
        assertFalse(serializedFlow.contains(RAW_COMMENTS));
    }
}
