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
package org.apache.nifi.tests.system.encryption;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Verifies that sensitive component properties and sensitive Parameters protected by a Property Encryption Provider are
 * written to the flow configuration in encrypted form and recovered after a restart.
 *
 * <p>The Provider is configured explicitly rather than relying on the default, so that the configured implementation is
 * loaded from its NAR and used for both flow serialization and flow synchronization.</p>
 */
class PropertyEncryptionProviderIT extends NiFiSystemIT {
    private static final String PROVIDER_IMPLEMENTATION = "org.apache.nifi.security.encryption.password.PasswordBasedPropertyEncryptionProvider";

    private static final String SENSITIVE_PROPERTY_VALUE = "Sensitive Property Value 4c1cf47b";

    private static final String SENSITIVE_PARAMETER_VALUE = "Sensitive Parameter Value 9a2be830";

    private static final String PARAMETER_CONTEXT_NAME = "Property Encryption Provider Context";

    private static final String PARAMETER_NAME = "sensitiveParameter";

    private static final String PARAMETER_REFERENCE = "#{%s}".formatted(PARAMETER_NAME);

    private static final String SENSITIVE_CONTENT_PROPERTY = "Sensitive Content";

    private static final String UPDATE_STRATEGY_PROPERTY = "Update Strategy";

    private static final String REPLACE_STRATEGY = "Replace";

    private static final String SUCCESS_RELATIONSHIP = "success";

    private static final String FLOW_CONFIGURATION_FILENAME = "conf/flow.json.gz";

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of(NiFiProperties.PROPERTY_ENCRYPTION_PROVIDER_IMPLEMENTATION, PROVIDER_IMPLEMENTATION);
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Test
    void testSensitivePropertyAndParameterRecoveredAfterRestart() throws NiFiClientException, IOException, InterruptedException {
        final ParameterContextEntity parameterContext = getClientUtil().createParameterContext(PARAMETER_CONTEXT_NAME, PARAMETER_NAME, SENSITIVE_PARAMETER_VALUE, true);
        getClientUtil().setParameterContext("root", parameterContext);

        final ProcessorEntity createdGenerate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity generate = getClientUtil().updateProcessorProperties(createdGenerate, Map.of("Max FlowFiles", "1"));

        final ProcessorEntity propertyUpdateContent = createUpdateContent(SENSITIVE_PROPERTY_VALUE);
        final ProcessorEntity parameterUpdateContent = createUpdateContent(PARAMETER_REFERENCE);

        final ConnectionEntity propertyConnection = createSensitiveContentFlow(generate, propertyUpdateContent);
        final ConnectionEntity parameterConnection = createSensitiveContentFlow(generate, parameterUpdateContent);

        restart();

        final String flowConfiguration = readFlowConfiguration();
        assertFalse(flowConfiguration.contains(SENSITIVE_PROPERTY_VALUE), "Sensitive property value written to flow configuration without encryption");
        assertFalse(flowConfiguration.contains(SENSITIVE_PARAMETER_VALUE), "Sensitive Parameter value written to flow configuration without encryption");

        startProcessor(propertyUpdateContent.getId());
        startProcessor(parameterUpdateContent.getId());
        startProcessor(generate.getId());

        waitForQueueCount(propertyConnection.getId(), getNumberOfNodes());
        waitForQueueCount(parameterConnection.getId(), getNumberOfNodes());

        assertEquals(SENSITIVE_PROPERTY_VALUE, getClientUtil().getFlowFileContentAsUtf8(propertyConnection.getId(), 0));
        assertEquals(SENSITIVE_PARAMETER_VALUE, getClientUtil().getFlowFileContentAsUtf8(parameterConnection.getId(), 0));
    }

    private ProcessorEntity createUpdateContent(final String sensitiveContent) throws NiFiClientException, IOException {
        final ProcessorEntity updateContent = getClientUtil().createProcessor("UpdateContent");
        return getClientUtil().updateProcessorProperties(updateContent, Map.of(SENSITIVE_CONTENT_PROPERTY, sensitiveContent, UPDATE_STRATEGY_PROPERTY, REPLACE_STRATEGY));
    }

    /**
     * Connect the source Processor to the UpdateContent Processor and connect UpdateContent to a TerminateFlowFile
     * Processor that is left stopped, so that the FlowFile written by UpdateContent stays queued for inspection
     *
     * @param generate Source Processor
     * @param updateContent Processor that writes the sensitive value to the FlowFile
     * @return Connection holding the FlowFiles written by UpdateContent
     */
    private ConnectionEntity createSensitiveContentFlow(final ProcessorEntity generate, final ProcessorEntity updateContent) throws NiFiClientException, IOException {
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        getClientUtil().createConnection(generate, updateContent, SUCCESS_RELATIONSHIP);
        return getClientUtil().createConnection(updateContent, terminate, SUCCESS_RELATIONSHIP);
    }

    private void restart() throws IOException {
        getNiFiInstance().stop();
        getNiFiInstance().start(true);
        setupClient();
    }

    private void startProcessor(final String processorId) throws NiFiClientException, IOException, InterruptedException {
        getClientUtil().waitForValidProcessor(processorId);
        getClientUtil().startProcessor(getNifiClient().getProcessorClient().getProcessor(processorId));
    }

    private String readFlowConfiguration() throws IOException {
        final File flowConfiguration = new File(getNiFiInstance().getInstanceDirectory(), FLOW_CONFIGURATION_FILENAME);

        try (
                InputStream inputStream = Files.newInputStream(flowConfiguration.toPath());
                InputStream compressed = new GZIPInputStream(inputStream);
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        ) {
            compressed.transferTo(outputStream);
            return outputStream.toString(StandardCharsets.UTF_8);
        }
    }
}
