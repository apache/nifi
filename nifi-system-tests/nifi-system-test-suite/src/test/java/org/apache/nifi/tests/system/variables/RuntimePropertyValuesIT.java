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
package org.apache.nifi.tests.system.variables;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RuntimePropertyValuesIT extends NiFiSystemIT {
    private Map<String, String> properties;
    private Map<String, String> expectedRuntimeProperties;

    @BeforeEach
    public void emptyPropertiesMap() {
        properties = new TreeMap<>();
        expectedRuntimeProperties = new TreeMap<>();
    }

    @Timeout(30)
    @Test
    public void testRuntimePropertyValues() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("WriteRuntimePropertiesToFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(processor, terminate, "success");

        properties.put("Always Optional", "test");
        properties.put("Required If Optional Property Set", "test-value");
        properties.put("Required If Optional Property Set To Foo", "test-value");
        properties.put("Depends On Always Optional", "test-value");
        properties.put("Depends On Depends On Always Optional", "test-value");

        expectedRuntimeProperties.put("Always Optional", "test");
        expectedRuntimeProperties.put("Required If Optional Property Set", "test-value");
        expectedRuntimeProperties.put("Required If Optional Property Set To Foo", null);
        expectedRuntimeProperties.put("Depends On Always Optional", "test-value");
        expectedRuntimeProperties.put("Depends On Depends On Always Optional", "test-value");

        getClientUtil().updateProcessorProperties(processor, properties);

        getClientUtil().waitForValidProcessor(processor.getId());
        getClientUtil().waitForValidProcessor(terminate.getId());

        runProcessorOnce(processor);
        getClientUtil().waitForStoppedProcessor(processor.getId());

        waitForQueueCount(connection.getId(), 1);

        final String runtimePropertiesString = getClientUtil().getFlowFileContentAsUtf8(connection.getId(), 0);
        final String expectedRuntimePropertiesString = getStringFromPropertiesMap(expectedRuntimeProperties);
        assertEquals(expectedRuntimePropertiesString, runtimePropertiesString);
    }

    @Timeout(30)
    @Test
    public void testRuntimePropertyValuesMissingRootDependency() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("WriteRuntimePropertiesToFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        final ConnectionEntity connection = getClientUtil().createConnection(processor, terminate, "success");

        properties.put("Required If Optional Property Set", "test-value");
        properties.put("Required If Optional Property Set To Foo", "test-value");

        expectedRuntimeProperties.put("Always Optional", null);
        expectedRuntimeProperties.put("Required If Optional Property Set", null);
        expectedRuntimeProperties.put("Required If Optional Property Set To Foo", null);
        expectedRuntimeProperties.put("Depends On Always Optional", null);
        expectedRuntimeProperties.put("Depends On Depends On Always Optional", null);

        getClientUtil().updateProcessorProperties(processor, properties);

        getClientUtil().waitForValidProcessor(processor.getId());

        runProcessorOnce(processor);
        getClientUtil().waitForStoppedProcessor(processor.getId());

        waitForQueueCount(connection.getId(), 1);
        final String runtimePropertiesString = getClientUtil().getFlowFileContentAsUtf8(connection.getId(), 0);
        final String expectedRuntimePropertiesString = getStringFromPropertiesMap(expectedRuntimeProperties);
        assertEquals(expectedRuntimePropertiesString, runtimePropertiesString);
    }

    @Timeout(30)
    @Test
    public void testRuntimePropertyValuesMissingMiddleDependency() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("WriteRuntimePropertiesToFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        final ConnectionEntity connection = getClientUtil().createConnection(processor, terminate, "success");

        properties.put("Always Optional", "foo");
        properties.put("Required If Optional Property Set", "test-value");
        properties.put("Required If Optional Property Set To Foo", "test-value");
        properties.put("Depends On Depends On Always Optional", "test-value");

        expectedRuntimeProperties.put("Always Optional", "foo");
        expectedRuntimeProperties.put("Required If Optional Property Set", "test-value");
        expectedRuntimeProperties.put("Required If Optional Property Set To Foo", "test-value");
        expectedRuntimeProperties.put("Depends On Always Optional", null);
        expectedRuntimeProperties.put("Depends On Depends On Always Optional", null);

        getClientUtil().updateProcessorProperties(processor, properties);

        getClientUtil().waitForValidProcessor(processor.getId());

        runProcessorOnce(processor);
        getClientUtil().waitForStoppedProcessor(processor.getId());

        waitForQueueCount(connection.getId(), 1);
        final String runtimePropertiesString = getClientUtil().getFlowFileContentAsUtf8(connection.getId(), 0);
        final String expectedRuntimePropertiesString = getStringFromPropertiesMap(expectedRuntimeProperties);
        assertEquals(expectedRuntimePropertiesString, runtimePropertiesString);
    }

    private String getStringFromPropertiesMap(final Map<String, String> properties) {
        final Map<String, String> formattedProperties = new TreeMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            final String descriptorName = entry.getKey();
            final String value = entry.getValue();
            formattedProperties.put(String.format("%s-allProperties", descriptorName), value);
            formattedProperties.put(String.format("%s-usingPropertyDescriptor", descriptorName), value);
            formattedProperties.put(String.format("%s-usingName", descriptorName), value);
        }

        return getFormattedPropertiesString(formattedProperties);
    }

    private String getFormattedPropertiesString(final Map<String, String> properties) {
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            sb.append(String.format("%s:%s,", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }

    private void runProcessorOnce(final ProcessorEntity processorEntity) throws NiFiClientException, IOException, InterruptedException {
        getNifiClient().getProcessorClient().runProcessorOnce(processorEntity);
        getClientUtil().waitForStoppedProcessor(processorEntity.getId());
    }
}
