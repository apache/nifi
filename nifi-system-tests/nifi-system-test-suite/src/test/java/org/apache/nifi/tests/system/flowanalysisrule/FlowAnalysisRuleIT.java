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

package org.apache.nifi.tests.system.flowanalysisrule;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlowAnalysisRuleIT extends NiFiSystemIT {


    public static final String SENSITIVE_PROPERTY_NAME = "SensitiveProperty";

    private static final String SENSITIVE_PROPERTY_VALUE = "SensitiveValue";

    private static final Set<String> SENSITIVE_DYNAMIC_PROPERTY_NAMES = Collections.singleton(SENSITIVE_PROPERTY_NAME);

    @Test
    public void testGetPropertyDescriptor() throws NiFiClientException, IOException {
        final FlowAnalysisRuleEntity flowAnalysisRuleEntity = getClientUtil().createFlowAnalysisRule("SensitiveDynamicPropertiesFlowAnalysisRule");

        final PropertyDescriptorEntity propertyDescriptorEntity = getNifiClient().getControllerClient().getFlowAnalysisRulePropertyDescriptor(
                flowAnalysisRuleEntity.getId(),
                SENSITIVE_PROPERTY_NAME,
                null
        );
        final PropertyDescriptorDTO propertyDescriptor = propertyDescriptorEntity.getPropertyDescriptor();
        assertFalse(propertyDescriptor.isSensitive());
        assertTrue(propertyDescriptor.isDynamic());

        final PropertyDescriptorEntity sensitivePropertyDescriptorEntity = getNifiClient().getControllerClient().getFlowAnalysisRulePropertyDescriptor(
                flowAnalysisRuleEntity.getId(),
                SENSITIVE_PROPERTY_NAME,
                true
        );
        final PropertyDescriptorDTO sensitivePropertyDescriptor = sensitivePropertyDescriptorEntity.getPropertyDescriptor();
        assertTrue(sensitivePropertyDescriptor.isSensitive());
        assertTrue(sensitivePropertyDescriptor.isDynamic());
    }

    @Test
    public void testSensitiveDynamicPropertiesNotSupported() throws NiFiClientException, IOException {
        final FlowAnalysisRuleEntity flowAnalysisRuleEntity = getClientUtil().createFlowAnalysisRule("ControllerServiceReferencingFlowAnalysisRule");
        final FlowAnalysisRuleDTO component = flowAnalysisRuleEntity.getComponent();
        assertFalse(component.getSupportsSensitiveDynamicProperties());

        component.setSensitiveDynamicPropertyNames(SENSITIVE_DYNAMIC_PROPERTY_NAMES);

        getNifiClient().getControllerClient().updateFlowAnalysisRule(flowAnalysisRuleEntity);

        getClientUtil().waitForFlowAnalysisRuleValidationStatus(flowAnalysisRuleEntity.getId(), FlowAnalysisRuleDTO.INVALID);
    }

    @Test
    public void testSensitiveDynamicPropertiesSupportedConfigured() throws NiFiClientException, IOException {
        final FlowAnalysisRuleEntity flowAnalysisRuleEntity = getClientUtil().createFlowAnalysisRule("SensitiveDynamicPropertiesFlowAnalysisRule");
        final FlowAnalysisRuleDTO component = flowAnalysisRuleEntity.getComponent();
        assertTrue(component.getSupportsSensitiveDynamicProperties());

        component.setSensitiveDynamicPropertyNames(SENSITIVE_DYNAMIC_PROPERTY_NAMES);
        component.setProperties(Collections.singletonMap(SENSITIVE_PROPERTY_NAME, SENSITIVE_PROPERTY_VALUE));

        getNifiClient().getControllerClient().updateFlowAnalysisRule(flowAnalysisRuleEntity);

        final FlowAnalysisRuleEntity updatedFlowAnalysisRuleEntity = getNifiClient().getControllerClient().getFlowAnalysisRule(flowAnalysisRuleEntity.getId());
        final FlowAnalysisRuleDTO updatedComponent = updatedFlowAnalysisRuleEntity.getComponent();

        final Map<String, String> properties = updatedComponent.getProperties();
        assertNotSame(SENSITIVE_PROPERTY_VALUE, properties.get(SENSITIVE_PROPERTY_NAME));

        final Map<String, PropertyDescriptorDTO> descriptors = updatedComponent.getDescriptors();
        final PropertyDescriptorDTO descriptor = descriptors.get(SENSITIVE_PROPERTY_NAME);
        assertNotNull(descriptor);
        assertTrue(descriptor.isSensitive());
        assertTrue(descriptor.isDynamic());

        getClientUtil().waitForFlowAnalysisRuleValid(flowAnalysisRuleEntity.getId());
    }
}
