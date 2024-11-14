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
import org.apache.nifi.toolkit.client.ControllerServicesClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
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

public class ControllerServiceDynamicPropertiesIT extends NiFiSystemIT {

    private static final String SENSITIVE_PROPERTY_NAME = "Credentials";

    private static final String SENSITIVE_PROPERTY_VALUE = "Token";

    private static final Set<String> SENSITIVE_DYNAMIC_PROPERTY_NAMES = Collections.singleton(SENSITIVE_PROPERTY_NAME);

    @Test
    void testGetPropertyDescriptor() throws NiFiClientException, IOException {
        final ControllerServiceEntity controllerServiceEntity = getClientUtil().createControllerService("SensitiveDynamicPropertiesService");

        final ControllerServicesClient servicesClient = getNifiClient().getControllerServicesClient();
        final PropertyDescriptorEntity propertyDescriptorEntity = servicesClient.getPropertyDescriptor(controllerServiceEntity.getId(), SENSITIVE_PROPERTY_NAME, null);
        final PropertyDescriptorDTO propertyDescriptor = propertyDescriptorEntity.getPropertyDescriptor();
        assertFalse(propertyDescriptor.isSensitive());
        assertTrue(propertyDescriptor.isDynamic());

        final PropertyDescriptorEntity sensitivePropertyDescriptorEntity = servicesClient.getPropertyDescriptor(controllerServiceEntity.getId(), SENSITIVE_PROPERTY_NAME, true);
        final PropertyDescriptorDTO sensitivePropertyDescriptor = sensitivePropertyDescriptorEntity.getPropertyDescriptor();
        assertTrue(sensitivePropertyDescriptor.isSensitive());
        assertTrue(sensitivePropertyDescriptor.isDynamic());
    }

    @Test
    public void testSensitiveDynamicPropertiesNotSupported() throws NiFiClientException, IOException {
        final ControllerServiceEntity controllerServiceEntity = getClientUtil().createControllerService("StandardCountService");
        final ControllerServiceDTO component = controllerServiceEntity.getComponent();
        assertFalse(component.getSupportsSensitiveDynamicProperties());

        component.setSensitiveDynamicPropertyNames(SENSITIVE_DYNAMIC_PROPERTY_NAMES);

        getClientUtil().updateControllerService(controllerServiceEntity, Collections.singletonMap(SENSITIVE_PROPERTY_NAME, SENSITIVE_PROPERTY_VALUE));

        getClientUtil().waitForControllerServiceValidationStatus(controllerServiceEntity.getId(), ControllerServiceDTO.INVALID);
    }

    @Test
    public void testSensitiveDynamicPropertiesSupportedConfigured() throws NiFiClientException, IOException {
        final ControllerServiceEntity controllerServiceEntity = getClientUtil().createControllerService("SensitiveDynamicPropertiesService");
        final ControllerServiceDTO component = controllerServiceEntity.getComponent();
        assertTrue(component.getSupportsSensitiveDynamicProperties());

        component.setSensitiveDynamicPropertyNames(SENSITIVE_DYNAMIC_PROPERTY_NAMES);
        component.setProperties(Collections.singletonMap(SENSITIVE_PROPERTY_NAME, SENSITIVE_PROPERTY_VALUE));

        getNifiClient().getControllerServicesClient().updateControllerService(controllerServiceEntity);

        final ControllerServiceEntity updatedControllerServiceEntity = getNifiClient().getControllerServicesClient().getControllerService(controllerServiceEntity.getId());
        final ControllerServiceDTO updatedComponent = updatedControllerServiceEntity.getComponent();

        final Map<String, String> properties = updatedComponent.getProperties();
        assertNotSame(SENSITIVE_PROPERTY_VALUE, properties.get(SENSITIVE_PROPERTY_NAME));

        final Map<String, PropertyDescriptorDTO> descriptors = updatedComponent.getDescriptors();
        final PropertyDescriptorDTO descriptor = descriptors.get(SENSITIVE_PROPERTY_NAME);
        assertNotNull(descriptor);
        assertTrue(descriptor.isSensitive());
        assertTrue(descriptor.isDynamic());

        getClientUtil().waitForControllerServiceValidationStatus(controllerServiceEntity.getId(), ControllerServiceDTO.VALID);
    }
}
