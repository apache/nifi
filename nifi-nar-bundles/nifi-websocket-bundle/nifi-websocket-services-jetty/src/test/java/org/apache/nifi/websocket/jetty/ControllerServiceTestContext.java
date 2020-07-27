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
package org.apache.nifi.websocket.jetty;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.MockPropertyValue;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ControllerServiceTestContext {

    private final ConfigurationContext configurationContext = mock(ConfigurationContext.class);
    private final ValidationContext validationContext = mock(ValidationContext.class);
    private MockControllerServiceInitializationContext initializationContext;

    public ControllerServiceTestContext(ControllerService controllerService, String id) {
        initializationContext = new MockControllerServiceInitializationContext(controllerService, id);
        doAnswer(invocation -> configurationContext.getProperty(invocation.getArgumentAt(0, PropertyDescriptor.class)))
                .when(validationContext).getProperty(any(PropertyDescriptor.class));
        controllerService.getPropertyDescriptors().forEach(prop -> setDefaultValue(prop));
    }

    public MockControllerServiceInitializationContext getInitializationContext() {
        return initializationContext;
    }

    public ConfigurationContext getConfigurationContext() {
        return configurationContext;
    }

    public MockPropertyValue setDefaultValue(PropertyDescriptor propertyDescriptor) {
        return setCustomValue(propertyDescriptor, propertyDescriptor.getDefaultValue());
    }

    public MockPropertyValue setCustomValue(PropertyDescriptor propertyDescriptor, String value) {
        final MockPropertyValue propertyValue = new MockPropertyValue(value, initializationContext);
        when(configurationContext.getProperty(eq(propertyDescriptor)))
                .thenReturn(propertyValue);
        return propertyValue;
    }

    public ValidationContext getValidationContext() {
        return validationContext;
    }
}
