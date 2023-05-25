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
package org.apache.nifi.bootstrap.email;

import org.apache.nifi.bootstrap.notification.NotificationContext;
import org.apache.nifi.bootstrap.notification.NotificationFailedException;
import org.apache.nifi.bootstrap.notification.NotificationInitializationContext;
import org.apache.nifi.bootstrap.notification.NotificationType;
import org.apache.nifi.bootstrap.notification.email.EmailNotificationService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class EmailNotificationServiceTest {

    private static final String SUBJECT = "Subject";

    private static final String MESSAGE = "Message";

    private static final String LOCALHOST_ADDRESS = "127.0.0.1";

    private static final String ADDRESS = "username@localhost.localdomain";

    @Test
    public void testNotifyMessagingException() {
        final Map<PropertyDescriptor, PropertyValue> properties = getProperties();
        final EmailNotificationService service = getNotificationService(properties);

        assertThrows(NotificationFailedException.class, () -> service.notify(getNotificationContext(), NotificationType.NIFI_STARTED, SUBJECT, MESSAGE));
    }

    private EmailNotificationService getNotificationService(final Map<PropertyDescriptor, PropertyValue> properties) {
        final EmailNotificationService service = new EmailNotificationService();
        final NotificationInitializationContext context = new NotificationInitializationContext() {
            @Override
            public String getIdentifier() {
                return NotificationInitializationContext.class.getName();
            }

            @Override
            public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                final PropertyValue propertyValue = properties.get(descriptor);
                return propertyValue == null ? new MockPropertyValue(descriptor.getDefaultValue()) : propertyValue;
            }

            @Override
            public Map<String, String> getAllProperties() {
                final Map<String, String> allProperties = new HashMap<>();
                for (final Map.Entry<PropertyDescriptor, PropertyValue> entry : properties.entrySet()) {
                    allProperties.put(entry.getKey().getName(), entry.getValue().getValue());
                }
                return allProperties;
            }
        };

        service.initialize(context);
        return service;
    }

    private NotificationContext getNotificationContext() {
        final Map<PropertyDescriptor, PropertyValue> properties = getProperties();
        return new NotificationContext() {
            @Override
            public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                final PropertyValue propertyValue = properties.get(descriptor);
                return propertyValue == null ? new MockPropertyValue(descriptor.getDefaultValue()) : propertyValue;
            }

            @Override
            public Map<PropertyDescriptor, String> getProperties() {
                final Map<PropertyDescriptor, String> propertyValues = new HashMap<>();
                for (final Map.Entry<PropertyDescriptor, PropertyValue> entry : properties.entrySet()) {
                    propertyValues.put(entry.getKey(), entry.getValue().getValue());
                }
                return propertyValues;
            }
        };
    }

    private Map<PropertyDescriptor, PropertyValue> getProperties() {
        final Map<PropertyDescriptor, PropertyValue> properties = new HashMap<>();

        properties.put(EmailNotificationService.SMTP_HOSTNAME, new MockPropertyValue(LOCALHOST_ADDRESS));

        properties.put(EmailNotificationService.SMTP_PORT, new MockPropertyValue("0"));
        properties.put(EmailNotificationService.SMTP_AUTH, new MockPropertyValue(Boolean.FALSE.toString()));
        properties.put(EmailNotificationService.FROM, new MockPropertyValue(ADDRESS));
        properties.put(EmailNotificationService.TO, new MockPropertyValue(ADDRESS));

        return properties;
    }
}
