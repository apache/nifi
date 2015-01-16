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
package org.apache.nifi.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.StringEncryptor;

public class StandardProcessContext implements ProcessContext, ControllerServiceLookup {

    private final ProcessorNode procNode;
    private final ControllerServiceProvider controllerServiceProvider;
    private final Map<PropertyDescriptor, PreparedQuery> preparedQueries;
    private final StringEncryptor encryptor;

    public StandardProcessContext(final ProcessorNode processorNode, final ControllerServiceProvider controllerServiceProvider, final StringEncryptor encryptor) {
        this.procNode = processorNode;
        this.controllerServiceProvider = controllerServiceProvider;
        this.encryptor = encryptor;

        preparedQueries = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : procNode.getProperties().entrySet()) {
            final PropertyDescriptor desc = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                value = desc.getDefaultValue();
            }

            final PreparedQuery pq = Query.prepare(value);
            preparedQueries.put(desc, pq);
        }
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor descriptor) {
        return getProperty(descriptor.getName());
    }

    /**
     * <p>
     * Returns the currently configured value for the property with the given
     * name.
     * </p>
     */
    @Override
    public PropertyValue getProperty(final String propertyName) {
        final Processor processor = procNode.getProcessor();
        final PropertyDescriptor descriptor = processor.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return null;
        }

        final String setPropertyValue = procNode.getProperty(descriptor);
        final String propValue = (setPropertyValue == null) ? descriptor.getDefaultValue() : setPropertyValue;

        return new StandardPropertyValue(propValue, this, preparedQueries.get(descriptor));
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return new StandardPropertyValue(rawValue, this, Query.prepare(rawValue));
    }

    @Override
    public void yield() {
        procNode.yield();
    }

    @Override
    public ControllerService getControllerService(final String serviceIdentifier) {
        return controllerServiceProvider.getControllerService(serviceIdentifier);
    }

    @Override
    public int getMaxConcurrentTasks() {
        return procNode.getMaxConcurrentTasks();
    }

    @Override
    public String getAnnotationData() {
        return procNode.getAnnotationData();
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return procNode.getProperties();
    }

    @Override
    public String encrypt(final String unencrypted) {
        return encryptor.encrypt(unencrypted);
    }

    @Override
    public String decrypt(final String encrypted) {
        return encryptor.decrypt(encrypted);
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        if (!serviceType.isInterface()) {
            throw new IllegalArgumentException("ControllerServices may be referenced only via their interfaces; " + serviceType + " is not an interface");
        }
        return controllerServiceProvider.getControllerServiceIdentifiers(serviceType);
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        return controllerServiceProvider.isControllerServiceEnabled(service);
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        return controllerServiceProvider.isControllerServiceEnabled(serviceIdentifier);
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this;
    }
}
