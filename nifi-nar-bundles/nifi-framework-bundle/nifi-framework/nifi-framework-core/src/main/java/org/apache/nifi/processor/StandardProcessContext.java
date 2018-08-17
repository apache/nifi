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

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.Query.Range;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.lifecycle.TaskTermination;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.processor.exception.TerminatedTaskException;
import org.apache.nifi.util.Connectables;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StandardProcessContext implements ProcessContext, ControllerServiceLookup {

    private final ProcessorNode procNode;
    private final ControllerServiceProvider controllerServiceProvider;
    private final Map<PropertyDescriptor, PreparedQuery> preparedQueries;
    private final StringEncryptor encryptor;
    private final StateManager stateManager;
    private final TaskTermination taskTermination;
    private final Map<PropertyDescriptor, String> properties;

    public StandardProcessContext(final ProcessorNode processorNode, final ControllerServiceProvider controllerServiceProvider, final StringEncryptor encryptor, final StateManager stateManager,
            final TaskTermination taskTermination) {
        this.procNode = processorNode;
        this.controllerServiceProvider = controllerServiceProvider;
        this.encryptor = encryptor;
        this.stateManager = stateManager;
        this.taskTermination = taskTermination;

        preparedQueries = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : procNode.getProperties().entrySet()) {
            final PropertyDescriptor desc = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                value = desc.getDefaultValue();
            }

            if (value != null) {
                final PreparedQuery pq = Query.prepare(value);
                preparedQueries.put(desc, pq);
            }
        }

        properties = Collections.unmodifiableMap(processorNode.getProperties());
    }

    private void verifyTaskActive() {
        if (taskTermination.isTerminated()) {
            throw new TerminatedTaskException();
        }
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor descriptor) {
        verifyTaskActive();

        final String setPropertyValue = properties.get(descriptor);
        if (setPropertyValue != null) {
            return new StandardPropertyValue(setPropertyValue, this, preparedQueries.get(descriptor), procNode.getVariableRegistry());
        }

        // Get the "canonical" Property Descriptor from the Processor
        final PropertyDescriptor canonicalDescriptor = procNode.getProcessor().getPropertyDescriptor(descriptor.getName());
        final String defaultValue = canonicalDescriptor.getDefaultValue();

        return new StandardPropertyValue(defaultValue, this, preparedQueries.get(descriptor), procNode.getVariableRegistry());
    }

    /**
     * <p>
     * Returns the currently configured value for the property with the given name.
     * </p>
     */
    @Override
    public PropertyValue getProperty(final String propertyName) {
        verifyTaskActive();
        final Processor processor = procNode.getProcessor();
        final PropertyDescriptor descriptor = processor.getPropertyDescriptor(propertyName);
        if (descriptor == null) {
            return null;
        }

        final String setPropertyValue = properties.get(descriptor);
        final String propValue = (setPropertyValue == null) ? descriptor.getDefaultValue() : setPropertyValue;

        return new StandardPropertyValue(propValue, this, preparedQueries.get(descriptor), procNode.getVariableRegistry());
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        verifyTaskActive();
        return new StandardPropertyValue(rawValue, this, Query.prepare(rawValue), procNode.getVariableRegistry());
    }

    @Override
    public void yield() {
        verifyTaskActive();
        procNode.yield();
    }

    @Override
    public ControllerService getControllerService(final String serviceIdentifier) {
        verifyTaskActive();
        return controllerServiceProvider.getControllerServiceForComponent(serviceIdentifier, procNode.getIdentifier());
    }

    @Override
    public int getMaxConcurrentTasks() {
        verifyTaskActive();
        return procNode.getMaxConcurrentTasks();
    }

    @Override
    public String getAnnotationData() {
        verifyTaskActive();
        return procNode.getAnnotationData();
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        verifyTaskActive();
        return properties;
    }

    @Override
    public Map<String, String> getAllProperties() {
        verifyTaskActive();
        final Map<String,String> propValueMap = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : getProperties().entrySet()) {
            propValueMap.put(entry.getKey().getName(), entry.getValue());
        }
        return propValueMap;
    }

    @Override
    public String encrypt(final String unencrypted) {
        verifyTaskActive();
        return encryptor.encrypt(unencrypted);
    }

    @Override
    public String decrypt(final String encrypted) {
        verifyTaskActive();
        return encryptor.decrypt(encrypted);
    }

    @Override
    public Set<String> getControllerServiceIdentifiers(final Class<? extends ControllerService> serviceType) {
        verifyTaskActive();
        if (!serviceType.isInterface()) {
            throw new IllegalArgumentException("ControllerServices may be referenced only via their interfaces; " + serviceType + " is not an interface");
        }
        return controllerServiceProvider.getControllerServiceIdentifiers(serviceType, procNode.getProcessGroup().getIdentifier());
    }

    @Override
    public boolean isControllerServiceEnabled(final ControllerService service) {
        verifyTaskActive();
        return controllerServiceProvider.isControllerServiceEnabled(service);
    }

    @Override
    public boolean isControllerServiceEnabled(final String serviceIdentifier) {
        verifyTaskActive();
        return controllerServiceProvider.isControllerServiceEnabled(serviceIdentifier);
    }

    @Override
    public boolean isControllerServiceEnabling(final String serviceIdentifier) {
        verifyTaskActive();
        return controllerServiceProvider.isControllerServiceEnabling(serviceIdentifier);
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        verifyTaskActive();
        return this;
    }

    @Override
    public Set<Relationship> getAvailableRelationships() {
        verifyTaskActive();
        final Set<Relationship> set = new HashSet<>();
        for (final Relationship relationship : procNode.getRelationships()) {
            final Collection<Connection> connections = procNode.getConnections(relationship);
            if (connections.isEmpty()) {
                set.add(relationship);
            } else {
                boolean available = true;
                for (final Connection connection : connections) {
                    if (connection.getFlowFileQueue().isFull()) {
                        available = false;
                    }
                }

                if (available) {
                    set.add(relationship);
                }
            }
        }

        return set;
    }

    @Override
    public String getControllerServiceName(final String serviceIdentifier) {
        verifyTaskActive();
        return controllerServiceProvider.getControllerServiceName(serviceIdentifier);
    }

    @Override
    public boolean hasIncomingConnection() {
        verifyTaskActive();
        return procNode.hasIncomingConnection();
    }

    @Override
    public boolean hasNonLoopConnection() {
        verifyTaskActive();
        return Connectables.hasNonLoopConnection(procNode);
    }

    @Override
    public boolean hasConnection(final Relationship relationship) {
        verifyTaskActive();
        final Set<Connection> connections = procNode.getConnections(relationship);
        return connections != null && !connections.isEmpty();
    }

    @Override
    public boolean isExpressionLanguagePresent(final PropertyDescriptor property) {
        verifyTaskActive();
        if (property == null || !property.isExpressionLanguageSupported()) {
            return false;
        }

        final List<Range> elRanges = Query.extractExpressionRanges(getProperty(property).getValue());
        return (elRanges != null && !elRanges.isEmpty());
    }

    @Override
    public StateManager getStateManager() {
        verifyTaskActive();
        return stateManager;
    }

    @Override
    public String getName() {
        verifyTaskActive();
        return procNode.getName();
    }

}
