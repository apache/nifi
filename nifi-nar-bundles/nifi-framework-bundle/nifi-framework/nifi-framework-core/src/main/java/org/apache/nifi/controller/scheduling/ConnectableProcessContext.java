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
package org.apache.nifi.controller.scheduling;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.Connectables;

/**
 * This class is essentially an empty shell for {@link Connectable}s that are not Processors
 */
public class ConnectableProcessContext implements ProcessContext {

    private final Connectable connectable;
    private final StringEncryptor encryptor;
    private final StateManager stateManager;

    public ConnectableProcessContext(final Connectable connectable, final StringEncryptor encryptor, final StateManager stateManager) {
        this.connectable = connectable;
        this.encryptor = encryptor;
        this.stateManager = stateManager;
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor descriptor) {
        return getProperty(descriptor.getName());
    }

    @Override
    public PropertyValue getProperty(final String propertyName) {
        // None of the connectable components other than Processor's will ever need to evaluate these.
        // Since Processors use a different implementation of ProcessContext all together, we will just
        // return null for all values
        return new PropertyValue() {
            @Override
            public String getValue() {
                return null;
            }

            @Override
            public Integer asInteger() {
                return null;
            }

            @Override
            public Long asLong() {
                return null;
            }

            @Override
            public Boolean asBoolean() {
                return null;
            }

            @Override
            public Float asFloat() {
                return null;
            }

            @Override
            public Double asDouble() {
                return null;
            }

            @Override
            public Long asTimePeriod(final TimeUnit timeUnit) {
                return null;
            }

            @Override
            public Double asDataSize(final DataUnit dataUnit) {
                return null;
            }

            @Override
            public PropertyValue evaluateAttributeExpressions() throws ProcessException {
                return this;
            }

            @Override
            public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile) throws ProcessException {
                return this;
            }

            @Override
            public PropertyValue evaluateAttributeExpressions(final AttributeValueDecorator decorator) throws ProcessException {
                return this;
            }

            @Override
            public PropertyValue evaluateAttributeExpressions(final FlowFile flowFile, final AttributeValueDecorator decorator) throws ProcessException {
                return this;
            }

            @Override
            public ControllerService asControllerService() {
                return null;
            }

            @Override
            public <T extends ControllerService> T asControllerService(Class<T> serviceType) throws IllegalArgumentException {
                return null;
            }

            @Override
            public boolean isSet() {
                return false;
            }

            @Override
            public PropertyValue evaluateAttributeExpressions(Map<String, String> attributes) throws ProcessException {
                return null;
            }

            @Override
            public PropertyValue evaluateAttributeExpressions(FlowFile flowFile, Map<String, String> additionalAttributes) throws ProcessException {
                return null;
            }

            @Override
            public PropertyValue evaluateAttributeExpressions(Map<String, String> attributes, AttributeValueDecorator decorator) throws ProcessException {
                return null;
            }

            @Override
            public PropertyValue evaluateAttributeExpressions(FlowFile flowFile, Map<String, String> additionalAttributes, AttributeValueDecorator decorator) throws ProcessException {
                return null;
            }

            @Override
            public PropertyValue evaluateAttributeExpressions(FlowFile flowFile, Map<String, String> additionalAttributes, AttributeValueDecorator decorator, Map<String, String> stateValues)
                    throws ProcessException {
                return null;
            }

            @Override
            public boolean isExpressionLanguagePresent() {
                return false;
            }
        };
    }

    @Override
    public PropertyValue newPropertyValue(String rawValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void yield() {
        connectable.yield();
    }

    @Override
    public int getMaxConcurrentTasks() {
        return connectable.getMaxConcurrentTasks();
    }

    @Override
    public String getAnnotationData() {
        return null;
    }

    @Override
    public Map<String, String> getAllProperties() {
        return new HashMap<>();
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return new HashMap<>();
    }

    @Override
    public String decrypt(String encrypted) {
        return encryptor.decrypt(encrypted);
    }

    @Override
    public String encrypt(String unencrypted) {
        return encryptor.encrypt(unencrypted);
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return null;
    }

    @Override
    public Set<Relationship> getAvailableRelationships() {
        for (final Connection connection : connectable.getConnections()) {
            if (connection.getFlowFileQueue().isFull()) {
                return Collections.emptySet();
            }
        }

        final Collection<Relationship> relationships = connectable.getRelationships();
        if (relationships instanceof Set) {
            return (Set<Relationship>) relationships;
        }
        return new HashSet<>(connectable.getRelationships());
    }

    @Override
    public boolean hasIncomingConnection() {
        return connectable.hasIncomingConnection();
    }

    @Override
    public boolean hasNonLoopConnection() {
        return Connectables.hasNonLoopConnection(connectable);
    }

    @Override
    public boolean hasConnection(Relationship relationship) {
        Set<Connection> connections = connectable.getConnections(relationship);
        return connections != null && !connections.isEmpty();
    }

    @Override
    public boolean isExpressionLanguagePresent(PropertyDescriptor property) {
        return false;
    }

    @Override
    public StateManager getStateManager() {
        return stateManager;
    }

    @Override
    public String getName() {
        return connectable.getName();
    }
}
