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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.Query.Range;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;

public class StandardSchedulingContext implements SchedulingContext {

    private final ProcessContext processContext;
    private final ControllerServiceProvider serviceProvider;
    private final ProcessorNode processorNode;
    private final StateManager stateManager;

    public StandardSchedulingContext(final ProcessContext processContext, final ControllerServiceProvider serviceProvider, final ProcessorNode processorNode, final StateManager stateManager) {
        this.processContext = processContext;
        this.serviceProvider = serviceProvider;
        this.processorNode = processorNode;
        this.stateManager = stateManager;
    }

    @Override
    public void leaseControllerService(final String identifier) {
        final ControllerServiceNode serviceNode = serviceProvider.getControllerServiceNode(identifier);
        if (serviceNode == null) {
            throw new IllegalArgumentException("Cannot lease Controller Service because no Controller Service exists with identifier " + identifier);
        }

        if (serviceNode.getState() != ControllerServiceState.ENABLED) {
            throw new IllegalStateException("Cannot lease Controller Service because Controller Service " + serviceNode.getProxiedControllerService().getIdentifier() + " is not currently enabled");
        }

        if (!serviceNode.isValid()) {
            throw new IllegalStateException("Cannot lease Controller Service because Controller Service " + serviceNode.getProxiedControllerService().getIdentifier() + " is not currently valid");
        }

        serviceNode.addReference(processorNode);
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor descriptor) {
        return processContext.getProperty(descriptor);
    }

    @Override
    public PropertyValue getProperty(final String propertyName) {
        return processContext.getProperty(propertyName);
    }

    @Override
    public PropertyValue newPropertyValue(final String rawValue) {
        return processContext.newPropertyValue(rawValue);
    }

    @Override
    public void yield() {
        processContext.yield();
    }

    @Override
    public int getMaxConcurrentTasks() {
        return processContext.getMaxConcurrentTasks();
    }

    @Override
    public String getAnnotationData() {
        return processContext.getAnnotationData();
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return processContext.getProperties();
    }

    @Override
    public Map<String, String> getAllProperties() {
        final Map<String,String> propValueMap = new LinkedHashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : getProperties().entrySet()) {
            propValueMap.put(entry.getKey().getName(), entry.getValue());
        }
        return propValueMap;
    }

    @Override
    public String encrypt(final String unencrypted) {
        return processContext.encrypt(unencrypted);
    }

    @Override
    public String decrypt(final String encrypted) {
        return processContext.decrypt(encrypted);
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return processContext.getControllerServiceLookup();
    }

    @Override
    public Set<Relationship> getAvailableRelationships() {
        return processContext.getAvailableRelationships();
    }

    @Override
    public boolean hasIncomingConnection() {
        return processContext.hasIncomingConnection();
    }

    @Override
    public boolean hasNonLoopConnection() {
        return processContext.hasNonLoopConnection();
    }

    @Override
    public boolean hasConnection(Relationship relationship) {
        return processContext.hasConnection(relationship);
    }

    @Override
    public boolean isExpressionLanguagePresent(PropertyDescriptor property) {
        if (property == null || !property.isExpressionLanguageSupported()) {
            return false;
        }

        final List<Range> elRanges = Query.extractExpressionRanges(getProperty(property).getValue());
        return (elRanges != null && !elRanges.isEmpty());
    }

    @Override
    public StateManager getStateManager() {
        return stateManager;
    }

    @Override
    public String getName() {
        return processorNode.getName();
    }
}
