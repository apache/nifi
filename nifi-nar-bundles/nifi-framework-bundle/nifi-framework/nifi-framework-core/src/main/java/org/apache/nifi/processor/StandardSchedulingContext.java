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

import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;

public class StandardSchedulingContext implements SchedulingContext {

    private final ProcessContext processContext;
    private final ControllerServiceProvider serviceProvider;
    private final ProcessorNode processorNode;

    public StandardSchedulingContext(final ProcessContext processContext, final ControllerServiceProvider serviceProvider, final ProcessorNode processorNode) {
        this.processContext = processContext;
        this.serviceProvider = serviceProvider;
        this.processorNode = processorNode;
    }

    @Override
    public void leaseControllerService(final String identifier) {
        final ControllerServiceNode serviceNode = serviceProvider.getControllerServiceNode(identifier);
        if (serviceNode == null) {
            throw new IllegalArgumentException("Cannot lease Controller Service because no Controller Service exists with identifier " + identifier);
        }

        if (serviceNode.getState() != ControllerServiceState.ENABLED) {
            throw new IllegalStateException("Cannot lease Controller Service because Controller Service " + serviceNode.getProxiedControllerService() + " is not currently enabled");
        }

        if (!serviceNode.isValid()) {
            throw new IllegalStateException("Cannot lease Controller Service because Controller Service " + serviceNode.getProxiedControllerService() + " is not currently valid");
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
    public boolean hasConnection(Relationship relationship) {
        return processContext.hasConnection(relationship);
    }
}
