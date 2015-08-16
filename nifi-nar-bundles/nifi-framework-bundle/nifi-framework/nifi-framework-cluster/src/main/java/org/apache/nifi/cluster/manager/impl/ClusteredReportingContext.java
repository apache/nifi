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
package org.apache.nifi.cluster.manager.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.processor.StandardPropertyValue;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.Severity;

public class ClusteredReportingContext implements ReportingContext {

    private final EventAccess eventAccess;
    private final BulletinRepository bulletinRepository;
    private final ControllerServiceProvider serviceProvider;
    private final Map<PropertyDescriptor, String> properties;
    private final Map<PropertyDescriptor, PreparedQuery> preparedQueries;

    public ClusteredReportingContext(final EventAccess eventAccess, final BulletinRepository bulletinRepository,
            final Map<PropertyDescriptor, String> properties, final ControllerServiceProvider serviceProvider) {
        this.eventAccess = eventAccess;
        this.bulletinRepository = bulletinRepository;
        this.properties = Collections.unmodifiableMap(properties);
        this.serviceProvider = serviceProvider;

        preparedQueries = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
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
    public EventAccess getEventAccess() {
        return eventAccess;
    }

    @Override
    public BulletinRepository getBulletinRepository() {
        return bulletinRepository;
    }

    @Override
    public Bulletin createBulletin(final String category, final Severity severity, final String message) {
        return BulletinFactory.createBulletin(category, severity.name(), message);
    }

    @Override
    public Bulletin createBulletin(final String componentId, final String category, final Severity severity, final String message) {
        final ProcessGroupStatus rootGroupStatus = eventAccess.getControllerStatus();
        final String groupId = findGroupId(rootGroupStatus, componentId);
        final String componentName = findComponentName(rootGroupStatus, componentId);
        final ComponentType componentType = findComponentType(rootGroupStatus, componentId);

        return BulletinFactory.createBulletin(groupId, componentId, componentType, componentName, category, severity.name(), message);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final String configuredValue = properties.get(property);
        return new StandardPropertyValue(configuredValue == null ? property.getDefaultValue() : configuredValue, serviceProvider, preparedQueries.get(property));
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return serviceProvider;
    }

    String findGroupId(final ProcessGroupStatus groupStatus, final String componentId) {
        for (final ProcessorStatus procStatus : groupStatus.getProcessorStatus()) {
            if (procStatus.getId().equals(componentId)) {
                return groupStatus.getId();
            }
        }

        for (final PortStatus portStatus : groupStatus.getInputPortStatus()) {
            if (portStatus.getId().equals(componentId)) {
                return groupStatus.getId();
            }
        }

        for (final PortStatus portStatus : groupStatus.getOutputPortStatus()) {
            if (portStatus.getId().equals(componentId)) {
                return groupStatus.getId();
            }
        }

        for (final ProcessGroupStatus childGroup : groupStatus.getProcessGroupStatus()) {
            final String groupId = findGroupId(childGroup, componentId);
            if (groupId != null) {
                return groupId;
            }
        }

        return null;
    }

    private ComponentType findComponentType(final ProcessGroupStatus groupStatus, final String componentId) {
        for (final ProcessorStatus procStatus : groupStatus.getProcessorStatus()) {
            if (procStatus.getId().equals(componentId)) {
                return ComponentType.PROCESSOR;
            }
        }

        for (final PortStatus portStatus : groupStatus.getInputPortStatus()) {
            if (portStatus.getId().equals(componentId)) {
                return ComponentType.INPUT_PORT;
            }
        }

        for (final PortStatus portStatus : groupStatus.getOutputPortStatus()) {
            if (portStatus.getId().equals(componentId)) {
                return ComponentType.OUTPUT_PORT;
            }
        }

        for (final RemoteProcessGroupStatus remoteStatus : groupStatus.getRemoteProcessGroupStatus()) {
            if (remoteStatus.getId().equals(componentId)) {
                return ComponentType.REMOTE_PROCESS_GROUP;
            }
        }

        for (final ProcessGroupStatus childGroup : groupStatus.getProcessGroupStatus()) {
            final ComponentType type = findComponentType(childGroup, componentId);
            if (type != null) {
                return type;
            }
        }

        final ControllerService service = serviceProvider.getControllerService(componentId);
        if (service != null) {
            return ComponentType.CONTROLLER_SERVICE;
        }

        return null;
    }

    private String findComponentName(final ProcessGroupStatus groupStatus, final String componentId) {
        for (final ProcessorStatus procStatus : groupStatus.getProcessorStatus()) {
            if (procStatus.getId().equals(componentId)) {
                return procStatus.getName();
            }
        }

        for (final PortStatus portStatus : groupStatus.getInputPortStatus()) {
            if (portStatus.getId().equals(componentId)) {
                return groupStatus.getName();
            }
        }

        for (final PortStatus portStatus : groupStatus.getOutputPortStatus()) {
            if (portStatus.getId().equals(componentId)) {
                return groupStatus.getName();
            }
        }

        for (final ProcessGroupStatus childGroup : groupStatus.getProcessGroupStatus()) {
            final String componentName = findComponentName(childGroup, componentId);
            if (componentName != null) {
                return componentName;
            }
        }

        return null;
    }
}
