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

package org.apache.nifi.controller.reporting;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceContext;
import org.apache.nifi.components.resource.StandardResourceContext;
import org.apache.nifi.components.resource.StandardResourceReferenceFactory;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.reporting.Severity;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AbstractReportingContext implements ReportingContext {
    private final ReportingTask reportingTask;
    private final BulletinRepository bulletinRepository;
    private final ControllerServiceProvider serviceProvider;
    private final Map<PropertyDescriptor, String> properties;
    private final Map<PropertyDescriptor, PreparedQuery> preparedQueries;
    private final ParameterLookup parameterLookup;
    private final VariableRegistry variableRegistry;

    public AbstractReportingContext(final ReportingTask reportingTask, final BulletinRepository bulletinRepository,
                                    final Map<PropertyDescriptor, String> properties, final ControllerServiceProvider controllerServiceProvider,
                                    final ParameterLookup parameterLookup, final VariableRegistry variableRegistry) {

        this.bulletinRepository = bulletinRepository;
        this.properties = Collections.unmodifiableMap(properties);
        this.serviceProvider = controllerServiceProvider;
        this.reportingTask = reportingTask;
        this.parameterLookup = parameterLookup;
        this.variableRegistry = variableRegistry;
        this.preparedQueries = new HashMap<>();

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

    protected ReportingTask getReportingTask() {
        return reportingTask;
    }

    @Override
    public BulletinRepository getBulletinRepository() {
        return bulletinRepository;
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return Collections.unmodifiableMap(properties);
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
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final PropertyDescriptor descriptor = reportingTask.getPropertyDescriptor(property.getName());
        if (descriptor == null) {
            return null;
        }

        final String configuredValue = properties.get(property);
        final ResourceContext resourceContext = new StandardResourceContext(new StandardResourceReferenceFactory(), descriptor);
        return new StandardPropertyValue(resourceContext, configuredValue == null ? descriptor.getDefaultValue() : configuredValue, serviceProvider, parameterLookup, preparedQueries.get(property),
            variableRegistry);
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return serviceProvider;
    }

    @Override
    public Bulletin createBulletin(final String category, final Severity severity, final String message) {
        return BulletinFactory.createBulletin(category, severity.name(), message);
    }

    @Override
    public Bulletin createBulletin(final String componentId, final String category, final Severity severity, final String message) {
        final Connectable connectable = getFlowManager().findConnectable(componentId);
        if (connectable == null) {
            throw new IllegalStateException("Cannot create Component-Level Bulletin because no component can be found with ID " + componentId);
        }
        return BulletinFactory.createBulletin(connectable, category, severity.name(), message);
    }

    protected abstract FlowManager getFlowManager();
}
