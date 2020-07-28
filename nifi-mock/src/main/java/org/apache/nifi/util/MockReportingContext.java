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
package org.apache.nifi.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinFactory;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.Severity;

public class MockReportingContext extends MockControllerServiceLookup implements ReportingContext, ControllerServiceLookup {

    private final Map<String, ControllerServiceConfiguration> controllerServices;
    private final MockEventAccess eventAccess = new MockEventAccess();
    private final Map<PropertyDescriptor, String> properties = new HashMap<>();
    private final StateManager stateManager;
    private final VariableRegistry variableRegistry;

    private final Map<String, List<Bulletin>> componentBulletinsCreated = new HashMap<>();

    public MockReportingContext(final Map<String, ControllerService> controllerServices, final StateManager stateManager, final VariableRegistry variableRegistry) {
        this.controllerServices = new HashMap<>();
        this.stateManager = stateManager;
        this.variableRegistry = variableRegistry;
        for (final Map.Entry<String, ControllerService> entry : controllerServices.entrySet()) {
            this.controllerServices.put(entry.getKey(), new ControllerServiceConfiguration(entry.getValue()));
        }
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
        final String configuredValue = properties.get(property);
        return new MockPropertyValue(configuredValue == null ? property.getDefaultValue() : configuredValue, this, variableRegistry);
    }

    public void setProperty(final String propertyName, final String value) {
        this.properties.put(new PropertyDescriptor.Builder().name(propertyName).build(), value);
    }

    public void setProperties(final Map<PropertyDescriptor, String> properties) {
        this.properties.clear();
        this.properties.putAll(properties);
    }

    @Override
    public MockEventAccess getEventAccess() {
        return eventAccess;
    }

    @Override
    public BulletinRepository getBulletinRepository() {
        return new MockBulletinRepository();
    }

    @Override
    public Bulletin createBulletin(final String category, final Severity severity, final String message) {
        return BulletinFactory.createBulletin(category, severity.name(), message);
    }

    @Override
    public Bulletin createBulletin(final String componentId, final String category, final Severity severity, final String message) {
        final Bulletin bulletin = BulletinFactory.createBulletin(null, null, componentId, "test processor", category, severity.name(), message);
        List<Bulletin> bulletins = componentBulletinsCreated.get(componentId);
        if (bulletins == null) {
            bulletins = new ArrayList<>();
            componentBulletinsCreated.put(componentId, bulletins);
        }
        bulletins.add(bulletin);
        return bulletin;
    }

    @Override
    public ControllerServiceLookup getControllerServiceLookup() {
        return this;
    }

    /**
     * @param componentId identifier of component to get bulletins for
     * @return all Bulletins that have been created for the component with the
     * given ID
     */
    public List<Bulletin> getComponentBulletins(final String componentId) {
        final List<Bulletin> created = componentBulletinsCreated.get(componentId);
        if (created == null) {
            return new ArrayList<>();
        }

        return new ArrayList<>(created);
    }

    @Override
    public StateManager getStateManager() {
        return stateManager;
    }

    @Override
    public boolean isClustered() {
        return false;
    }

    @Override
    public String getClusterNodeIdentifier() {
        return null;
    }
}
