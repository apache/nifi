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
package org.apache.nifi.web.controller;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceContext;
import org.apache.nifi.components.resource.StandardResourceContext;
import org.apache.nifi.components.resource.StandardResourceReferenceFactory;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.search.SearchContext;

import java.util.Map;

/**
 *
 */
public class StandardSearchContext implements SearchContext {

    private final String searchTerm;
    private final ProcessorNode processorNode;
    private final ControllerServiceLookup controllerServiceLookup;
    private final VariableRegistry variableRegistry;

    public StandardSearchContext(final String searchTerm, final ProcessorNode processorNode, final ControllerServiceLookup controllerServiceLookup, VariableRegistry variableRegistry) {
        this.searchTerm = searchTerm;
        this.processorNode = processorNode;
        this.controllerServiceLookup = controllerServiceLookup;
        this.variableRegistry = variableRegistry;
    }

    @Override
    public String getSearchTerm() {
        return searchTerm;
    }

    @Override
    public String getAnnotationData() {
        return processorNode.getAnnotationData();
    }

    @Override
    public PropertyValue getProperty(final PropertyDescriptor property) {
        final String configuredValue = processorNode.getRawPropertyValue(property);
        final ResourceContext resourceContext = new StandardResourceContext(new StandardResourceReferenceFactory(), property);
        return new StandardPropertyValue(resourceContext, configuredValue == null ? property.getDefaultValue() : configuredValue, controllerServiceLookup, ParameterLookup.EMPTY, variableRegistry);
    }

    @Override
    public Map<PropertyDescriptor, String> getProperties() {
        return processorNode.getRawPropertyValues();
    }

}
