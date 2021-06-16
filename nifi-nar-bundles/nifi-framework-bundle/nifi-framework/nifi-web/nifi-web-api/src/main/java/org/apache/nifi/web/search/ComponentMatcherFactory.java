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
package org.apache.nifi.web.search;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.web.search.attributematchers.AttributeMatcher;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ComponentMatcherFactory {
    public ComponentMatcher<Connectable> getInstanceForConnectable(final List<AttributeMatcher<Connectable>> attributeMatchers) {
        return new AttributeBasedComponentMatcher<>(attributeMatchers, component -> component.getIdentifier(), component -> component.getName());
    }

    public ComponentMatcher<Connection> getInstanceForConnection(final List<AttributeMatcher<Connection>> attributeMatchers) {
        return new AttributeBasedComponentMatcher<>(attributeMatchers, component -> component.getIdentifier(), new GetConnectionName());
    }

    public ComponentMatcher<Parameter> getInstanceForParameter(final List<AttributeMatcher<Parameter>> attributeMatchers) {
        return new AttributeBasedComponentMatcher<>(attributeMatchers, component -> component.getDescriptor().getName(), component -> component.getDescriptor().getName());
    }

    public ComponentMatcher<ParameterContext> getInstanceForParameterContext(final List<AttributeMatcher<ParameterContext>> attributeMatchers) {
        return new AttributeBasedComponentMatcher<>(attributeMatchers, component -> component.getIdentifier(), component -> component.getName());
    }

    public ComponentMatcher<ProcessGroup> getInstanceForProcessGroup(final List<AttributeMatcher<ProcessGroup>> attributeMatchers) {
        return new AttributeBasedComponentMatcher<>(attributeMatchers, component -> component.getIdentifier(), component -> component.getName());
    }

    public ComponentMatcher<RemoteProcessGroup> getInstanceForRemoteProcessGroup(final List<AttributeMatcher<RemoteProcessGroup>> attributeMatchers) {
        return new AttributeBasedComponentMatcher<>(attributeMatchers, component -> component.getIdentifier(), component -> component.getName());
    }

    public ComponentMatcher<Label> getInstanceForLabel(final List<AttributeMatcher<Label>> attributeMatchers) {
        return new AttributeBasedComponentMatcher<>(attributeMatchers, component -> component.getIdentifier(), component -> component.getValue());
    }

    public ComponentMatcher<ControllerServiceNode> getInstanceForControllerServiceNode(final List<AttributeMatcher<ControllerServiceNode>> attributeMatchers) {
        return new AttributeBasedComponentMatcher<>(attributeMatchers, component -> component.getIdentifier(), component -> component.getName());
    }

    private static class GetConnectionName implements Function<Connection, String> {
        private static final String DEFAULT_NAME_PREFIX = "From source ";
        private static final String SEPARATOR = ", ";

        public String apply(final Connection component) {
            String result = null;

            if (StringUtils.isNotBlank(component.getName())) {
                result = component.getName();
            } else if (!component.getRelationships().isEmpty()) {
                result = component.getRelationships().stream()
                        .filter(relationship -> StringUtils.isNotBlank(relationship.getName()))
                        .map(relationship -> relationship.getName())
                        .collect(Collectors.joining(SEPARATOR));
            }

            return result == null
                    ? DEFAULT_NAME_PREFIX + component.getSource().getName()
                    : result;
        }
    }
}
