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
package org.apache.nifi.stateless.core;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterReferenceManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class StatelessParameterContext implements ParameterContext {
    private final Map<String, Parameter> parameterMap = new HashMap<>();

    public StatelessParameterContext(final Set<Parameter> parameters) {
        for (final Parameter parameter : parameters) {
            parameterMap.put(parameter.getDescriptor().getName(), parameter);
        }
    }

    @Override
    public String getIdentifier() {
        return "NiFi Stateless Parameter Context";
    }

    @Override
    public String getName() {
        return "NiFi Stateless Parameter Context";
    }

    @Override
    public void setName(final String name) {
    }

    @Override
    public String getDescription() {
        return "NiFi Stateless Parameter Context";
    }

    @Override
    public void setDescription(final String description) {
    }

    @Override
    public void setParameters(final Set<Parameter> updatedParameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void verifyCanSetParameters(final Set<Parameter> parameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Parameter> getParameter(final String parameterName) {
        return Optional.ofNullable(parameterMap.get(parameterName));
    }

    @Override
    public Optional<Parameter> getParameter(final ParameterDescriptor parameterDescriptor) {
        return getParameter(parameterDescriptor.getName());
    }

    @Override
    public boolean isEmpty() {
        return parameterMap.isEmpty();
    }

    @Override
    public Map<ParameterDescriptor, Parameter> getParameters() {
        final Map<ParameterDescriptor, Parameter> map = new HashMap<>();
        for (final Parameter parameter : parameterMap.values()) {
            map.put(parameter.getDescriptor(), parameter);
        }

        return map;
    }

    @Override
    public ParameterReferenceManager getParameterReferenceManager() {
        return null;
    }

    @Override
    public long getVersion() {
        return 0;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return null;
    }

    @Override
    public Resource getResource() {
        return null;
    }
}
