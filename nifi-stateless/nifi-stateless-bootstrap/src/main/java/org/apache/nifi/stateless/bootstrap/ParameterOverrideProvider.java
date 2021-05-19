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

package org.apache.nifi.stateless.bootstrap;

import org.apache.nifi.stateless.config.ParameterOverride;
import org.apache.nifi.stateless.config.ParameterProvider;

import java.util.List;
import java.util.Objects;

public class ParameterOverrideProvider implements ParameterProvider {
    private final List<ParameterOverride> parameterOverrides;

    public ParameterOverrideProvider(final List<ParameterOverride> overrides) {
        this.parameterOverrides = overrides;
    }

    @Override
    public String getParameterValue(final String contextName, final String parameterName) {
        final ParameterOverride override = getParameterOverride(contextName, parameterName);
        return (override == null) ? null : override.getParameterValue();
    }

    @Override
    public boolean isParameterDefined(final String contextName, final String parameterName) {
        final ParameterOverride override = getParameterOverride(contextName, parameterName);
        return override != null;
    }

    private ParameterOverride getParameterOverride(final String contextName, final String parameterName) {
        for (final ParameterOverride override : parameterOverrides) {
            if ((override.getContextName() == null || Objects.equals(override.getContextName(), contextName)) && Objects.equals(override.getParameterName(), parameterName)) {
                return override;
            }
        }

        return null;
    }
}
