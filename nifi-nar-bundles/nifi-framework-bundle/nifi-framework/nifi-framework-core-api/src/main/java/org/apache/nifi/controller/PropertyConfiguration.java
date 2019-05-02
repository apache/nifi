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
package org.apache.nifi.controller;

import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterReference;
import org.apache.nifi.parameter.ParameterTokenList;
import org.apache.nifi.parameter.StandardParameterTokenList;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class PropertyConfiguration {
    public static PropertyConfiguration EMPTY = new PropertyConfiguration(null, new StandardParameterTokenList(null, Collections.emptyList()));

    private final String rawValue;
    private final ParameterTokenList parameterTokenList;
    private final List<ParameterReference> parameterReferences;
    private final AtomicReference<ComputedEffectiveValue> effectiveValue = new AtomicReference<>();

    public PropertyConfiguration(final String rawValue, final ParameterTokenList tokenList) {
        this.rawValue = rawValue;
        this.parameterTokenList = tokenList;
        this.parameterReferences = tokenList.toReferenceList();
    }

    public String getRawValue() {
        return rawValue;
    }

    public String getEffectiveValue(final ParameterContext parameterContext) {
        if (rawValue == null) {
            return null;
        }

        if (parameterTokenList == null) {
            return rawValue;
        }

        // We don't want to perform the substitution every time this method is called. But we can't just always
        // cache the Effective Value because we may have a different Parameter Context. So, we cache a Tuple of
        // the Parameter Context and the effective value for that Parameter Context.
        final ComputedEffectiveValue computedEffectiveValue = effectiveValue.get();
        if (computedEffectiveValue != null && computedEffectiveValue.matches(parameterContext)) {
            return computedEffectiveValue.getValue();
        }

        final String substituted = parameterTokenList.substitute(parameterContext);
        final ComputedEffectiveValue updatedValue = new ComputedEffectiveValue(parameterContext, substituted);
        effectiveValue.compareAndSet(computedEffectiveValue, updatedValue);
        return substituted;
    }

    public List<ParameterReference> getParameterReferences() {
        return parameterReferences;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null) {
            return false;
        }

        if (!(o instanceof PropertyConfiguration)) {
            return false;
        }

        final PropertyConfiguration that = (PropertyConfiguration) o;
        return Objects.equals(rawValue, that.rawValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawValue);
    }


    public static class ComputedEffectiveValue {
        private final ParameterContext parameterContext;
        private final long contextVersion;
        private final String value;

        public ComputedEffectiveValue(final ParameterContext parameterContext, final String value) {
            this.parameterContext = parameterContext;
            this.contextVersion = parameterContext == null ? -1 : parameterContext.getVersion();
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public boolean matches(final ParameterContext context) {
            if (!Objects.equals(context, this.parameterContext)) {
                return false;
            }

            if (context == null) {
                return true;
            }

            return context.getVersion() == contextVersion;
        }
    }
}
