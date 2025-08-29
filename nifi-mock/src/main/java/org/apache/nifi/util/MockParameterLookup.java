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

import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterLookup;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;


public class MockParameterLookup implements ParameterLookup {
    private final Map<String, String> parameters;
    private final AtomicLong version = new AtomicLong(1);

    public MockParameterLookup(final Map<String, String> parameters) {
        this.parameters = parameters == null ? Collections.emptyMap() : Collections.unmodifiableMap(parameters);
    }

    @Override
    public Optional<Parameter> getParameter(final String parameterName) {
        final String value = parameters.get(parameterName);
        if (value == null) {
            return Optional.empty();
        }

        return Optional.of(new Parameter.Builder()
            .name(parameterName)
            .value(value)
            .build());
    }

    @Override
    public boolean isEmpty() {
        return parameters.isEmpty();
    }

    @Override
    public long getVersion() {
        return version.get();
    }
}
