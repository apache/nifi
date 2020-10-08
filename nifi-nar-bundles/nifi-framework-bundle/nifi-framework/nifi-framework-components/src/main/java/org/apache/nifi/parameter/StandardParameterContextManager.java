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
package org.apache.nifi.parameter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class StandardParameterContextManager implements ParameterContextManager {
    private final Map<String, ParameterContext> parameterContexts = new HashMap<>();

    @Override
    public synchronized ParameterContext getParameterContext(final String id) {
        return parameterContexts.get(id);
    }

    @Override
    public synchronized void addParameterContext(final ParameterContext parameterContext) {
        Objects.requireNonNull(parameterContext);

        if (parameterContexts.containsKey(parameterContext.getIdentifier())) {
            throw new IllegalStateException("Cannot add Parameter Context because another Parameter Context already exists with the same ID");
        }

        for (final ParameterContext context : parameterContexts.values()) {
            if (context.getName().equals(parameterContext.getName())) {
                throw new IllegalStateException("Cannot add Parameter Context because another Parameter Context already exists with the name '" + parameterContext + "'");
            }
        }

        parameterContexts.put(parameterContext.getIdentifier(), parameterContext);
    }

    @Override
    public synchronized ParameterContext removeParameterContext(final String parameterContextId) {
        Objects.requireNonNull(parameterContextId);
        return parameterContexts.remove(parameterContextId);
    }

    @Override
    public synchronized Set<ParameterContext> getParameterContexts() {
        return new HashSet<>(parameterContexts.values());
    }
}
