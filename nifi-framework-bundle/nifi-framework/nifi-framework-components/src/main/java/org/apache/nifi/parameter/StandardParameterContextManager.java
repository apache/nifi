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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class StandardParameterContextManager implements ParameterContextManager {
    private final Map<String, ParameterContext> parameterContexts = new HashMap<>();
    private final Map<String, ParameterContext> nameIndex = new HashMap<>();
    private volatile Map<String, ParameterContext> cachedNameMapping = Collections.emptyMap();

    @Override
    public synchronized boolean hasParameterContext(final String id) {
        return parameterContexts.get(id) != null;
    }

    @Override
    public synchronized ParameterContext getParameterContext(final String id) {
        return parameterContexts.get(id);
    }

    @Override
    public synchronized void addParameterContext(final ParameterContext parameterContext) {
        Objects.requireNonNull(parameterContext);

        final ParameterContext existingById = parameterContexts.get(parameterContext.getIdentifier());
        if (existingById != null && !(existingById instanceof ReferenceOnlyParameterContext)) {
            throw new IllegalStateException("Cannot add Parameter Context because another Parameter Context already exists with the same ID");
        }

        final ParameterContext existingByName = nameIndex.get(parameterContext.getName());
        if (existingByName != null && existingByName != existingById) {
            throw new IllegalStateException("Cannot add Parameter Context because another Parameter Context already exists with the name '" + parameterContext + "'");
        }

        if (existingById instanceof ReferenceOnlyParameterContext) {
            nameIndex.remove(existingById.getName(), existingById);
        }

        parameterContexts.put(parameterContext.getIdentifier(), parameterContext);
        nameIndex.put(parameterContext.getName(), parameterContext);
        updateCachedNameMapping();
    }

    @Override
    public synchronized ParameterContext removeParameterContext(final String parameterContextId) {
        Objects.requireNonNull(parameterContextId);
        final ParameterContext removed = parameterContexts.remove(parameterContextId);
        if (removed != null) {
            nameIndex.remove(removed.getName(), removed);
            updateCachedNameMapping();
        }
        return removed;
    }

    @Override
    public synchronized void setParameterContextName(final String parameterContextId, final String name) {
        Objects.requireNonNull(parameterContextId);
        Objects.requireNonNull(name);

        final ParameterContext parameterContext = parameterContexts.get(parameterContextId);
        if (parameterContext == null) {
            throw new IllegalStateException("Cannot rename Parameter Context because no Parameter Context exists with the specified ID");
        }

        final ParameterContext existingByName = nameIndex.get(name);
        if (existingByName != null && existingByName != parameterContext) {
            throw new IllegalStateException("Cannot rename Parameter Context because another Parameter Context already exists with the name '" + name + "'");
        }

        nameIndex.remove(parameterContext.getName(), parameterContext);
        parameterContext.setName(name);
        nameIndex.put(parameterContext.getName(), parameterContext);
        updateCachedNameMapping();
    }

    @Override
    public synchronized Set<ParameterContext> getParameterContexts() {
        return new HashSet<>(parameterContexts.values());
    }

    @Override
    public Map<String, ParameterContext> getParameterContextNameMapping() {
        return cachedNameMapping;
    }

    private void updateCachedNameMapping() {
        cachedNameMapping = Collections.unmodifiableMap(new HashMap<>(nameIndex));
    }
}
