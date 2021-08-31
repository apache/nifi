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

import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterProvider;

import java.util.Map;
import java.util.Set;

public interface ParameterProviderNode extends ComponentNode {

    ParameterProvider getParameterProvider();

    void setParameterProvider(LoggableComponent<ParameterProvider> parameterProvider);

    ConfigurationContext getConfigurationContext();

    String getComments();

    void setComments(String comment);

    void verifyCanFetchParameters();

    void fetchParameters();

    void verifyCanApplyParameters(Set<String> parameterNames);

    Set<String> getFetchedParameterNames();

    Map<ParameterContext, Map<String, Parameter>> getFetchedParametersToApply(Set<String> parameterNames);

    void verifyCanClearState();

    void verifyCanDelete();

    boolean isSensitiveParameterProvider();

    /**
     * Returns all ParameterContexts that reference this ParameterProvider
     * @return
     */
    Set<ParameterContext> getReferences();

    /**
     * Indicates that the given parameter context is now referencing this Parameter Provider
     * @param parameterContext the parameter context referencing this provider
     */
    void addReference(ParameterContext parameterContext);

    /**
     * Indicates that the given parameter context is no longer referencing this Parameter Provider
     * @param parameterContext the parameter context referencing this provider
     */
    void removeReference(ParameterContext parameterContext);
}
