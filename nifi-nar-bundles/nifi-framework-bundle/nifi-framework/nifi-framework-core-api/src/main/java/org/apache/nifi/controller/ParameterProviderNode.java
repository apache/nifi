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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterGroupConfiguration;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface ParameterProviderNode extends ComponentNode {

    ParameterProvider getParameterProvider();

    void setParameterProvider(LoggableComponent<ParameterProvider> parameterProvider);

    ConfigurationContext getConfigurationContext();

    String getComments();

    void setComments(String comment);

    void verifyCanFetchParameters();

    void fetchParameters();

    void verifyCanApplyParameters(Collection<ParameterGroupConfiguration> parameterNames);

    Collection<ParameterGroupConfiguration> getParameterGroupConfigurations();

    List<ParametersApplication> getFetchedParametersToApply(Collection<ParameterGroupConfiguration> parameterGroupConfigurations);

    void verifyCanClearState();

    void verifyCanDelete();

    /**
     * @return all ParameterContexts that reference this ParameterProvider
     */
    Set<ParameterContext> getReferences();

    /**
     * Indicates that a parameter context is now referencing this Parameter Provider
     * @param parameterContext the parameter context that references this provider
     */
    void addReference(ParameterContext parameterContext);

    /**
     * Indicates that a parameter context is no longer referencing this Parameter Provider
     * @param parameterContext the parameter context that no longer references this provider
     */
    void removeReference(ParameterContext parameterContext);

    /**
     * Verifies that the given configuration is valid for the Parameter Provider
     *
     * @param context the configuration to verify
     * @param logger a logger that can be used when performing verification
     * @param extensionManager extension manager that is used for obtaining appropriate NAR ClassLoaders
     * @return a list of results indicating whether or not the given configuration is valid
     */
    List<ConfigVerificationResult> verifyConfiguration(ConfigurationContext context, ComponentLog logger, ExtensionManager extensionManager);
}
