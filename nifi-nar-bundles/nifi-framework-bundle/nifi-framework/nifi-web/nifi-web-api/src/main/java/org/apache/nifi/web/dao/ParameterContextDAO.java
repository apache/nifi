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
package org.apache.nifi.web.dao;

import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextLookup;
import org.apache.nifi.web.api.dto.ParameterContextDTO;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ParameterContextDAO extends ParameterContextLookup {

    /**
     * Determines whether this parameter context can be created.
     *
     * @param parameterContextDto dto
     */
    void verifyCreate(ParameterContextDTO parameterContextDto);

    /**
     * Creates a parameter context.
     *
     * @param parameterContextDto The parameter context dto
     * @return The parameter context
     */
    ParameterContext createParameterContext(ParameterContextDTO parameterContextDto);

    /**
     * Returns a map from parameter name to intended parameter, given the DTO.
     * @param parameterContextDto A parameter context DTO containing parameter updates
     * @param context The existing parameter context
     * @return The resulting parameter map containing updated parameters (or removals)
     */
    Map<String, Parameter> getParameters(ParameterContextDTO parameterContextDto, ParameterContext context);

    /**
     * Gets all of the parameter contexts.
     *
     * @return The parameter contexts
     */
    Set<ParameterContext> getParameterContexts();

    /**
     * Updates the specified parameter context
     *
     * @param parameterContextDto The parameter context DTO
     * @return The parameter context
     */
    ParameterContext updateParameterContext(ParameterContextDTO parameterContextDto);

    /**
     * Returns a list of the inherited parameter contexts proposed by the DTO.
     * @param parameterContextDto The parameter context DTO
     * @return a list of the inherited parameter contexts proposed by the DTO
     */
    List<ParameterContext> getInheritedParameterContexts(ParameterContextDTO parameterContextDto);

    /**
     * Determines whether this parameter context can be updated.
     *
     * @param parameterContextDto dto
     * @param verifyComponentStates if <code>true</code>, will ensure that any processor referencing the parameter context is stopped/disabled and any controller service referencing the parameter
     * context is disabled. If <code>false</code>, these verifications will not be performed.
     */
    void verifyUpdate(ParameterContextDTO parameterContextDto, boolean verifyComponentStates);

    /**
     * Determines whether this parameter context can be removed.
     *
     * @param parameterContextId id
     */
    void verifyDelete(String parameterContextId);

    /**
     * Deletes the specified Parameter Context
     *
     * @param parameterContextId the ID of the Parameter Context
     */
    void deleteParameterContext(String parameterContextId);
}
