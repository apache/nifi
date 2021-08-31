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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ParametersApplication;
import org.apache.nifi.controller.parameter.ParameterProviderLookup;
import org.apache.nifi.parameter.ParameterGroupConfiguration;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.ParameterProviderDTO;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ParameterProviderDAO extends ParameterProviderLookup {

    /**
     * Determines if the specified parameter provider exists.
     *
     * @param parameterProviderId id
     * @return true if parameter provider exists
     */
    boolean hasParameterProvider(String parameterProviderId);

    /**
     * Determines whether this parameter provider can be created.
     *
     * @param parameterProviderDTO dto
     */
    void verifyCreate(ParameterProviderDTO parameterProviderDTO);

    /**
     * Creates a parameter provider.
     *
     * @param parameterProviderDTO The parameter provider DTO
     * @return The parameter provider
     */
    ParameterProviderNode createParameterProvider(ParameterProviderDTO parameterProviderDTO);

    /**
     * Gets all of the parameter providers.
     *
     * @return The parameter providers
     */
    Set<ParameterProviderNode> getParameterProviders();

    /**
     * Updates the specified parameter provider.
     *
     * @param parameterProviderDTO The parameter provider DTO
     * @return The parameter provider
     */
    ParameterProviderNode updateParameterProvider(ParameterProviderDTO parameterProviderDTO);

    /**
     * Determines whether this parameter provider can be updated.
     *
     * @param parameterProviderDTO dto
     */
    void verifyUpdate(ParameterProviderDTO parameterProviderDTO);

    /**
     * Verifies the Parameter Provider is in a state in which its configuration can be verified
     * @param parameterProviderId the id of the Parameter Provider
     */
    void verifyConfigVerification(String parameterProviderId);

    /**
     * Performs verification of the Configuration for the Parameter Provider with the given ID
     * @param parameterProviderId the id of the Parameter Provider
     * @param properties the configured properties to verify
     * @return verification results
     */
    List<ConfigVerificationResultDTO> verifyConfiguration(String parameterProviderId, Map<String, String> properties);

    /**
     * Verifies the specified parameter provider is able to fetch its parameters.
     *
     * @param parameterProviderId parameter provider id
     */
    void verifyCanFetchParameters(String parameterProviderId);

    /**
     * Fetches the parameters and caches them in the parameter provider.
     * @param parameterProviderId parameter provider id
     * @return The parameter provider
     */
    ParameterProviderNode fetchParameters(String parameterProviderId);

    /**
     * Verifies the specified parameter provider is able to apply its parameters to the flow.
     *
     * @param parameterProviderId parameter provider id
     * @param parameterNames A set of fetched parameter names to include.  Any parameters not found in this set will not be included in the update verification.
     */
    void verifyCanApplyParameters(String parameterProviderId, Collection<ParameterGroupConfiguration> parameterNames);

    /**
     * Returns a ParametersApplication for each referenced ParameterContext, encapsulating
     * all changed, new, or removed parameters resulting from the Fetch operation.  This is essentially
     * a diff from the existing parameters to the fetched parameters.  A null parameter indicates a removal.
     * @param parameterProviderId parameter provider id
     * @param parameterNames A set of fetched parameter names to include.  Any parameters not found in this set will not be included in the update.
     * @return list of ParametersApplication objects
     */
    List<ParametersApplication> getFetchedParametersToApply(String parameterProviderId, Collection<ParameterGroupConfiguration> parameterNames);

    /**
     * Determines whether this parameter provider can be removed.
     *
     * @param parameterProviderId id
     */
    void verifyDelete(String parameterProviderId);

    /**
     * Deletes the specified parameter provider.
     *
     * @param parameterProviderId The parameter provider id
     */
    void deleteParameterProvider(String parameterProviderId);

    /**
     * Gets the specified parameter provider.
     *
     * @param parameterProviderId parameter provider id
     * @return state map
     */
    StateMap getState(String parameterProviderId, Scope scope);

    /**
     * Verifies the parameter provider can clear state.
     *
     * @param parameterProviderId parameter provider id
     */
    void verifyClearState(String parameterProviderId);

    /**
     * Clears the state of the specified parameter provider.
     *
     * @param parameterProviderId parameter provider id
     */
    void clearState(String parameterProviderId);

}
