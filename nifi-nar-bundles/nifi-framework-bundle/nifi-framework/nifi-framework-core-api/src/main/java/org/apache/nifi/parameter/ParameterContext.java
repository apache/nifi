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

import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.controller.parameter.ParameterProviderLookup;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ParameterContext extends ParameterLookup, ComponentAuthorizable {

    /**
     * @return the UUID for this Parameter Context
     */
    String getIdentifier();

    /**
     * @return the name of the Parameter Context
     */
    String getName();

    /**
     * Sets the name of the Parameter Context
     * @param name the name of the Parameter Context
     */
    void setName(String name);

    /**
     * @return a user-supplied description for the Parameter Context
     */
    String getDescription();

    /**
     * Sets the description for the Parameter Context
     * @param description the description
     */
    void setDescription(String description);

    /**
     * Updates the Parameters within this context to match the given set of Parameters. If the Parameter Context contains any parameters that are not in
     * the given set of updated Parameters, those parameters are unaffected. However, if the Map contains any key with a <code>null</code> value, the
     * parameter whose name is given by the key will be removed
     *
     * @param updatedParameters the updated set of parameters, keyed by Parameter name
     * @throws IllegalStateException if any parameter is modified or removed and that parameter is being referenced by a running Processor or an enabled Controller Service, or if
     * an update would result in changing the sensitivity of any parameter
     */
    void setParameters(Map<String, Parameter> updatedParameters);

    /**
     * Ensures that it is legal to update the Parameters for this Parameter Context to match the given set of Parameters
     * @param parameters the updated set of parameters, keyed by Parameter name
     * @throws IllegalStateException if setting the given set of Parameters is not legal
     */
    void verifyCanSetParameters(Map<String, Parameter> parameters);

    /**
     * Returns the Parameter with the given descriptor, considering this and all inherited
     * ParameterContexts.
     *
     * @param parameterDescriptor descriptor for the parameter
     * @return the Parameter with the given name, or <code>null</code> if no parameter exists with the given descriptor
     */
    Optional<Parameter> getParameter(ParameterDescriptor parameterDescriptor);

    /**
     * Checks whether this ParameterContext would still have an effective value for the given parameter if the
     * parameter was removed from this or any inherited parameter context, no matter how indirect.  This allows
     * the ParameterContext to be checked for validity: if it will still have an effective value, the parameter
     * can be safely removed.
     *
     * @param parameterDescriptor parameter descriptor to check
     * @return True if, when the parameter is removed, this ParameterContext would still have an effective value
     * for the parameter.
     */
    boolean hasEffectiveValueIfRemoved(ParameterDescriptor parameterDescriptor);

    /**
     * Returns the Map of all Parameters in this context (not in any inherited ParameterContexts). Note that the Map that
     * is returned may either be immutable or may be a defensive copy but modifying the Map that is returned will have
     * no effect on the contents of this Parameter Context.
     *
     * @return a Map that contains all Parameters in the context keyed by their descriptors
     */
    Map<ParameterDescriptor, Parameter> getParameters();

    /**
     * Returns the Map of all Parameters in this context, as well as in all inherited ParameterContexts.  Any duplicate
     * parameters will be overridden as described in {@link #setInheritedParameterContexts(List) setParameterContexts}.
     * Note that the Map that is returned may either be immutable or may be a defensive copy but
     * modifying the Map that is returned will have no effect on the contents of this Parameter Context or any other.
     *
     * @return a Map that contains all Parameters in the context and all nested ParameterContexts, keyed by their descriptors
     */
    Map<ParameterDescriptor, Parameter> getEffectiveParameters();

    /**
     * Returns the ParameterReferenceManager that is associated with this ParameterContext
     * @return the ParameterReferenceManager that is associated with this ParameterContext
     */
    ParameterReferenceManager getParameterReferenceManager();

    /**
     * Verifies whether the parameter context can be updated with the provided parameters and inherited parameter contexts.
     * @param parameterUpdates A map from parameter name to updated parameter (null if removal is desired)
     * @param inheritedParameterContexts the list of ParameterContexts from which to inherit parameters
     */
    void verifyCanUpdateParameterContext(Map<String, Parameter> parameterUpdates, List<ParameterContext> inheritedParameterContexts);

    /**
     * Returns the ParameterProviderLookup that is associated with this ParameterContext
     * @return the ParameterProviderLookup that is associated with this ParameterContext
     */
    ParameterProviderLookup getParameterProviderLookup();

    /**
     * Returns a map from parameter name to Parameter, representing all parameters that would be effectively
     * updated if the provided configuration was applied.  Only parameters that would be effectively updated or added are
     * included in the map.  A null value for a Parameter represents an effective deletion of that parameter name.
     * @param parameters A proposed map from parameter name to Parameter (if Parameter is null, this represents a deletion)
     * @param inheritedParameterContexts A proposed list of inherited parameter contexts
     * @param sensitiveParameterProvider A proposed sensitive ParameterProvider (null if it should be unset)
     * @param nonSensitiveParameterProvider A proposed non-sensitive ParameterProvider (null if it should be unset)
     * @return A map of effective parameter updates
     */
    Map<String, Parameter> getEffectiveParameterUpdates(final Map<String, Parameter> parameters, final List<ParameterContext> inheritedParameterContexts,
                                                        final SensitiveParameterProvider sensitiveParameterProvider, final NonSensitiveParameterProvider nonSensitiveParameterProvider);

    /**
     *
     * @return The {@link SensitiveParameterProvider}, or <code>Optional.empty()</code>
     * if none is set.  In the latter case, sensitive Parameters are set manually.
     */
    Optional<SensitiveParameterProvider> getSensitiveParameterProvider();

    /**
     * If a provider is specified, ensures the SensitiveParameterProvider can be set (and any user-entered sensitive parameters removed).
     * If <code>null</code> is specified, ensures any existing provided sensitive parameters can be removed.
     * @param parameterProvider A SensitiveParameterProvider
     * @throws IllegalStateException if setting the given ParameterProvider would not be legal
     */
    void verifyCanSetSensitiveParameterProvider(SensitiveParameterProvider parameterProvider);

    /**
     * Sets a {@link SensitiveParameterProvider} that will be used to provide sensitive Parameters.
     * @param parameterProvider A SensitiveParameterProvider.
     */
    void setSensitiveParameterProvider(SensitiveParameterProvider parameterProvider);

    /**
     *
     * @return The {@link NonSensitiveParameterProvider}, or <code>Optional.empty()</code>
     * if none is set.  In the latter case, non-sensitive Parameters are set manually.
     */
    Optional<NonSensitiveParameterProvider> getNonSensitiveParameterProvider();

    /**
     * If a provider is specified, ensures the NonSensitiveParameterProvider can be set (and any user-entered non-sensitive parameters removed).
     * If <code>null</code> is specified, ensures any existing provided non-sensitive parameters can be removed.
     * @param parameterProvider A NonSensitiveParameterProvider
     * @throws IllegalStateException if setting the given ParameterProvider would not be legal
     */
    void verifyCanSetNonSensitiveParameterProvider(NonSensitiveParameterProvider parameterProvider);

    /**
     * Sets a {@link NonSensitiveParameterProvider} that will be used to provide non-sensitive Parameters.
     * @param parameterProvider A NonSensitiveParameterProvider.
     */
    void setNonSensitiveParameterProvider(NonSensitiveParameterProvider parameterProvider);

    /**
     * Updates the ParameterContexts within this context to match the given list of ParameterContexts. All parameter in these
     * ParameterContexts are inherited by this ParameterContext, and can be referenced as if they were actually in this ParameterContext.
     * The order of the list specifies the priority of parameter overriding, where parameters in the first ParameterContext in the list have
     * top priority. However, all parameters in this ParameterContext take precedence over any in its list of inherited ParameterContexts.
     * Note that this method should only update the ordering of the ParameterContexts, it cannot be used to modify the
     * contents of the ParameterContexts in the list.
     *
     * @param inheritedParameterContexts the list of ParameterContexts from which to inherit parameters, in priority order first to last
     * @throws IllegalStateException if the list of ParameterContexts is invalid (in case of a circular reference or
     * in case {@link #verifyCanSetParameters(Map) verifyCanSetParameters} would throw an exception)
     */
    void setInheritedParameterContexts(List<ParameterContext> inheritedParameterContexts);

    /**
     * Returns a list of ParameterContexts from which this ParameterContext inherits parameters.
     * See {@link #setInheritedParameterContexts(List) setParameterContexts} for further information.  Note that the List that is returned may
     * either be immutable or may be a defensive copy but modifying the list will not update the ParameterContexts inherited by this one.
     * @return An ordered list of ParameterContexts from which this one inherits parameters
     */
    List<ParameterContext> getInheritedParameterContexts();

    /**
     * Returns a list of names of ParameterContexts from which this ParameterContext inherits parameters.
     * See {@link #setInheritedParameterContexts(List) setParameterContexts} for further information.  Note that the List that is returned may
     * either be immutable or may be a defensive copy but modifying the list will not update the ParameterContexts inherited by this one.
     * @return An ordered list of ParameterContext names from which this one inherits parameters
     */
    List<String> getInheritedParameterContextNames();

    /**
     * Returns true if this ParameterContext inherits from the given parameter context, either
     * directly or indirectly.
     * @param parameterContextId The ID of the sought parameter context
     * @return True if this inherits from the given ParameterContext
     */
    boolean inheritsFrom(String parameterContextId);
}
