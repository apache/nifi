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
     * Returns the Parameter with the given descriptor
     *
     * @param parameterDescriptor descriptor for the parameter
     * @return the Parameter with the given name, or <code>null</code> if no parameter exists with the given descriptor
     */
    Optional<Parameter> getParameter(ParameterDescriptor parameterDescriptor);

    /**
     * Returns the Map of all Parameters in this context. Note that the Map that is returned may either be immutable or may be a defensive copy but
     * modifying the Map that is returned will have no effect on the contents of this Parameter Context.
     *
     * @return a Map that contains all Parameters in the context keyed by their descriptors
     */
    Map<ParameterDescriptor, Parameter> getParameters();

    /**
     * Returns the ParameterReferenceManager that is associated with this ParameterContext
     * @return the ParameterReferenceManager that is associated with this ParameterContext
     */
    ParameterReferenceManager getParameterReferenceManager();


}
