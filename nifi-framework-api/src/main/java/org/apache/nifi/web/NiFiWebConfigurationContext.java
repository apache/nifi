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
package org.apache.nifi.web;

import java.util.Collection;
import java.util.Map;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.registry.VariableRegistryProvider;

/**
 * NiFi web context providing limited access to dataflow configuration for
 * component custom UIs.
 */
public interface NiFiWebConfigurationContext extends VariableRegistryProvider {

    /**
     * @param serviceIdentifier of the controller service
     * @param componentId the id of the component that is referencing the controller service
     * @return the ControllerService for the specified identifier. If a
     * corresponding service cannot be found, null is returned. If this NiFi is
     * clustered, the only services available will be those those availability
     * is NCM only
     */
    ControllerService getControllerService(String serviceIdentifier, String componentId);

    /**
     * Provides a mechanism for custom UIs to save actions to appear in NiFi
     * configuration history. Note all fields within each Action must be
     * populated. Null values will result in a failure to insert the audit
     * record. Since the saving to these actions is separate from the actual
     * configuration change, a failure to insert here will just generate a
     * warning log message. The recording of these actions typically happens
     * after a configuration change is applied. Since those changes have already
     * been applied to the flow, we cannot revert them because of a failure to
     * insert an audit record.
     *
     * @param requestContext context of the request
     * @param actions to save
     * @throws IllegalArgumentException When the requestContext isn't fully
     * populated or isn't appropriate for the given request
     */
    void saveActions(NiFiWebRequestContext requestContext, Collection<ConfigurationAction> actions);

    /**
     * @return the current user identity. The value may be a DN, an email, a username, or any string that identities the user. Returns null if no user is found
     */
    String getCurrentUserIdentity();

    /**
     * Sets the annotation data for the underlying component.
     *
     * @param configurationContext config context
     * @param annotationData the data
     * @param properties component properties
     * @return the configuration for the underlying component
     * @throws ResourceNotFoundException if the underlying component does not
     * exit
     * @throws InvalidRevisionException if a revision other than the current
     * revision is given
     * @throws ClusterRequestException if the annotation data was unable to be
     * set for the underlying component. This exception will only be thrown when
     * operating in a cluster.
     * @throws IllegalArgumentException When the requestContext isn't fully
     * populated or isn't appropriate for the given request
     */
    ComponentDetails updateComponent(NiFiWebConfigurationRequestContext configurationContext, String annotationData, Map<String, String> properties)
            throws ResourceNotFoundException, InvalidRevisionException, ClusterRequestException;


    /**
     * Gets the details for the underlying component (including configuration,
     * validation errors, and annotation data).
     *
     * @param requestContext context of request
     * @return the configuration for the underlying component
     * @throws ResourceNotFoundException if the underlying component does not
     * exit
     * @throws ClusterRequestException if the underlying component was unable to
     * be retrieved from the cluster. This exception will only be thrown when
     * operating in a cluster.
     * @throws IllegalArgumentException When the requestContext isn't fully
     * populated or isn't appropriate for the given request
     */
    ComponentDetails getComponentDetails(NiFiWebRequestContext requestContext) throws ResourceNotFoundException, ClusterRequestException;
}
