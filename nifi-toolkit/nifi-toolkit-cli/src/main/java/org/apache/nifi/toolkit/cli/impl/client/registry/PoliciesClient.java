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
package org.apache.nifi.toolkit.cli.impl.client.registry;

import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.client.NiFiRegistryException;

import java.io.IOException;

/**
 * Provides API for the services might be called from registry related to access policies.
 */
public interface PoliciesClient {

    /**
     * Returns with a given access policy.
     *
     * @param id The identifier of the access policy.
     *
     * @return The access policy.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.
     */
    AccessPolicy getPolicy(String id) throws NiFiRegistryException, IOException;

    /**
     * Creates a new access policy within the registry.
     *
     * @param policy The attributes of the access policy. Note: identifier will be ignored and generated.
     *
     * @return The access policy after store, containing it's identifier.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.
     */
    AccessPolicy createPolicy(AccessPolicy policy) throws NiFiRegistryException, IOException;

    /**
     * Updates an existing access policy.
     *
     * @param policy The updated attributes of the access policy.
     *
     * @return The stored access policy.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.
     */
    AccessPolicy updatePolicy(AccessPolicy policy) throws NiFiRegistryException, IOException;
}
