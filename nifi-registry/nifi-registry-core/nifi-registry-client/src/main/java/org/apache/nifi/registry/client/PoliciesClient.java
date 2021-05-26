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
package org.apache.nifi.registry.client;

import org.apache.nifi.registry.authorization.AccessPolicy;

import java.io.IOException;

public interface PoliciesClient {

    /**
     * Returns a given access policy.
     *
     * @param resource The action allowed by the access policy.
     * @param action The resource managed by the access policy.
     *
     * @return The access policy.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    AccessPolicy getAccessPolicy(String action, String resource) throws NiFiRegistryException, IOException;

    /**
     * Creates a new access policy.
     *
     * @param policy The access policy to be created. Note: identifier will be ignored and assigned by NiFi Registry.
     *
     * @return The created access with an assigned identifier.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    AccessPolicy createAccessPolicy(AccessPolicy policy) throws NiFiRegistryException, IOException;

    /**
     * Updates an existing access policy.
     *
     * @param policy The access policy with new attributes.
     *
     * @return The updated access policy.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    AccessPolicy updateAccessPolicy(AccessPolicy policy) throws NiFiRegistryException, IOException;

}
