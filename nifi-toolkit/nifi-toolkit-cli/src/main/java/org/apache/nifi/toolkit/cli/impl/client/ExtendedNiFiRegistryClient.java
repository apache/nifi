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
package org.apache.nifi.toolkit.cli.impl.client;

import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.toolkit.cli.impl.client.registry.PoliciesClient;
import org.apache.nifi.toolkit.cli.impl.client.registry.TenantsClient;

/**
 * Extends NiFiRegistry client with additional exposed service.
 *
 * Note: in longer term the functionality of this should be merged into the NiFiRegistryClient.
 */
public interface ExtendedNiFiRegistryClient extends NiFiRegistryClient {

    /**
     * Returns client for interacting with tenants.
     *
     * @return the client for interacting with tenants
     */
    TenantsClient getTenantsClient();

    /**
     * Returns client for interacting with tenants.
     *
     * @param proxiedEntity The given proxied entities.
     *
     * @return the client for interacting with tenants on behalf of the given proxied entities.
     */
    TenantsClient getTenantsClient(String ... proxiedEntity);

    /**
     * Returns client for interacting with access policies.
     *
     * @return the client for interacting with access policies
     */
    PoliciesClient getPoliciesClient();

    /**
     * Returns client for interacting with access policies.
     *
     * @param proxiedEntity The given proxied entities.
     *
     * @return the client for interacting with access policies on behalf of the given proxied entities.
     */
    PoliciesClient getPoliciesClient(String ... proxiedEntity);
}
