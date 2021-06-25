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
package org.apache.nifi.registry.client.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.PoliciesClient;
import org.apache.nifi.registry.client.RequestConfig;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;

public class JerseyPoliciesClient extends AbstractCRUDJerseyClient implements PoliciesClient {

    public static final String ACCESS_POLICY = "Access policy";
    public static final String POLICIES_PATH = "policies";

    public JerseyPoliciesClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyPoliciesClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(baseTarget, requestConfig);
    }

    @Override
    public AccessPolicy getAccessPolicy(String action, String resource) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(resource) || StringUtils.isBlank(action)) {
            throw new IllegalArgumentException("Resource and action cannot be null");
        }

        return executeAction("Error retrieving access policy", () -> {
            final WebTarget target = baseTarget.path(POLICIES_PATH).path(action).path(resource);
            return getRequestBuilder(target).get(AccessPolicy.class);
        });
    }

    @Override
    public AccessPolicy createAccessPolicy(final AccessPolicy policy) throws NiFiRegistryException, IOException {
        return create(policy, AccessPolicy.class, ACCESS_POLICY, POLICIES_PATH);
    }

    @Override
    public AccessPolicy updateAccessPolicy(final AccessPolicy policy) throws NiFiRegistryException, IOException {
        return update(policy, policy.getIdentifier(), AccessPolicy.class, ACCESS_POLICY, POLICIES_PATH);
    }

}

