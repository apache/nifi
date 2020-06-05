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
package org.apache.nifi.toolkit.cli.impl.client.registry.impl;

import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.impl.AbstractJerseyClient;
import org.apache.nifi.toolkit.cli.impl.client.registry.PoliciesClient;
import org.apache.nifi.util.StringUtils;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class JerseyPoliciesClient extends AbstractJerseyClient implements PoliciesClient {
    private final WebTarget policiesTarget;

    public JerseyPoliciesClient(final WebTarget baseTarget, final Map<String, String> headers) {
        super(headers);
        this.policiesTarget = baseTarget.path("/policies");
    }

    public JerseyPoliciesClient(final WebTarget baseTarget) {
        this(baseTarget, Collections.emptyMap());
    }

    @Override
    public AccessPolicy getPolicy(final String id) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Access policy id cannot be null");
        }

        return executeAction("Error retrieving access policy", () -> {
            final WebTarget target = policiesTarget.path("{id}").resolveTemplate("id", id);
            return getRequestBuilder(target).get(AccessPolicy.class);
        });
    }

    @Override
    public AccessPolicy createPolicy(final AccessPolicy policy) throws NiFiRegistryException, IOException {
        if (policy == null) {
            throw new IllegalArgumentException("Access policy cannot be null");
        }

        return executeAction("Error creating access policy", () -> {
            return getRequestBuilder(policiesTarget).post(
                    Entity.entity(policy, MediaType.APPLICATION_JSON_TYPE), AccessPolicy.class
            );
        });
    }

    @Override
    public AccessPolicy updatePolicy(final AccessPolicy policy) throws NiFiRegistryException, IOException {
        if (policy == null) {
            throw new IllegalArgumentException("Access policy cannot be null");
        }

        return executeAction("Error creating access policy", () -> {
            final WebTarget target = policiesTarget.path("{id}").resolveTemplate("id", policy.getIdentifier());
            return getRequestBuilder(target).put(
                    Entity.entity(policy, MediaType.APPLICATION_JSON_TYPE), AccessPolicy.class
            );
        });
    }
}
