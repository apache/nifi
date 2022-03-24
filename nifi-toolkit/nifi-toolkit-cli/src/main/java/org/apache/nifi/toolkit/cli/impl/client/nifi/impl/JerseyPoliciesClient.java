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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.PoliciesClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Jersey implementation of PoliciesClient.
 */
public class JerseyPoliciesClient extends AbstractJerseyClient implements PoliciesClient {

    private final WebTarget policiesTarget;

    public JerseyPoliciesClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyPoliciesClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.policiesTarget = baseTarget.path("/policies");
    }

    @Override
    public AccessPolicyEntity getAccessPolicy(final String resource, final String action) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(resource) || StringUtils.isBlank(action)) {
            throw new IllegalArgumentException("Resouce and action cannot be null");
        }

        return executeAction("Error retrieving configuration of access policy", () -> {
            final WebTarget target = policiesTarget.path(action).path(resource);
            return getRequestBuilder(target).get(AccessPolicyEntity.class);
        });
    }

    @Override
    public AccessPolicyEntity createAccessPolicy(final AccessPolicyEntity accessPolicyEntity) throws NiFiClientException, IOException {
        if (accessPolicyEntity == null) {
            throw new IllegalArgumentException("Access policy entity cannot be null");
        }

        return executeAction("Error creating access policy", () ->
                getRequestBuilder(policiesTarget).post(
                        Entity.entity(accessPolicyEntity, MediaType.APPLICATION_JSON),
                        AccessPolicyEntity.class
                ));
    }

    @Override
    public AccessPolicyEntity updateAccessPolicy(final AccessPolicyEntity accessPolicyEntity) throws NiFiClientException, IOException {
        if (accessPolicyEntity == null) {
            throw new IllegalArgumentException("Access policy entity cannot be null");
        }

        if (StringUtils.isBlank(accessPolicyEntity.getId())) {
            throw new IllegalArgumentException("Access policy entity must contain an id");
        }

        return executeAction("Error updating access policy", () -> {
            final WebTarget target = policiesTarget.path(accessPolicyEntity.getId());

            return getRequestBuilder(target).put(
                    Entity.entity(accessPolicyEntity, MediaType.APPLICATION_JSON),
                    AccessPolicyEntity.class
            );
        });
    }
}
