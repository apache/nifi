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
package org.apache.nifi.integration.auth;

import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.AccessPolicyProvider;
import org.apache.nifi.authorization.AccessPolicyProviderInitializationContext;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class VolatileAccessPolicyProvider implements AccessPolicyProvider {
    private final VolatileUserGroupProvider userGroupProvider = new VolatileUserGroupProvider();
    private Set<AccessPolicy> accessPolicies = new HashSet<>();

    public synchronized void grantAccess(final String user, final String resourceIdentifier, final RequestAction action) {
        final AccessPolicy existingPolicy = getAccessPolicy(resourceIdentifier, action);

        final AccessPolicy policy;
        if (existingPolicy == null) {
            policy = new AccessPolicy.Builder()
                .addUser(user)
                .action(action)
                .identifierGenerateRandom()
                .resource(resourceIdentifier)
                .build();
        } else {
            policy = new AccessPolicy.Builder()
                .addUsers(existingPolicy.getUsers())
                .addUser(user)
                .action(action)
                .identifier(existingPolicy.getIdentifier())
                .resource(resourceIdentifier)
                .build();
        }

        accessPolicies.remove(existingPolicy);
        accessPolicies.add(policy);
    }

    public synchronized void revokeAccess(final String user, final String resourceIdentifier, final RequestAction action) {
        final AccessPolicy existingPolicy = getAccessPolicy(resourceIdentifier, action);

        if (existingPolicy == null) {
            return;
        }

        final AccessPolicy policy= new AccessPolicy.Builder()
                .addUsers(existingPolicy.getUsers())
                .removeUser(user)
                .action(action)
                .identifier(existingPolicy.getIdentifier())
                .resource(resourceIdentifier)
                .build();

        accessPolicies.remove(existingPolicy);
        accessPolicies.add(policy);
    }

    @Override
    public synchronized Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return new HashSet<>(accessPolicies);
    }

    @Override
    public synchronized AccessPolicy getAccessPolicy(final String identifier) throws AuthorizationAccessException {
        return accessPolicies.stream()
            .filter(policy -> policy.getIdentifier().equals(identifier))
            .findAny()
            .orElse(null);
    }

    @Override
    public synchronized AccessPolicy getAccessPolicy(final String resourceIdentifier, final RequestAction action) throws AuthorizationAccessException {
        return accessPolicies.stream()
            .filter(policy -> Objects.equals(policy.getResource(), resourceIdentifier))
            .filter(policy -> Objects.equals(policy.getAction(), action))
            .findAny()
            .orElse(null);
    }

    @Override
    public synchronized VolatileUserGroupProvider getUserGroupProvider() {
        return userGroupProvider;
    }

    @Override
    public void initialize(final AccessPolicyProviderInitializationContext initializationContext) throws AuthorizerCreationException {
    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
    }
}
