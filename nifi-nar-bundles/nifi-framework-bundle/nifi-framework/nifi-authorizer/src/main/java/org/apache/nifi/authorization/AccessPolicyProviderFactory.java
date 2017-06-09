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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.nar.NarCloseable;

import java.util.Set;

public final class AccessPolicyProviderFactory {

    public static AccessPolicyProvider withNarLoader(final AccessPolicyProvider baseAccessPolicyProvider) {
        if (baseAccessPolicyProvider instanceof ConfigurableAccessPolicyProvider) {
            final ConfigurableAccessPolicyProvider baseConfigurableAccessPolicyProvider = (ConfigurableAccessPolicyProvider) baseAccessPolicyProvider;
            return new ConfigurableAccessPolicyProvider() {
                @Override
                public AccessPolicy addAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableAccessPolicyProvider.addAccessPolicy(accessPolicy);
                    }
                }

                @Override
                public AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableAccessPolicyProvider.updateAccessPolicy(accessPolicy);
                    }
                }

                @Override
                public AccessPolicy deleteAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableAccessPolicyProvider.deleteAccessPolicy(accessPolicy);
                    }
                }

                @Override
                public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableAccessPolicyProvider.getAccessPolicies();
                    }
                }

                @Override
                public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableAccessPolicyProvider.getAccessPolicy(identifier);
                    }
                }

                @Override
                public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableAccessPolicyProvider.getAccessPolicy(resourceIdentifier, action);
                    }
                }

                @Override
                public UserGroupProvider getUserGroupProvider() {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableAccessPolicyProvider.getUserGroupProvider();
                    }
                }

                @Override
                public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseConfigurableAccessPolicyProvider.inheritFingerprint(fingerprint);
                    }
                }

                @Override
                public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseConfigurableAccessPolicyProvider.checkInheritability(proposedFingerprint);
                    }
                }

                @Override
                public String getFingerprint() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableAccessPolicyProvider.getFingerprint();
                    }
                }

                @Override
                public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseConfigurableAccessPolicyProvider.initialize(initializationContext);
                    }
                }

                @Override
                public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseConfigurableAccessPolicyProvider.onConfigured(configurationContext);
                    }
                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseConfigurableAccessPolicyProvider.preDestruction();
                    }
                }
            };
        } else {
            return new AccessPolicyProvider() {
                @Override
                public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseAccessPolicyProvider.getAccessPolicies();
                    }
                }

                @Override
                public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseAccessPolicyProvider.getAccessPolicy(identifier);
                    }
                }

                @Override
                public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseAccessPolicyProvider.getAccessPolicy(resourceIdentifier, action);
                    }
                }

                @Override
                public UserGroupProvider getUserGroupProvider() {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseAccessPolicyProvider.getUserGroupProvider();
                    }
                }

                @Override
                public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseAccessPolicyProvider.initialize(initializationContext);
                    }
                }

                @Override
                public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseAccessPolicyProvider.onConfigured(configurationContext);
                    }
                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseAccessPolicyProvider.preDestruction();
                    }
                }
            };
        }
    }

    private AccessPolicyProviderFactory() {}
}
