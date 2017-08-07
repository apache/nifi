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

public final class AuthorizerCapabilityDetection {

    public static boolean isManagedAuthorizer(final Authorizer authorizer) {
        return authorizer instanceof ManagedAuthorizer;
    }

    public static boolean isConfigurableAccessPolicyProvider(final Authorizer authorizer) {
        if (!isManagedAuthorizer(authorizer)) {
            return false;
        }

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;
        return managedAuthorizer.getAccessPolicyProvider() instanceof ConfigurableAccessPolicyProvider;
    }

    public static boolean isConfigurableUserGroupProvider(final Authorizer authorizer) {
        if (!isManagedAuthorizer(authorizer)) {
            return false;
        }

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;
        final AccessPolicyProvider accessPolicyProvider = managedAuthorizer.getAccessPolicyProvider();
        return accessPolicyProvider.getUserGroupProvider() instanceof ConfigurableUserGroupProvider;
    }

    public static boolean isUserConfigurable(final Authorizer authorizer, final User user) {
        if (!isConfigurableUserGroupProvider(authorizer)) {
            return false;
        }

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;
        final ConfigurableUserGroupProvider configurableUserGroupProvider = (ConfigurableUserGroupProvider) managedAuthorizer.getAccessPolicyProvider().getUserGroupProvider();
        return configurableUserGroupProvider.isConfigurable(user);
    }

    public static boolean isGroupConfigurable(final Authorizer authorizer, final Group group) {
        if (!isConfigurableUserGroupProvider(authorizer)) {
            return false;
        }

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;
        final ConfigurableUserGroupProvider configurableUserGroupProvider = (ConfigurableUserGroupProvider) managedAuthorizer.getAccessPolicyProvider().getUserGroupProvider();
        return configurableUserGroupProvider.isConfigurable(group);
    }

    public static boolean isAccessPolicyConfigurable(final Authorizer authorizer, final AccessPolicy accessPolicy) {
        if (!isConfigurableAccessPolicyProvider(authorizer)) {
            return false;
        }

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;
        final ConfigurableAccessPolicyProvider configurableAccessPolicyProvider = (ConfigurableAccessPolicyProvider) managedAuthorizer.getAccessPolicyProvider();
        return configurableAccessPolicyProvider.isConfigurable(accessPolicy);
    }

    private AuthorizerCapabilityDetection() {}
}
