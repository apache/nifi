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
package org.apache.nifi.authorization.mock;

import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.AccessPolicyProvider;
import org.apache.nifi.authorization.AccessPolicyProviderInitializationContext;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;

import java.util.Set;

public class MockAccessPolicyProvider implements AccessPolicyProvider {
    @Override
    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return null;
    }

    @Override
    public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
        return null;
    }

    @Override
    public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
        return null;
    }

    @Override
    public UserGroupProvider getUserGroupProvider() {
        return null;
    }

    @Override
    public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws AuthorizerCreationException {

    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {

    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {

    }
}
