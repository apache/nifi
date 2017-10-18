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
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;

/**
 * Provides support for configuring AccessPolicies.
 *
 * NOTE: Extensions will be called often and frequently. Because of this, if the underlying implementation needs to
 * make remote calls or expensive calculations those should probably be done asynchronously and/or cache the results.
 *
 * Additionally, extensions need to be thread safe.
 */
public interface ConfigurableAccessPolicyProvider extends AccessPolicyProvider {

    /**
     * Returns a fingerprint representing the authorizations managed by this authorizer. The fingerprint will be
     * used for comparison to determine if two policy-based authorizers represent a compatible set of policies.
     *
     * @return the fingerprint for this Authorizer
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    String getFingerprint() throws AuthorizationAccessException;

    /**
     * Parses the fingerprint and adds any policies to the current AccessPolicyProvider.
     *
     * @param fingerprint the fingerprint that was obtained from calling getFingerprint() on another Authorizer.
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    void inheritFingerprint(final String fingerprint) throws AuthorizationAccessException;

    /**
     * When the fingerprints are not equal, this method will check if the proposed fingerprint is inheritable.
     * If the fingerprint is an exact match, this method will not be invoked as there is nothing to inherit.
     *
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws UninheritableAuthorizationsException if the proposed fingerprint was uninheritable
     */
    void checkInheritability(final String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException;

    /**
     * Adds the given policy ensuring that multiple policies can not be added for the same resource and action.
     *
     * @param accessPolicy the policy to add
     * @return the policy that was added
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    AccessPolicy addAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException;

    /**
     * Determines whether the specified access policy is configurable. Provides the opportunity for a ConfigurableAccessPolicyProvider to prevent
     * editing of a specific access policy. By default, all known access policies are configurable.
     *
     * @param accessPolicy the access policy
     * @return is configurable
     */
    default boolean isConfigurable(AccessPolicy accessPolicy) {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("Access policy cannot be null");
        }

        return getAccessPolicy(accessPolicy.getIdentifier()) != null;
    }

    /**
     * The policy represented by the provided instance will be updated based on the provided instance.
     *
     * @param accessPolicy an updated policy
     * @return the updated policy, or null if no matching policy was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException;

    /**
     * Deletes the given policy.
     *
     * @param accessPolicy the policy to delete
     * @return the deleted policy, or null if no matching policy was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    AccessPolicy deleteAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException;
}
