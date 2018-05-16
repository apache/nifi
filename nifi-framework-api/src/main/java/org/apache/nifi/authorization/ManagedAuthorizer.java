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

public interface ManagedAuthorizer extends Authorizer {

    /**
     * Returns a fingerprint representing the authorizations managed by this authorizer. The fingerprint will be
     * used for comparison to determine if two managed authorizers represent a compatible set of users,
     * groups, and/or policies. Must be non null
     *
     * @return the fingerprint for this Authorizer
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    String getFingerprint() throws AuthorizationAccessException;

    /**
     * Parses the fingerprint and adds any users, groups, and policies to the current Authorizer.
     *
     * @param fingerprint the fingerprint that was obtained from calling getFingerprint() on another Authorizer.
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    void inheritFingerprint(final String fingerprint) throws AuthorizationAccessException;

    /**
     * When the fingerprints are not equal, this method will check if the proposed fingerprint is inheritable.
     * If the fingerprint is an exact match, this method will not be invoked as there is nothing to inherit.
     *
     * @param proposedFingerprint the proposed fingerprint
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws UninheritableAuthorizationsException if the proposed fingerprint was uninheritable
     */
    void checkInheritability(final String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException;

    /**
     * Returns the AccessPolicy provider for this managed Authorizer. Must be non null
     *
     * @return the AccessPolicy provider
     */
    AccessPolicyProvider getAccessPolicyProvider();

}
