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

import java.util.Set;

/**
 * A holder object to provide atomic access to policies for a given resource and users by
 * identity. Implementations must ensure consistent access to the data backing this instance.
 */
public interface UsersAndAccessPolicies {

    /**
     * Retrieves the set of access policies for a given resource.
     *
     * @param resourceIdentifier the resource identifier to retrieve policies for
     * @return the set of access policies for the given resource
     */
    public Set<AccessPolicy> getAccessPolicies(final String resourceIdentifier);

    /**
     * Retrieves a user by an identity string.
     *
     * @param identity the identity of the user to retrieve
     * @return the user with the given identity
     */
    public User getUser(final String identity);

    /**
     * Retrieves the groups for a given user identity.
     *
     * @param userIdentity a user identity
     * @return the set of groups for the given user identity
     */
    public Set<Group> getGroups(final String userIdentity);

}
