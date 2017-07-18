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
package org.apache.nifi.authorization.util;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class UserGroupUtil {

    /**
     * Gets the groups for the user with the specified identity. Returns null if the authorizer is not able to load user groups.
     *
     * @param authorizer the authorizer to load the groups from
     * @param userIdentity the user identity
     * @return the listing of groups for the user
     */
    public static Set<String> getUserGroups(final Authorizer authorizer, final String userIdentity) {
        if (authorizer instanceof ManagedAuthorizer) {
            final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) authorizer;
            final UserGroupProvider userGroupProvider = managedAuthorizer.getAccessPolicyProvider().getUserGroupProvider();
            final UserAndGroups userAndGroups = userGroupProvider.getUserAndGroups(userIdentity);
            final Set<Group> userGroups = userAndGroups.getGroups();

            if (userGroups == null || userGroups.isEmpty()) {
                return Collections.EMPTY_SET;
            } else {
                return userAndGroups.getGroups().stream().map(group -> group.getName()).collect(Collectors.toSet());
            }
        } else {
            return null;
        }
    }
}
