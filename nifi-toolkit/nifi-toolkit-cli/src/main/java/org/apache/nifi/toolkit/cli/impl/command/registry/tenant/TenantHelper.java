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
package org.apache.nifi.toolkit.cli.impl.command.registry.tenant;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.authorization.Tenant;
import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.toolkit.cli.impl.client.registry.TenantsClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class TenantHelper {
    private static final String SEPARATOR = ",";

    private TenantHelper() {
        // no op
    }

    public static Set<Tenant> getExistingUsers(final TenantsClient client, final String userNamesArgument, final String userIdsArgument)
            throws IOException, NiFiRegistryException {
        final Set<Tenant> result = new HashSet<>();

        final Set<String> userNames = StringUtils.isNotBlank(userNamesArgument)
                ? new HashSet<>(Arrays.asList(userNamesArgument.split(SEPARATOR)))
                : Collections.emptySet();

        final Set<String> userIds = StringUtils.isNotBlank(userIdsArgument)
                ? new HashSet<>(Arrays.asList(userIdsArgument.split(SEPARATOR)))
                : Collections.emptySet();

        if (userNames.isEmpty() && userIds.isEmpty()) {
            return result;
        }

        final List<User> users = client.getUsers();

        for (final User user : users) {
            if (userNames.contains(user.getIdentity()) || userIds.contains(user.getIdentifier())) {
                result.add(user);
            }
        }

        return result;
    }

    public static Set<Tenant> getExistingGroups(final TenantsClient client, final String userGroupNamesArgument, final String userGroupIdsArgument)
            throws IOException, NiFiRegistryException {
        final Set<Tenant> result = new HashSet<>();

        final Set<String> userGroupNames = StringUtils.isNotBlank(userGroupNamesArgument)
                ? new HashSet<>(Arrays.asList(userGroupNamesArgument.split(SEPARATOR)))
                : Collections.emptySet();

        final Set<String> userGroupIds = StringUtils.isNotBlank(userGroupIdsArgument)
                ? new HashSet<>(Arrays.asList(userGroupIdsArgument.split(SEPARATOR)))
                : Collections.emptySet();

        if (userGroupNames.isEmpty() && userGroupIds.isEmpty()) {
            return result;
        }

        final List<UserGroup> usersGroups = client.getUserGroups();

        for (final UserGroup userGroup : usersGroups) {
            if (userGroupNames.contains(userGroup.getIdentity()) || userGroupIds.contains(userGroup.getIdentifier())) {
                result.add(userGroup);
            }
        }

        return result;
    }
}
