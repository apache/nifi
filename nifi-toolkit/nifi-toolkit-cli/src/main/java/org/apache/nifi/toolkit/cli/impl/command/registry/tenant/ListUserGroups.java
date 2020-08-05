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

import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.TenantsClient;
import org.apache.nifi.toolkit.cli.impl.result.registry.UserGroupsResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Command to get the list of user groups.
 */
public class ListUserGroups extends AbstractListTenants<UserGroup, UserGroupsResult> {
    public ListUserGroups() {
        super("list-user-groups", UserGroupsResult.class);
    }

    @Override
    public String getDescription() {
        return "Retrieves the list of user group.";
    }

    @Override
    protected UserGroupsResult getTenants(Properties properties, TenantsClient tenantsClient) throws NiFiRegistryException, IOException {
        List<UserGroup> userGroups = tenantsClient.getUserGroups();

        return new UserGroupsResult(getResultType(properties), userGroups);
    }
}
