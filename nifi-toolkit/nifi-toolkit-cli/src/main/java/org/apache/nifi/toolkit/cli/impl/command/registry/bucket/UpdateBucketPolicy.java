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
package org.apache.nifi.toolkit.cli.impl.command.registry.bucket;

import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.authorization.Tenant;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.PoliciesClient;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.registry.AbstractNiFiRegistryCommand;
import org.apache.nifi.toolkit.cli.impl.command.registry.tenant.TenantHelper;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

public class UpdateBucketPolicy extends AbstractNiFiRegistryCommand<StringResult> {


    public UpdateBucketPolicy() {
        super("update-bucket-policy", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Updates access policy of bucket, NOTE: Overwrites the users/user-groups in the specified policy";
    }

    @Override
    public void doInitialize(final Context context) {
        addOption(CommandOption.BUCKET_NAME.createOption());
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.USER_NAME_LIST.createOption());
        addOption(CommandOption.USER_ID_LIST.createOption());
        addOption(CommandOption.GROUP_NAME_LIST.createOption());
        addOption(CommandOption.GROUP_ID_LIST.createOption());
        addOption(CommandOption.POLICY_ACTION.createOption());
    }


    @Override
    public StringResult doExecute(NiFiRegistryClient client, Properties properties) throws IOException, NiFiRegistryException, ParseException {
        final PoliciesClient policiesClient = client.getPoliciesClient();

        final String bucketName = getArg(properties, CommandOption.BUCKET_NAME);
        String bucketId = getArg(properties, CommandOption.BUCKET_ID);

        final String userNames = getArg(properties, CommandOption.USER_NAME_LIST);
        final String userIds = getArg(properties, CommandOption.USER_ID_LIST);
        final String groupNames = getArg(properties, CommandOption.GROUP_NAME_LIST);
        final String groupIds = getArg(properties, CommandOption.GROUP_ID_LIST);

        final String policyAction = getRequiredArg(properties, CommandOption.POLICY_ACTION);
        final HashSet<String> permittedActions = new HashSet<>(Arrays.asList("read", "write", "delete"));
        if (!permittedActions.contains(policyAction)) {
            throw new IllegalArgumentException("Only read, write, delete actions permitted");
        }
        if (StringUtils.isBlank(bucketName) == StringUtils.isBlank(bucketId)) {
            throw new IllegalArgumentException("Specify either bucket name or bucket id");
        }
        if (StringUtils.isBlank(bucketId)) {
            final Optional<Bucket> optionalBucket = client.getBucketClient().getAll()
                    .stream().filter(b -> bucketName.equals(b.getName())).findAny();
            if (!optionalBucket.isPresent()) {
                throw new IllegalArgumentException("Specified bucket does not exist");
            }
            bucketId = optionalBucket.get().getIdentifier();
        } else {
            try {
                client.getBucketClient().get(bucketId);
            } catch (NiFiRegistryException e) {
                throw new IllegalArgumentException("Specified bucket does not exist");
            }
        }
        AccessPolicy accessPolicy;
        String resource = "/buckets/" + bucketId;
        try {
            accessPolicy = policiesClient.getAccessPolicy(policyAction, resource);
        } catch (NiFiRegistryException e) {
            accessPolicy = new AccessPolicy();
            accessPolicy.setResource(resource);
            accessPolicy.setAction(policyAction);
        }
        if (!StringUtils.isBlank(userNames) || !StringUtils.isBlank(userIds)) {
            Set<Tenant> users = TenantHelper.selectExistingTenants(userNames,
                    userIds, client.getTenantsClient().getUsers());
            //Overwrite users, similar to CreateOrUpdateAccessPolicy of Registry
            accessPolicy.setUsers(users);
        }
        if (!StringUtils.isBlank(groupNames) || !StringUtils.isBlank(groupIds)) {
            Set<Tenant> groups = TenantHelper.selectExistingTenants(groupNames,
                    groupIds, client.getTenantsClient().getUserGroups());
            //Overwrite user-groups, similar to CreateOrUpdateAccessPolicy of Registry
            accessPolicy.setUserGroups(groups);
        }
        AccessPolicy updatedPolicy = StringUtils.isBlank(accessPolicy.getIdentifier())
                ? policiesClient.createAccessPolicy(accessPolicy)
                : policiesClient.updateAccessPolicy(accessPolicy);
        return new StringResult(updatedPolicy.getIdentifier(), getContext().isInteractive());
    }

}
