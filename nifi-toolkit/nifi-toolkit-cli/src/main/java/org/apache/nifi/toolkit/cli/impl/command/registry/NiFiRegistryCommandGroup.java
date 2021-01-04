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
package org.apache.nifi.toolkit.cli.impl.command.registry;

import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.impl.command.AbstractCommandGroup;
import org.apache.nifi.toolkit.cli.impl.command.registry.access.GetAccessToken;
import org.apache.nifi.toolkit.cli.impl.command.registry.access.GetAccessTokenSpnego;
import org.apache.nifi.toolkit.cli.impl.command.registry.access.LogoutAccessToken;
import org.apache.nifi.toolkit.cli.impl.command.registry.bucket.CreateBucket;
import org.apache.nifi.toolkit.cli.impl.command.registry.bucket.DeleteBucket;
import org.apache.nifi.toolkit.cli.impl.command.registry.bucket.ListBuckets;
import org.apache.nifi.toolkit.cli.impl.command.registry.bucket.UpdateBucketPolicy;
import org.apache.nifi.toolkit.cli.impl.command.registry.extension.DownloadBundle;
import org.apache.nifi.toolkit.cli.impl.command.registry.extension.GetBundleChecksum;
import org.apache.nifi.toolkit.cli.impl.command.registry.extension.ListBundleArtifacts;
import org.apache.nifi.toolkit.cli.impl.command.registry.extension.ListBundleGroups;
import org.apache.nifi.toolkit.cli.impl.command.registry.extension.ListBundleVersions;
import org.apache.nifi.toolkit.cli.impl.command.registry.extension.ListExtensionTags;
import org.apache.nifi.toolkit.cli.impl.command.registry.extension.ListExtensions;
import org.apache.nifi.toolkit.cli.impl.command.registry.extension.UploadBundle;
import org.apache.nifi.toolkit.cli.impl.command.registry.extension.UploadBundles;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.CreateFlow;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.DeleteFlow;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.DiffFlowVersions;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.ExportFlowVersion;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.ImportFlowVersion;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.ListFlowVersions;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.ListFlows;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.SyncFlowVersions;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.TransferFlowVersion;
import org.apache.nifi.toolkit.cli.impl.command.registry.policy.GetAccessPolicy;
import org.apache.nifi.toolkit.cli.impl.command.registry.tenant.CreateUser;
import org.apache.nifi.toolkit.cli.impl.command.registry.tenant.CreateUserGroup;
import org.apache.nifi.toolkit.cli.impl.command.registry.policy.CreateOrUpdateAccessPolicy;
import org.apache.nifi.toolkit.cli.impl.command.registry.tenant.ListUserGroups;
import org.apache.nifi.toolkit.cli.impl.command.registry.tenant.ListUsers;
import org.apache.nifi.toolkit.cli.impl.command.registry.tenant.UpdateUser;
import org.apache.nifi.toolkit.cli.impl.command.registry.tenant.UpdateUserGroup;
import org.apache.nifi.toolkit.cli.impl.command.registry.user.CurrentUser;

import java.util.ArrayList;
import java.util.List;

/**
 * CommandGroup for NiFi Registry commands.
 */
public class NiFiRegistryCommandGroup extends AbstractCommandGroup {

    public static String REGISTRY_COMMAND_GROUP = "registry";

    public NiFiRegistryCommandGroup() {
        super(REGISTRY_COMMAND_GROUP);
    }

    @Override
    protected List<Command> createCommands() {
        final List<AbstractNiFiRegistryCommand> commandList = new ArrayList<>();
        commandList.add(new CurrentUser());
        commandList.add(new ListBuckets());
        commandList.add(new CreateBucket());
        commandList.add(new DeleteBucket());
        commandList.add(new ListFlows());
        commandList.add(new CreateFlow());
        commandList.add(new DeleteFlow());
        commandList.add(new ListFlowVersions());
        commandList.add(new ExportFlowVersion());
        commandList.add(new ImportFlowVersion());
        commandList.add(new SyncFlowVersions());
        commandList.add(new TransferFlowVersion());
        commandList.add(new DiffFlowVersions());
        commandList.add(new UploadBundle());
        commandList.add(new UploadBundles());
        commandList.add(new ListBundleGroups());
        commandList.add(new ListBundleArtifacts());
        commandList.add(new ListBundleVersions());
        commandList.add(new DownloadBundle());
        commandList.add(new GetBundleChecksum());
        commandList.add(new ListExtensionTags());
        commandList.add(new ListExtensions());
        commandList.add(new ListUsers());
        commandList.add(new CreateUser());
        commandList.add(new UpdateUser());
        commandList.add(new ListUserGroups());
        commandList.add(new CreateUserGroup());
        commandList.add(new UpdateUserGroup());
        commandList.add(new GetAccessPolicy());
        commandList.add(new CreateOrUpdateAccessPolicy());
        commandList.add(new UpdateBucketPolicy());
        commandList.add(new GetAccessToken());
        commandList.add(new GetAccessTokenSpnego());
        commandList.add(new LogoutAccessToken());
        return new ArrayList<>(commandList);
    }
}
