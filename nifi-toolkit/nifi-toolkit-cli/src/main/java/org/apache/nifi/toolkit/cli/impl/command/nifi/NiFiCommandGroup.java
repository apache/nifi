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
package org.apache.nifi.toolkit.cli.impl.command.nifi;

import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.impl.command.AbstractCommandGroup;
import org.apache.nifi.toolkit.cli.impl.command.nifi.access.GetAccessToken;
import org.apache.nifi.toolkit.cli.impl.command.nifi.access.GetAccessTokenSpnego;
import org.apache.nifi.toolkit.cli.impl.command.nifi.access.LogoutAccessToken;
import org.apache.nifi.toolkit.cli.impl.command.nifi.cs.CreateControllerService;
import org.apache.nifi.toolkit.cli.impl.command.nifi.cs.DisableControllerServices;
import org.apache.nifi.toolkit.cli.impl.command.nifi.cs.EnableControllerServices;
import org.apache.nifi.toolkit.cli.impl.command.nifi.cs.GetControllerService;
import org.apache.nifi.toolkit.cli.impl.command.nifi.cs.GetControllerServices;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.ClusterSummary;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.CreateFlowAnalysisRule;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.CreateReportingTask;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.CurrentUser;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.DeleteFlowAnalysisRule;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.DisableFlowAnalysisRules;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.EnableFlowAnalysisRules;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.ExportReportingTask;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.ExportReportingTasks;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.GetControllerConfiguration;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.GetFlowAnalysisRule;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.GetFlowAnalysisRules;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.GetReportingTask;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.GetReportingTasks;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.GetRootId;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.ImportReportingTasks;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.StartReportingTasks;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.StopReportingTasks;
import org.apache.nifi.toolkit.cli.impl.command.nifi.flow.UpdateControllerConfiguration;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nar.DeleteNar;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nar.DownloadNar;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nar.ListNarComponentTypes;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nar.ListNars;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nar.UploadNar;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.ConnectNode;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.DeleteNode;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.DisconnectNode;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.GetNode;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.GetNodes;
import org.apache.nifi.toolkit.cli.impl.command.nifi.nodes.OffloadNode;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.AddAssetReference;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.CreateAsset;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.CreateParamContext;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.CreateParamProvider;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.DeleteAsset;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.DeleteParam;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.DeleteParamContext;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.DeleteParamProvider;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.ExportParamContext;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.FetchParams;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.GetAsset;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.GetParamContext;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.GetParamProvider;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.ImportParamContext;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.ListAssets;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.ListParamContexts;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.ListParamProviders;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.MergeParamContext;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.RemoveAssetReference;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.RemoveInheritedParamContexts;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.SetInheritedParamContexts;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.SetParam;
import org.apache.nifi.toolkit.cli.impl.command.nifi.params.SetParamProviderProperty;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGChangeAllVersions;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGChangeVersion;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGConnect;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGCreate;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGCreateControllerService;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGDelete;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGDisableControllerServices;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGEmptyQueues;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGEnableControllerServices;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGExport;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGGetAllVersions;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGGetControllerServices;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGGetParamContext;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGGetVersion;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGImport;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGList;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGListProcessors;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGReplace;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGSetParamContext;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGStart;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGStatus;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGStop;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGStopVersionControl;
import org.apache.nifi.toolkit.cli.impl.command.nifi.policies.GetAccessPolicy;
import org.apache.nifi.toolkit.cli.impl.command.nifi.policies.UpdateAccessPolicy;
import org.apache.nifi.toolkit.cli.impl.command.nifi.processors.ChangeVersionProcessor;
import org.apache.nifi.toolkit.cli.impl.command.nifi.processors.ProcessorClearState;
import org.apache.nifi.toolkit.cli.impl.command.nifi.processors.ProcessorRunOnce;
import org.apache.nifi.toolkit.cli.impl.command.nifi.processors.ProcessorStart;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.CreateRegistryClient;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.GetRegistryClientId;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.ListBranches;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.ListBuckets;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.ListFlowVersions;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.ListFlows;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.ListRegistryClients;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.SetRegistryClientProperty;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.UpdateRegistryClient;
import org.apache.nifi.toolkit.cli.impl.command.nifi.reporting.DeleteReportingTask;
import org.apache.nifi.toolkit.cli.impl.command.nifi.tenants.CreateUser;
import org.apache.nifi.toolkit.cli.impl.command.nifi.tenants.CreateUserGroup;
import org.apache.nifi.toolkit.cli.impl.command.nifi.tenants.ListUserGroups;
import org.apache.nifi.toolkit.cli.impl.command.nifi.tenants.ListUsers;
import org.apache.nifi.toolkit.cli.impl.command.nifi.tenants.UpdateUserGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * CommandGroup for NiFi commands.
 */
public class NiFiCommandGroup extends AbstractCommandGroup {

    public static final String NIFI_COMMAND_GROUP = "nifi";

    public NiFiCommandGroup() {
        super(NIFI_COMMAND_GROUP);
    }

    @Override
    protected List<Command> createCommands() {
        final List<AbstractNiFiCommand<?>> commands = new ArrayList<>();
        commands.add(new CurrentUser());
        commands.add(new ClusterSummary());
        commands.add(new ConnectNode());
        commands.add(new DeleteNode());
        commands.add(new DisconnectNode());
        commands.add(new GetRootId());
        commands.add(new GetNode());
        commands.add(new GetNodes());
        commands.add(new OffloadNode());
        commands.add(new ListRegistryClients());
        commands.add(new CreateRegistryClient());
        commands.add(new UpdateRegistryClient());
        commands.add(new SetRegistryClientProperty());
        commands.add(new GetRegistryClientId());
        commands.add(new ListBranches());
        commands.add(new ListBuckets());
        commands.add(new ListFlows());
        commands.add(new ListFlowVersions());
        commands.add(new PGImport());
        commands.add(new PGConnect());
        commands.add(new PGStart());
        commands.add(new PGStop());
        commands.add(new PGCreate());
        commands.add(new PGGetVersion());
        commands.add(new PGStopVersionControl());
        commands.add(new PGChangeVersion());
        commands.add(new PGChangeAllVersions());
        commands.add(new PGGetAllVersions());
        commands.add(new PGList());
        commands.add(new PGStatus());
        commands.add(new PGGetControllerServices());
        commands.add(new PGCreateControllerService());
        commands.add(new PGEnableControllerServices());
        commands.add(new PGDisableControllerServices());
        commands.add(new PGGetParamContext());
        commands.add(new PGSetParamContext());
        commands.add(new PGReplace());
        commands.add(new PGExport());
        commands.add(new PGEmptyQueues());
        commands.add(new PGDelete());
        commands.add(new PGListProcessors());
        commands.add(new GetControllerServices());
        commands.add(new GetControllerService());
        commands.add(new CreateControllerService());
        commands.add(new EnableControllerServices());
        commands.add(new DisableControllerServices());
        commands.add(new GetReportingTasks());
        commands.add(new GetReportingTask());
        commands.add(new CreateReportingTask());
        commands.add(new DeleteReportingTask());
        commands.add(new StartReportingTasks());
        commands.add(new StopReportingTasks());
        commands.add(new ExportReportingTasks());
        commands.add(new ExportReportingTask());
        commands.add(new ImportReportingTasks());
        commands.add(new CreateFlowAnalysisRule());
        commands.add(new GetFlowAnalysisRules());
        commands.add(new GetFlowAnalysisRule());
        commands.add(new EnableFlowAnalysisRules());
        commands.add(new DisableFlowAnalysisRules());
        commands.add(new DeleteFlowAnalysisRule());
        commands.add(new ListUsers());
        commands.add(new CreateUser());
        commands.add(new ListUserGroups());
        commands.add(new CreateUserGroup());
        commands.add(new UpdateUserGroup());
        commands.add(new GetAccessPolicy());
        commands.add(new UpdateAccessPolicy());
        commands.add(new ListParamContexts());
        commands.add(new GetParamContext());
        commands.add(new CreateParamContext());
        commands.add(new DeleteParamContext());
        commands.add(new SetInheritedParamContexts());
        commands.add(new RemoveInheritedParamContexts());
        commands.add(new SetParam());
        commands.add(new DeleteParam());
        commands.add(new ExportParamContext());
        commands.add(new ImportParamContext());
        commands.add(new MergeParamContext());
        commands.add(new ListParamProviders());
        commands.add(new GetParamProvider());
        commands.add(new CreateParamProvider());
        commands.add(new DeleteParamProvider());
        commands.add(new FetchParams());
        commands.add(new SetParamProviderProperty());
        commands.add(new GetAccessToken());
        commands.add(new GetAccessTokenSpnego());
        commands.add(new LogoutAccessToken());
        commands.add(new GetControllerConfiguration());
        commands.add(new UpdateControllerConfiguration());
        commands.add(new ChangeVersionProcessor());
        commands.add(new ProcessorStart());
        commands.add(new ProcessorRunOnce());
        commands.add(new ProcessorClearState());
        commands.add(new UploadNar());
        commands.add(new DownloadNar());
        commands.add(new ListNars());
        commands.add(new ListNarComponentTypes());
        commands.add(new DeleteNar());
        commands.add(new CreateAsset());
        commands.add(new ListAssets());
        commands.add(new GetAsset());
        commands.add(new DeleteAsset());
        commands.add(new AddAssetReference());
        commands.add(new RemoveAssetReference());
        return new ArrayList<>(commands);
    }
}
