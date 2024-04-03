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

import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { Settings } from './settings.component';
import { General } from '../ui/general/general.component';
import { ManagementControllerServices } from '../ui/management-controller-services/management-controller-services.component';
import { ReportingTasks } from '../ui/reporting-tasks/reporting-tasks.component';
import { FlowAnalysisRules } from '../ui/flow-analysis-rules/flow-analysis-rules.component';
import { RegistryClients } from '../ui/registry-clients/registry-clients.component';
import { ParameterProviders } from '../ui/parameter-providers/parameter-providers.component';
import { authorizationGuard } from '../../../service/guard/authorization.guard';
import { CurrentUser } from '../../../state/current-user';
import { AdvancedUi } from '../../../ui/common/advanced-ui/advanced-ui.component';
import { controllerServiceAdvancedUiParamsResolver } from '../service/resolver/controller-service-advanced-ui-params.resolver';
import { reportingTaskAdvancedUiParamsResolver } from '../service/resolver/reporting-task-advanced-ui-params.resolver';
import { parameterProviderAdvancedUiParamsResolver } from '../service/resolver/parameter-provider-advanced-ui-params.resolver';

const routes: Routes = [
    {
        path: 'management-controller-services/:id/advanced',
        resolve: { advancedUiParams: controllerServiceAdvancedUiParamsResolver },
        component: AdvancedUi
    },
    {
        path: 'reporting-tasks/:id/advanced',
        resolve: { advancedUiParams: reportingTaskAdvancedUiParamsResolver },
        component: AdvancedUi
    },
    {
        path: 'parameter-providers/:id/advanced',
        resolve: { advancedUiParams: parameterProviderAdvancedUiParamsResolver },
        component: AdvancedUi
    },
    {
        path: '',
        component: Settings,
        canMatch: [authorizationGuard((user: CurrentUser) => user.controllerPermissions.canRead)],
        children: [
            { path: '', pathMatch: 'full', redirectTo: 'general' },
            { path: 'general', component: General },
            {
                path: 'management-controller-services',
                component: ManagementControllerServices,
                children: [
                    {
                        path: ':id',
                        component: ManagementControllerServices,
                        children: [
                            {
                                path: 'edit',
                                component: ManagementControllerServices
                            }
                        ]
                    }
                ]
            },
            {
                path: 'reporting-tasks',
                component: ReportingTasks,
                children: [
                    {
                        path: ':id',
                        component: ReportingTasks,
                        children: [
                            {
                                path: 'edit',
                                component: ReportingTasks
                            }
                        ]
                    }
                ]
            },
            {
                path: 'flow-analysis-rules',
                component: FlowAnalysisRules,
                children: [
                    {
                        path: ':id',
                        component: FlowAnalysisRules,
                        children: [
                            {
                                path: 'edit',
                                component: FlowAnalysisRules
                            }
                        ]
                    }
                ]
            },
            {
                path: 'registry-clients',
                component: RegistryClients,
                children: [
                    {
                        path: ':id',
                        component: RegistryClients,
                        children: [
                            {
                                path: 'edit',
                                component: RegistryClients
                            }
                        ]
                    }
                ]
            },
            {
                path: 'parameter-providers',
                component: ParameterProviders,
                children: [
                    {
                        path: ':id',
                        component: ParameterProviders,
                        children: [
                            {
                                path: 'edit',
                                component: ParameterProviders
                            },
                            {
                                path: 'fetch',
                                component: ParameterProviders
                            }
                        ]
                    }
                ]
            }
        ]
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class SettingsRoutingModule {}
