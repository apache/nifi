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

import { RouterModule, Routes } from '@angular/router';
import { Documentation } from './documentation.component';
import { ProcessorDefinition } from '../ui/processor-definition/processor-definition.component';
import { NgModule } from '@angular/core';
import { ComponentType } from '@nifi/shared';
import { ControllerServiceDefinition } from '../ui/controller-service-definition/controller-service-definition.component';
import { ReportingTaskDefinition } from '../ui/reporting-task-definition/reporting-task-definition.component';
import { ParameterProviderDefinition } from '../ui/parameter-provider-definition/parameter-provider-definition.component';
import { FlowAnalysisRuleDefinition } from '../ui/flow-analysis-rule-definition/flow-analysis-rule-definition.component';
import { Overview } from '../ui/overview/overview.component';

const routes: Routes = [
    {
        path: '',
        component: Documentation,
        children: [
            { path: '', pathMatch: 'full', redirectTo: 'overview' },
            {
                path: `${ComponentType.Processor}/:group/:artifact/:version/:type`,
                component: ProcessorDefinition
            },
            {
                path: `${ComponentType.ControllerService}/:group/:artifact/:version/:type`,
                component: ControllerServiceDefinition
            },
            {
                path: `${ComponentType.ReportingTask}/:group/:artifact/:version/:type`,
                component: ReportingTaskDefinition
            },
            {
                path: `${ComponentType.ParameterProvider}/:group/:artifact/:version/:type`,
                component: ParameterProviderDefinition
            },
            {
                path: `${ComponentType.FlowAnalysisRule}/:group/:artifact/:version/:type`,
                component: FlowAnalysisRuleDefinition
            },
            {
                path: 'overview',
                component: Overview
            }
        ]
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class DocumentationRoutingModule {}
