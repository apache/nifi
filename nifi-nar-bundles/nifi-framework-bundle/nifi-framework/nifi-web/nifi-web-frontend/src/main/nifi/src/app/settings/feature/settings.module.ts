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
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { Settings } from './settings.component';
import { reducers, settingsFeatureKey } from '../state';
import { SettingsRoutingModule } from './settings-routing.module';
import { MatTabsModule } from '@angular/material/tabs';
import { General } from '../ui/general/general.component';
import { ManagementControllerServices } from '../ui/management-controller-services/management-controller-services.component';
import { ReportingTasks } from '../ui/reporting-tasks/reporting-tasks.component';
import { FlowAnalysisRules } from '../ui/flow-analysis-rules/flow-analysis-rules.component';
import { RegistryClients } from '../ui/registry-clients/registry-clients.component';
import { ParameterProviders } from '../ui/parameter-providers/parameter-providers.component';
import { GeneralEffects } from '../state/general/general.effects';
import { GeneralForm } from '../ui/general/general-form/general-form.component';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';

@NgModule({
    declarations: [
        Settings,
        General,
        GeneralForm,
        ManagementControllerServices,
        ReportingTasks,
        FlowAnalysisRules,
        RegistryClients,
        ParameterProviders
    ],
    exports: [Settings],
    imports: [
        CommonModule,
        SettingsRoutingModule,
        StoreModule.forFeature(settingsFeatureKey, reducers),
        EffectsModule.forFeature(GeneralEffects),
        FormsModule,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatInputModule,
        MatButtonModule,
        MatTabsModule,
        NgxSkeletonLoaderModule
    ]
})
export class SettingsModule {}
