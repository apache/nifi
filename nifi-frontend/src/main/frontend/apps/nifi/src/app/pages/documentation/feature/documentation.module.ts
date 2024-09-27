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
import { Documentation } from './documentation.component';
import { DocumentationRoutingModule } from './documentation-routing.module';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';
import { MatAccordion, MatExpansionModule } from '@angular/material/expansion';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { documentationFeatureKey, reducers } from '../state';
import { ProcessorDefinitionEffects } from '../state/processor-definition/processor-definition.effects';
import { ControllerServiceDefinitionEffects } from '../state/controller-service-definition/controller-service-definition.effects';
import { AdditionalDetailsEffects } from '../state/additional-details/additional-details.effects';
import { DocumentationEffects } from '../state/documentation/documentation.effects';
import { ReportingTaskDefinitionEffects } from '../state/reporting-task-definition/reporting-task-definition.effects';
import { ParameterProviderDefinitionEffects } from '../state/parameter-provider-definition/parameter-provider-definition.effects';
import { FlowAnalysisRuleDefinitionEffects } from '../state/flow-analysis-rule-definition/flow-analysis-rule-definition.effects';

@NgModule({
    declarations: [Documentation],
    exports: [Documentation],
    imports: [
        CommonModule,
        DocumentationRoutingModule,
        StoreModule.forFeature(documentationFeatureKey, reducers),
        EffectsModule.forFeature(
            ProcessorDefinitionEffects,
            ControllerServiceDefinitionEffects,
            ReportingTaskDefinitionEffects,
            ParameterProviderDefinitionEffects,
            FlowAnalysisRuleDefinitionEffects,
            AdditionalDetailsEffects,
            DocumentationEffects
        ),
        Navigation,
        BannerText,
        MatAccordion,
        MatExpansionModule,
        FormsModule,
        MatFormFieldModule,
        MatInputModule,
        ReactiveFormsModule
    ]
})
export class DocumentationModule {}
