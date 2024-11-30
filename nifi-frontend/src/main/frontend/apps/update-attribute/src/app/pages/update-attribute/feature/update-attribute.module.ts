/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { UpdateAttribute } from './update-attribute.component';
import { UpdateAttributeRoutingModule } from './update-attribute-routing.module';
import { StoreModule } from '@ngrx/store';
import { updateAttributeFeatureKey, reducers } from '../state';
import { EffectsModule } from '@ngrx/effects';
import { RulesEffects } from '../state/rules/rules.effects';
import { EvaluationContextEffects } from '../state/evaluation-context/evaluation-context.effects';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { RuleListing } from '../ui/rule-listing/rule-listing.component';

@NgModule({
    declarations: [UpdateAttribute],
    exports: [UpdateAttribute],
    imports: [
        CommonModule,
        UpdateAttributeRoutingModule,
        StoreModule.forFeature(updateAttributeFeatureKey, reducers),
        EffectsModule.forFeature(RulesEffects, EvaluationContextEffects),
        MatProgressSpinnerModule,
        RuleListing
    ]
})
export class UpdateAttributeModule {}
