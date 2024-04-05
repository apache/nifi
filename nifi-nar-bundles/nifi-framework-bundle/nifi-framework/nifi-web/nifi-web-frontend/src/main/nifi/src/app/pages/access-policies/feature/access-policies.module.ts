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
import { AccessPolicies } from './access-policies.component';
import { AccessPoliciesRoutingModule } from './access-policies-routing.module';
import { StoreModule } from '@ngrx/store';
import { reducers, accessPoliciesFeatureKey } from '../state';
import { EffectsModule } from '@ngrx/effects';
import { MatDialogModule } from '@angular/material/dialog';
import { AccessPolicyEffects } from '../state/access-policy/access-policy.effects';
import { TenantsEffects } from '../state/tenants/tenants.effects';
import { PolicyComponentEffects } from '../state/policy-component/policy-component.effects';
import { Navigation } from '../../../ui/common/navigation/navigation.component';

@NgModule({
    declarations: [AccessPolicies],
    exports: [AccessPolicies],
    imports: [
        CommonModule,
        AccessPoliciesRoutingModule,
        StoreModule.forFeature(accessPoliciesFeatureKey, reducers),
        EffectsModule.forFeature(AccessPolicyEffects, TenantsEffects, PolicyComponentEffects),
        MatDialogModule,
        Navigation
    ]
})
export class AccessPoliciesModule {}
