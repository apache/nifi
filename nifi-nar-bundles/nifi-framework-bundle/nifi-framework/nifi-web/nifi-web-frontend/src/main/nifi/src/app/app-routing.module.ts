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
import { authenticationGuard } from './service/guard/authentication.guard';

const routes: Routes = [
    {
        path: 'login',
        loadChildren: () => import('./pages/login/feature/login.module').then((m) => m.LoginModule)
    },
    {
        path: 'settings',
        canMatch: [authenticationGuard],
        loadChildren: () => import('./pages/settings/feature/settings.module').then((m) => m.SettingsModule)
    },
    {
        path: 'provenance',
        canMatch: [authenticationGuard],
        loadChildren: () => import('./pages/provenance/feature/provenance.module').then((m) => m.ProvenanceModule)
    },
    {
        path: 'parameter-contexts',
        canMatch: [authenticationGuard],
        loadChildren: () =>
            import('./pages/parameter-contexts/feature/parameter-contexts.module').then(
                (m) => m.ParameterContextsModule
            )
    },
    {
        path: 'counters',
        canMatch: [authenticationGuard],
        loadChildren: () => import('./pages/counters/feature/counters.module').then((m) => m.CountersModule)
    },
    {
        path: 'users',
        canMatch: [authenticationGuard],
        loadChildren: () => import('./pages/users/feature/users.module').then((m) => m.UsersModule)
    },
    {
        path: 'summary',
        canMatch: [authenticationGuard],
        loadChildren: () => import('./pages/summary/feature/summary.module').then((m) => m.SummaryModule)
    },
    {
        path: 'bulletins',
        canMatch: [authenticationGuard],
        loadChildren: () => import('./pages/bulletins/feature/bulletins.module').then((m) => m.BulletinsModule)
    },
    {
        path: '',
        canMatch: [authenticationGuard],
        loadChildren: () =>
            import('./pages/flow-designer/feature/flow-designer.module').then((m) => m.FlowDesignerModule)
    }
];

@NgModule({
    imports: [
        RouterModule.forRoot(routes, {
            paramsInheritanceStrategy: 'always',
            useHash: true
        })
    ],
    exports: [RouterModule]
})
export class AppRoutingModule {}
