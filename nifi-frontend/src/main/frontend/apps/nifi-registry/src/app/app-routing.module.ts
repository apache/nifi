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
import { ExplorerComponent } from './pages/expolorer/feature/explorer.component';
import { Error } from './pages/error/feature/error.component';

const routes: Routes = [
    {
        path: 'error',
        loadComponent: () => Error
    },
    {
        path: 'explorer',
        component: ExplorerComponent,
        children: [
            {
                path: ':id',
                component: ExplorerComponent
            }
        ]
    },
    {
        path: 'nifi-registry',
        redirectTo: 'explorer'
    },
    {
        path: '',
        redirectTo: 'explorer',
        pathMatch: 'full'
    },
    {
        path: '',
        loadComponent: () => import('./pages/expolorer/feature/explorer.component').then((m) => m.ExplorerComponent)
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
