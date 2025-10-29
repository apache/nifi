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

import { RouterModule, Routes } from '@angular/router';
import { NgModule } from '@angular/core';
import { ResourcesComponent } from './resources.component';
import { resourcesActivateGuard } from '../../../service/guard/resources.guard';

const routes: Routes = [
    {
        path: '',
        component: ResourcesComponent,
        canActivate: [resourcesActivateGuard],
        children: [
            {
                path: ':id',
                component: ResourcesComponent,
                canActivate: [resourcesActivateGuard]
            }
        ]
    },
    // Backward compatibility routes for old NiFi Registry deep links
    {
        path: 'grid-list',
        redirectTo: '',
        pathMatch: 'full'
    },
    {
        path: 'grid-list/buckets/:bucketId',
        redirectTo: '',
        pathMatch: 'full'
    },
    {
        path: 'grid-list/buckets/:bucketId/:dropletType/:dropletId',
        redirectTo: '',
        pathMatch: 'full'
    },
    {
        path: 'grid-list/buckets',
        redirectTo: '',
        pathMatch: 'full'
    },
    // Catch-all for any other grid-list sub-routes
    {
        path: 'grid-list/**',
        redirectTo: ''
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class ResourcesRoutingModule {}
