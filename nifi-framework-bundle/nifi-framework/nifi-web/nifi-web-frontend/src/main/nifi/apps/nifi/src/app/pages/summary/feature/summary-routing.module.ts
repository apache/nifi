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
import { Summary } from './summary.component';
import { NgModule } from '@angular/core';
import { ProcessorStatusListing } from '../ui/processor-status-listing/processor-status-listing.component';
import { InputPortStatusListing } from '../ui/input-port-status-listing/input-port-status-listing.component';
import { OutputPortStatusListing } from '../ui/output-port-status-listing/output-port-status-listing.component';
import { RemoteProcessGroupStatusListing } from '../ui/remote-process-group-status-listing/remote-process-group-status-listing.component';
import { ConnectionStatusListing } from '../ui/connection-status-listing/connection-status-listing.component';
import { ProcessGroupStatusListing } from '../ui/process-group-status-listing/process-group-status-listing.component';

const routes: Routes = [
    {
        path: '',
        component: Summary,
        children: [
            { path: '', pathMatch: 'full', redirectTo: 'processors' },
            {
                path: 'processors',
                component: ProcessorStatusListing,
                children: [
                    {
                        path: ':id',
                        component: ProcessorStatusListing,
                        children: [
                            {
                                path: 'history',
                                component: ProcessorStatusListing
                            }
                        ]
                    }
                ]
            },
            {
                path: 'input-ports',
                component: InputPortStatusListing,
                children: [
                    {
                        path: ':id',
                        component: InputPortStatusListing
                    }
                ]
            },
            {
                path: 'output-ports',
                component: OutputPortStatusListing,
                children: [
                    {
                        path: ':id',
                        component: OutputPortStatusListing
                    }
                ]
            },
            {
                path: 'remote-process-groups',
                component: RemoteProcessGroupStatusListing,
                children: [
                    {
                        path: ':id',
                        component: RemoteProcessGroupStatusListing,
                        children: [
                            {
                                path: 'history',
                                component: RemoteProcessGroupStatusListing
                            }
                        ]
                    }
                ]
            },
            {
                path: 'connections',
                component: ConnectionStatusListing,
                children: [
                    {
                        path: ':id',
                        component: ConnectionStatusListing,
                        children: [
                            {
                                path: 'history',
                                component: ConnectionStatusListing
                            }
                        ]
                    }
                ]
            },
            {
                path: 'process-groups',
                component: ProcessGroupStatusListing,
                children: [
                    {
                        path: ':id',
                        component: ProcessGroupStatusListing,
                        children: [
                            {
                                path: 'history',
                                component: ProcessGroupStatusListing
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
export class SummaryRoutingModule {}
