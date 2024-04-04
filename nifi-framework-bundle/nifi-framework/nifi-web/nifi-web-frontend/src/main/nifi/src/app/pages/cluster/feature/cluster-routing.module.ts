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
import { Cluster } from './cluster.component';
import { NgModule } from '@angular/core';
import { ClusterNodeListing } from '../ui/cluster-node-listing/cluster-node-listing.component';
import { ClusterSystemListing } from '../ui/cluster-system-listing/cluster-system-listing.component';
import { ClusterJvmListing } from '../ui/cluster-jvm-listing/cluster-jvm-listing.component';
import { ClusterFlowFileStorageListing } from '../ui/cluster-flow-file-storage-listing/cluster-flow-file-storage-listing.component';
import { ClusterContentStorageListing } from '../ui/cluster-content-storage-listing/cluster-content-storage-listing.component';
import { ClusterProvenanceStorageListing } from '../ui/cluster-provenance-storage-listing/cluster-provenance-storage-listing.component';
import { ClusterVersionListing } from '../ui/cluster-version-listing/cluster-version-listing.component';
import { authorizationGuard } from '../../../service/guard/authorization.guard';
import { CurrentUser } from '../../../state/current-user';

const routes: Routes = [
    {
        path: '',
        component: Cluster,
        canMatch: [authorizationGuard((user: CurrentUser) => user.controllerPermissions.canRead)],
        children: [
            { path: '', pathMatch: 'full', redirectTo: 'nodes' },
            {
                path: 'nodes',
                component: ClusterNodeListing,
                children: [
                    {
                        path: ':id',
                        component: ClusterNodeListing
                    }
                ]
            },
            {
                path: 'system',
                component: ClusterSystemListing,
                canMatch: [authorizationGuard((user: CurrentUser) => user.systemPermissions.canRead, '/cluster')],
                children: [
                    {
                        path: ':id',
                        component: ClusterSystemListing
                    }
                ]
            },
            {
                path: 'jvm',
                component: ClusterJvmListing,
                canMatch: [authorizationGuard((user: CurrentUser) => user.systemPermissions.canRead, '/cluster')],
                children: [
                    {
                        path: ':id',
                        component: ClusterJvmListing
                    }
                ]
            },
            {
                path: 'flowfile-storage',
                component: ClusterFlowFileStorageListing,
                canMatch: [authorizationGuard((user: CurrentUser) => user.systemPermissions.canRead, '/cluster')],
                children: [
                    {
                        path: ':id',
                        component: ClusterFlowFileStorageListing
                    }
                ]
            },
            {
                path: 'content-storage',
                component: ClusterContentStorageListing,
                canMatch: [authorizationGuard((user: CurrentUser) => user.systemPermissions.canRead, '/cluster')],
                children: [
                    {
                        path: ':id',
                        component: ClusterContentStorageListing,
                        children: [
                            {
                                path: ':repo',
                                component: ClusterContentStorageListing
                            }
                        ]
                    }
                ]
            },
            {
                path: 'provenance-storage',
                component: ClusterProvenanceStorageListing,
                canMatch: [authorizationGuard((user: CurrentUser) => user.systemPermissions.canRead, '/cluster')],
                children: [
                    {
                        path: ':id',
                        component: ClusterProvenanceStorageListing,
                        children: [
                            {
                                path: ':repo',
                                component: ClusterProvenanceStorageListing
                            }
                        ]
                    }
                ]
            },
            {
                path: 'versions',
                component: ClusterVersionListing,
                canMatch: [authorizationGuard((user: CurrentUser) => user.systemPermissions.canRead, '/cluster')],
                children: [
                    {
                        path: ':id',
                        component: ClusterVersionListing
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
export class ClusterRoutingModule {}
