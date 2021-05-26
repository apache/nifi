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

import { RouterModule } from '@angular/router';
import NfPageNotFoundComponent from 'components/page-not-found/nf-registry-page-not-found';
import NfLoginComponent from 'components/login/nf-registry-login';
import NfRegistryExplorer from 'components/explorer/nf-registry-explorer';
import NfRegistryAdministration from 'components/administration/nf-registry-administration';
import NfRegistryUsersAdministration from 'components/administration/users/nf-registry-users-administration';
import NfRegistryManageUser from 'components/administration/users/sidenav/manage-user/nf-registry-manage-user';
import NfRegistryManageGroup from 'components/administration/users/sidenav/manage-group/nf-registry-manage-group';
import NfRegistryManageBucket from 'components/administration/workflow/sidenav/manage-bucket/nf-registry-manage-bucket';
import NfRegistryWorkflowAdministration from 'components/administration/workflow/nf-registry-workflow-administration';
import NfRegistryGridListViewer from 'components/explorer/grid-list/registry/nf-registry-grid-list-viewer';
import NfRegistryBucketGridListViewer from 'components/explorer/grid-list/registry/nf-registry-bucket-grid-list-viewer';
import NfRegistryDropletGridListViewer from 'components/explorer/grid-list/registry/nf-registry-droplet-grid-list-viewer';
import {
    NfRegistryLoginAuthGuard,
    NfRegistryResourcesAuthGuard,
    NfRegistryUsersAdministrationAuthGuard,
    NfRegistryWorkflowsAdministrationAuthGuard
} from 'services/nf-registry.auth-guard.service';

// eslint-disable-next-line new-cap
const NfRegistryRoutes = new RouterModule.forRoot([{
    path: 'explorer',
    component: NfRegistryExplorer,
    children: [
        {
            path: '',
            redirectTo: 'grid-list',
            pathMatch: 'full'
        },
        {
            path: 'grid-list',
            component: NfRegistryGridListViewer,
            canActivate: [NfRegistryResourcesAuthGuard]
        }, {
            path: 'grid-list/buckets/:bucketId',
            component: NfRegistryBucketGridListViewer,
            canActivate: [NfRegistryResourcesAuthGuard]
        },
        {
            path: 'grid-list/buckets/:bucketId/:dropletType/:dropletId',
            component: NfRegistryDropletGridListViewer,
            canActivate: [NfRegistryResourcesAuthGuard]
        }
    ]
}, {
    path: 'login',
    component: NfLoginComponent,
    canActivate: [NfRegistryLoginAuthGuard]
}, {
    path: 'administration',
    component: NfRegistryAdministration,
    children: [{
        path: '',
        redirectTo: 'workflow',
        pathMatch: 'full'
    }, {
        path: 'users',
        component: NfRegistryUsersAdministration,
        canActivate: [NfRegistryUsersAdministrationAuthGuard]
    }, {
        path: 'workflow',
        component: NfRegistryWorkflowAdministration,
        canActivate: [NfRegistryWorkflowsAdministrationAuthGuard]
    }]
}, {
    path: 'explorer/grid-list/buckets',
    redirectTo: '/explorer/grid-list',
    pathMatch: 'full'
}, {
    path: 'nifi-registry',
    redirectTo: 'explorer/grid-list'
}, {
    path: '',
    redirectTo: 'explorer/grid-list',
    pathMatch: 'full'
}, {
    path: '**',
    component: NfPageNotFoundComponent
}, {
    path: 'manage/user/:userId',
    component: NfRegistryManageUser,
    canActivate: [NfRegistryUsersAdministrationAuthGuard],
    outlet: 'sidenav'
}, {
    path: 'manage/group/:groupId',
    component: NfRegistryManageGroup,
    canActivate: [NfRegistryUsersAdministrationAuthGuard],
    outlet: 'sidenav'
}, {
    path: 'manage/bucket/:bucketId',
    component: NfRegistryManageBucket,
    canActivate: [NfRegistryWorkflowsAdministrationAuthGuard],
    outlet: 'sidenav'
}], { useHash: true });

export default NfRegistryRoutes;
