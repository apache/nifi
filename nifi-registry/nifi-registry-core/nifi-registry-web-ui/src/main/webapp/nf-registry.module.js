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

import { HttpClientModule, HTTP_INTERCEPTORS } from '@angular/common/http';
import { MomentModule } from 'angular2-moment';
import { NgModule } from '@angular/core';

import NfRegistryRoutes from 'nf-registry.routes';
import { FdsCoreModule } from '@nifi-fds/core';
import NfRegistry from 'nf-registry';
import NfRegistryApi from 'services/nf-registry.api';
import NfRegistryService from 'services/nf-registry.service';
import NfPageNotFoundComponent from 'components/page-not-found/nf-registry-page-not-found';
import NfRegistryExplorer from 'components/explorer/nf-registry-explorer';
import NfRegistryAdministration from 'components/administration/nf-registry-administration';
import NfRegistryUsersAdministration from 'components/administration/users/nf-registry-users-administration';
import NfRegistryAddUser from 'components/administration/users/dialogs/add-user/nf-registry-add-user';
import NfRegistryManageGroup from 'components/administration/users/sidenav/manage-group/nf-registry-manage-group';
import NfRegistryManageUser from 'components/administration/users/sidenav/manage-user/nf-registry-manage-user';
import NfRegistryManageBucket from 'components/administration/workflow/sidenav/manage-bucket/nf-registry-manage-bucket';
import NfRegistryWorkflowAdministration from 'components/administration/workflow/nf-registry-workflow-administration';
import NfRegistryGridListViewer from 'components/explorer/grid-list/registry/nf-registry-grid-list-viewer';
import NfRegistryBucketGridListViewer from 'components/explorer/grid-list/registry/nf-registry-bucket-grid-list-viewer';
import NfRegistryDropletGridListViewer from 'components/explorer/grid-list/registry/nf-registry-droplet-grid-list-viewer';
import NfRegistryTokenInterceptor from 'services/nf-registry.token.interceptor';
import NfStorage from 'services/nf-storage.service';
import NfLoginComponent from 'components/login/nf-registry-login';
import NfUserLoginComponent from 'components/login/dialogs/nf-registry-user-login';
import NfRegistryCreateBucket from 'components/administration/workflow/dialogs/create-bucket/nf-registry-create-bucket';
import NfRegistryAddUsersToGroup from 'components/administration/users/dialogs/add-users-to-group/nf-registry-add-users-to-group';
import NfRegistryAddUserToGroups from 'components/administration/users/dialogs/add-user-to-groups/nf-registry-add-user-to-groups';
import NfRegistryAddPolicyToBucket from 'components/administration/workflow/dialogs/add-policy-to-bucket/nf-registry-add-policy-to-bucket';
import NfRegistryEditBucketPolicy from 'components/administration/workflow/dialogs/edit-bucket-policy/nf-registry-edit-bucket-policy';
import NfRegistryCreateNewGroup from 'components/administration/users/dialogs/create-new-group/nf-registry-create-new-group';
import {
    NfRegistryLoginAuthGuard,
    NfRegistryResourcesAuthGuard,
    NfRegistryUsersAdministrationAuthGuard,
    NfRegistryWorkflowsAdministrationAuthGuard
} from 'services/nf-registry.auth-guard.service';
import NfRegistryImportVersionedFlow from './components/explorer/grid-list/dialogs/import-versioned-flow/nf-registry-import-versioned-flow';
import NfRegistryImportNewFlow from './components/explorer/grid-list/dialogs/import-new-flow/nf-registry-import-new-flow';
import NfRegistryExportVersionedFlow from './components/explorer/grid-list/dialogs/export-versioned-flow/nf-registry-export-versioned-flow';

function NfRegistryModule() {
}

NfRegistryModule.prototype = {
    constructor: NfRegistryModule
};

NfRegistryModule.annotations = [
    new NgModule({
        imports: [
            MomentModule,
            FdsCoreModule,
            HttpClientModule,
            NfRegistryRoutes
        ],
        declarations: [
            NfRegistry,
            NfRegistryExplorer,
            NfRegistryAdministration,
            NfRegistryUsersAdministration,
            NfRegistryManageUser,
            NfRegistryManageGroup,
            NfRegistryManageBucket,
            NfRegistryWorkflowAdministration,
            NfRegistryAddUser,
            NfRegistryCreateBucket,
            NfRegistryCreateNewGroup,
            NfRegistryAddUserToGroups,
            NfRegistryAddUsersToGroup,
            NfRegistryAddPolicyToBucket,
            NfRegistryEditBucketPolicy,
            NfRegistryGridListViewer,
            NfRegistryBucketGridListViewer,
            NfRegistryDropletGridListViewer,
            NfPageNotFoundComponent,
            NfLoginComponent,
            NfUserLoginComponent,
            NfRegistryExportVersionedFlow,
            NfRegistryImportVersionedFlow,
            NfRegistryImportNewFlow
        ],
        entryComponents: [
            NfRegistryAddUser,
            NfRegistryCreateBucket,
            NfRegistryCreateNewGroup,
            NfRegistryAddUserToGroups,
            NfRegistryAddUsersToGroup,
            NfRegistryAddPolicyToBucket,
            NfRegistryEditBucketPolicy,
            NfUserLoginComponent,
            NfRegistryExportVersionedFlow,
            NfRegistryImportVersionedFlow,
            NfRegistryImportNewFlow
        ],
        providers: [
            NfRegistryService,
            NfRegistryUsersAdministrationAuthGuard,
            NfRegistryWorkflowsAdministrationAuthGuard,
            NfRegistryLoginAuthGuard,
            NfRegistryResourcesAuthGuard,
            NfRegistryApi,
            NfStorage,
            {
                provide: HTTP_INTERCEPTORS,
                useClass: NfRegistryTokenInterceptor,
                multi: true
            }
        ],
        bootstrap: [NfRegistry]
    })
];

export default NfRegistryModule;
