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

import { TestBed } from '@angular/core/testing';
import {
    BrowserDynamicTestingModule,
    platformBrowserDynamicTesting
} from '@angular/platform-browser-dynamic/testing';
import { HttpClientModule, HTTP_INTERCEPTORS } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { MomentModule } from 'angular2-moment';

import 'hammerjs';
import NfRegistryRoutes from 'nf-registry.routes';
import { APP_BASE_HREF } from '@angular/common';
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

const initTestBed = ({ providers } = { providers: [] }) => {
    TestBed.resetTestEnvironment();

    TestBed.initTestEnvironment(
        BrowserDynamicTestingModule,
        platformBrowserDynamicTesting()
    );

    const testBedConfigured = TestBed.configureTestingModule({
        imports: [
            MomentModule,
            HttpClientModule,
            HttpClientTestingModule,
            FdsCoreModule,
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
            NfRegistryAddUser,
            NfRegistryWorkflowAdministration,
            NfRegistryGridListViewer,
            NfRegistryBucketGridListViewer,
            NfRegistryDropletGridListViewer,
            NfPageNotFoundComponent,
            NfLoginComponent,
            NfUserLoginComponent
        ],
        providers: [
            NfRegistryService,
            NfRegistryApi,
            NfStorage,
            {
                provide: HTTP_INTERCEPTORS,
                useClass: NfRegistryTokenInterceptor,
                multi: true
            },
            {
                provide: APP_BASE_HREF,
                useValue: '/'
            },

            ...providers

        ],
        bootstrap: [NfRegistry]
    }).compileComponents();

    return testBedConfigured;
};

export default initTestBed;
