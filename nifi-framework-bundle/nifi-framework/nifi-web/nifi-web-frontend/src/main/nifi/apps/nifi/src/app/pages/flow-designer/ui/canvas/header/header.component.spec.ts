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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { HeaderComponent } from './header.component';
import { provideMockStore } from '@ngrx/store/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NewCanvasItem } from './new-canvas-item/new-canvas-item.component';
import { MatMenuModule } from '@angular/material/menu';
import { MatDividerModule } from '@angular/material/divider';
import { selectControllerBulletins, selectControllerStatus } from '../../../state/flow/flow.selectors';
import { ControllerStatus, flowFeatureKey } from '../../../state/flow';
import { CdkConnectedOverlay, CdkOverlayOrigin } from '@angular/cdk/overlay';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ClusterSummary } from '../../../../../state/cluster-summary';
import { selectClusterSummary } from '../../../../../state/cluster-summary/cluster-summary.selectors';
import { selectCurrentUser } from '../../../../../state/current-user/current-user.selectors';
import * as fromUser from '../../../../../state/current-user/current-user.reducer';
import * as fromNavigation from '../../../../../state/navigation/navigation.reducer';
import * as fromFlow from '../../../state/flow/flow.reducer';
import { selectFlowConfiguration } from '../../../../../state/flow-configuration/flow-configuration.selectors';
import * as fromFlowConfiguration from '../../../../../state/flow-configuration/flow-configuration.reducer';
import { selectLoginConfiguration } from '../../../../../state/login-configuration/login-configuration.selectors';
import * as fromLoginConfiguration from '../../../../../state/login-configuration/login-configuration.reducer';
import { navigationFeatureKey } from '../../../../../state/navigation';
import { canvasFeatureKey } from '../../../state';
import { MockComponent } from 'ng-mocks';
import { FlowStatus } from './flow-status/flow-status.component';
import { Navigation } from '../../../../../ui/common/navigation/navigation.component';

describe('HeaderComponent', () => {
    let component: HeaderComponent;
    let fixture: ComponentFixture<HeaderComponent>;

    const clusterSummary: ClusterSummary = {
        clustered: false,
        connectedToCluster: false,
        connectedNodes: '',
        connectedNodeCount: 0,
        totalNodeCount: 0
    };
    const controllerStatus: ControllerStatus = {
        activeThreadCount: 0,
        terminatedThreadCount: 0,
        queued: '0 / 0 bytes',
        flowFilesQueued: 0,
        bytesQueued: 0,
        runningCount: 0,
        stoppedCount: 3,
        invalidCount: 12,
        disabledCount: 0,
        activeRemotePortCount: 0,
        inactiveRemotePortCount: 2,
        upToDateCount: 0,
        locallyModifiedCount: 0,
        staleCount: 0,
        locallyModifiedAndStaleCount: 0,
        syncFailureCount: 0
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [
                HeaderComponent,
                MockComponent(NewCanvasItem),
                HttpClientTestingModule,
                MockComponent(FlowStatus),
                MatMenuModule,
                MatDividerModule,
                RouterTestingModule,
                CdkOverlayOrigin,
                CdkConnectedOverlay,
                FormsModule,
                ReactiveFormsModule,
                MockComponent(Navigation)
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: fromFlow.initialState,
                            [navigationFeatureKey]: fromNavigation.initialState
                        }
                    },
                    selectors: [
                        {
                            selector: selectClusterSummary,
                            value: clusterSummary
                        },
                        {
                            selector: selectControllerStatus,
                            value: controllerStatus
                        },
                        {
                            selector: selectControllerBulletins,
                            value: []
                        },
                        {
                            selector: selectCurrentUser,
                            value: fromUser.initialState.user
                        },
                        {
                            selector: selectFlowConfiguration,
                            value: fromFlowConfiguration.initialState.flowConfiguration
                        },
                        {
                            selector: selectLoginConfiguration,
                            value: fromLoginConfiguration.initialState.loginConfiguration
                        }
                    ]
                })
            ]
        });
        fixture = TestBed.createComponent(HeaderComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
