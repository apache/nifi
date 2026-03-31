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

import { EditPort } from './edit-port.component';
import { EditComponentDialogRequest } from '../../../../../state/flow';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { ComponentType } from '@nifi/shared';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';
import { initialState as initialErrorState } from '../../../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../../../../state/current-user';
import { canvasFeatureKey } from '../../../../../state';
import { flowFeatureKey } from '../../../../../state/flow';
import { initialState as initialTransformState } from '../../../../../state/transform/transform.reducer';
import { transformFeatureKey } from '../../../../../state/transform';
import { controllerServicesFeatureKey } from '../../../../../state/controller-services';
import { initialState as initialControllerServicesState } from '../../../../../state/controller-services/controller-services.reducer';
import { parameterFeatureKey } from '../../../../../state/parameter';
import { initialState as initialParameterState } from '../../../../../state/parameter/parameter.reducer';
import { queueFeatureKey } from '../../../../../../queue/state';
import { initialState as initialQueueState } from '../../../../../state/queue/queue.reducer';
import { flowAnalysisFeatureKey } from '../../../../../state/flow-analysis';
import { initialState as initialFlowAnalysisState } from '../../../../../state/flow-analysis/flow-analysis.reducer';
import { selectFlowState } from '../../../../../state/flow/flow.selectors';
import { selectCurrentUser } from '../../../../../../../state/current-user/current-user.selectors';
import { selectFlowConfiguration } from '../../../../../../../state/flow-configuration/flow-configuration.selectors';
import * as fromFlowConfiguration from '../../../../../../../state/flow-configuration/flow-configuration.reducer';
import { flowConfigurationFeatureKey } from '../../../../../../../state/flow-configuration';

describe('EditPort', () => {
    let component: EditPort;
    let fixture: ComponentFixture<EditPort>;

    const data: EditComponentDialogRequest = {
        type: ComponentType.OutputPort,
        uri: 'https://localhost:4200/nifi-api/output-ports/a687e30e-018b-1000-f904-849a9f8e6bdb',
        entity: {
            revision: {
                version: 0
            },
            id: 'a687e30e-018b-1000-f904-849a9f8e6bdb',
            uri: 'https://localhost:4200/nifi-api/output-ports/a687e30e-018b-1000-f904-849a9f8e6bdb',
            position: {
                x: 912,
                y: -48
            },
            permissions: {
                canRead: true,
                canWrite: true
            },
            bulletins: [],
            component: {
                id: 'a687e30e-018b-1000-f904-849a9f8e6bdb',
                versionedComponentId: '56cf65da-e2cd-3ec5-9d69-d73c382a9049',
                parentGroupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                position: {
                    x: 912,
                    y: -48
                },
                name: 'out',
                state: 'STOPPED',
                type: 'OUTPUT_PORT',
                transmitting: false,
                concurrentlySchedulableTaskCount: 1,
                allowRemoteAccess: true,
                portFunction: 'STANDARD'
            },
            status: {
                id: 'a687e30e-018b-1000-f904-849a9f8e6bdb',
                groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                name: 'out',
                transmitting: false,
                runStatus: 'Stopped',
                statsLastRefreshed: '13:38:10 EST',
                aggregateSnapshot: {
                    id: 'a687e30e-018b-1000-f904-849a9f8e6bdb',
                    groupId: '95a4b210-018b-1000-772a-5a9ebfa03287',
                    name: 'out',
                    activeThreadCount: 0,
                    flowFilesIn: 0,
                    bytesIn: 0,
                    input: '0 (0 bytes)',
                    flowFilesOut: 0,
                    bytesOut: 0,
                    output: '0 (0 bytes)',
                    runStatus: 'Stopped'
                }
            },
            portType: 'OUTPUT_PORT',
            operatePermissions: {
                canRead: false,
                canWrite: false
            },
            allowRemoteAccess: true
        }
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [EditPort, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [flowConfigurationFeatureKey]: fromFlowConfiguration.initialState,
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialState,
                            [transformFeatureKey]: initialTransformState,
                            [controllerServicesFeatureKey]: initialControllerServicesState,
                            [parameterFeatureKey]: initialParameterState,
                            [queueFeatureKey]: initialQueueState,
                            [flowAnalysisFeatureKey]: initialFlowAnalysisState
                        }
                    },
                    selectors: [
                        {
                            selector: selectFlowState,
                            value: initialState
                        },
                        {
                            selector: selectCurrentUser,
                            value: initialCurrentUserState.user
                        },
                        {
                            selector: selectFlowConfiguration,
                            value: fromFlowConfiguration.initialState.flowConfiguration
                        }
                    ]
                }),
                {
                    provide: ClusterConnectionService,
                    useValue: {
                        isDisconnectionAcknowledged: vi.fn()
                    }
                },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(EditPort);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
