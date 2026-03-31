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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ChangeColorDialog } from './change-color-dialog.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../state/flow/flow.reducer';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { initialState as initialErrorState } from '../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../../state/current-user';
import { canvasFeatureKey } from '../../../state';
import { flowFeatureKey } from '../../../state/flow';
import { initialState as initialTransformState } from '../../../state/transform/transform.reducer';
import { transformFeatureKey } from '../../../state/transform';
import { controllerServicesFeatureKey } from '../../../state/controller-services';
import { initialState as initialControllerServicesState } from '../../../state/controller-services/controller-services.reducer';
import { parameterFeatureKey } from '../../../state/parameter';
import { initialState as initialParameterState } from '../../../state/parameter/parameter.reducer';
import { queueFeatureKey } from '../../../../queue/state';
import { initialState as initialQueueState } from '../../../state/queue/queue.reducer';
import { flowAnalysisFeatureKey } from '../../../state/flow-analysis';
import { initialState as initialFlowAnalysisState } from '../../../state/flow-analysis/flow-analysis.reducer';
import { selectFlowState } from '../../../state/flow/flow.selectors';
import { selectCurrentUser } from '../../../../../state/current-user/current-user.selectors';
import { selectFlowConfiguration } from '../../../../../state/flow-configuration/flow-configuration.selectors';
import * as fromFlowConfiguration from '../../../../../state/flow-configuration/flow-configuration.reducer';
import { flowConfigurationFeatureKey } from '../../../../../state/flow-configuration';
import { loginFeatureKey } from '../../../../login/state';
import { accessFeatureKey } from '../../../../login/state/access';
import { initialState as initialAccessState } from '../../../../login/state/access/access.reducer';

describe('ChangeColorDialog', () => {
    let component: ChangeColorDialog;
    let fixture: ComponentFixture<ChangeColorDialog>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ChangeColorDialog, NoopAnimationsModule],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: {
                        request: []
                    }
                },
                { provide: MatDialogRef, useValue: null },
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [flowConfigurationFeatureKey]: fromFlowConfiguration.initialState,
                        [loginFeatureKey]: {
                            [accessFeatureKey]: initialAccessState
                        },
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
                })
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ChangeColorDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
