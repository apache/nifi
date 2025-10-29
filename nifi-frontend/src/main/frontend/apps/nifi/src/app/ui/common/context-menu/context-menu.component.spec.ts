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

import { ContextMenu } from './context-menu.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState as initialFlowState } from '../../../pages/flow-designer/state/flow/flow.reducer';
import { flowFeatureKey } from '../../../pages/flow-designer/state/flow';
import { canvasFeatureKey } from '../../../pages/flow-designer/state';
import { initialState as initialErrorState } from '../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../state/error';
import { initialState as initialCurrentUserState } from '../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../state/current-user';

describe('ContextMenu', () => {
    let component: ContextMenu;
    let fixture: ComponentFixture<ContextMenu>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ContextMenu],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialFlowState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(ContextMenu);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
