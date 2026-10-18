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

import { NewCanvasItem } from './new-canvas-item.component';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../state/flow/flow.reducer';
import { initialState as initialErrorState } from '../../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../../state/error';
import { initialState as initialCurrentUserState } from '../../../../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../../../../state/current-user';
import { flowConfigurationFeatureKey } from '../../../../../../state/flow-configuration';
import { initialState as flowConfigurationInitialState } from '../../../../../../state/flow-configuration/flow-configuration.reducer';
import { canvasFeatureKey } from '../../../../state';
import { flowFeatureKey } from '../../../../state/flow';
import { transformFeatureKey } from '../../../../state/transform';
import { initialState as transformInitialState } from '../../../../state/transform/transform.reducer';

import { CanvasView } from '../../../../service/canvas-view.service';
import { Client } from '../../../../../../service/client.service';
import { ComponentType } from '@nifi/shared';
import { createComponentRequest } from '../../../../state/flow/flow.actions';

describe('NewCanvasItem', () => {
    let component: NewCanvasItem;
    let fixture: ComponentFixture<NewCanvasItem>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [NewCanvasItem],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [flowConfigurationFeatureKey]: flowConfigurationInitialState,
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialState,
                            [transformFeatureKey]: transformInitialState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(NewCanvasItem);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
    it('places keyboard/AT creation using the actual canvas viewport and existing creation action', () => {
        component.type = ComponentType.Processor;
        vi.spyOn(TestBed.inject(CanvasView), 'getCanvasBoundingClientRect').mockReturnValue({
            left: 200,
            top: 100,
            width: 800,
            height: 500
        } as DOMRect);
        const position = vi.spyOn(TestBed.inject(CanvasView), 'getCanvasPosition').mockReturnValue({ x: 400, y: 250 });
        vi.spyOn(TestBed.inject(Client), 'getClientId').mockReturnValue('keyboard-client');
        const dispatch = vi.spyOn(TestBed.inject(MockStore), 'dispatch');
        component.onActivation(new MouseEvent('click', { detail: 0 }));
        expect(position).toHaveBeenCalledWith({ x: 600, y: 350 });
        expect(dispatch).toHaveBeenCalledExactlyOnceWith(
            createComponentRequest({
                request: {
                    revision: { version: 0, clientId: 'keyboard-client' },
                    type: ComponentType.Processor,
                    position: { x: 400, y: 250 }
                }
            })
        );
    });
    it('does not duplicate a pointer gesture or create when disabled/dragging', () => {
        const position = vi.spyOn(TestBed.inject(CanvasView), 'getCanvasPosition');
        component.onActivation(new MouseEvent('click', { detail: 1 }));
        component.disabled = true;
        component.onActivation(new MouseEvent('click', { detail: 0 }));
        component.disabled = false;
        component.dragging = true;
        component.onActivation(new MouseEvent('click', { detail: 0 }));
        expect(position).not.toHaveBeenCalled();
    });
    it('does not create outside an absent or zero-sized canvas', () => {
        const bounds = vi.spyOn(TestBed.inject(CanvasView), 'getCanvasBoundingClientRect').mockReturnValue(null);
        const position = vi.spyOn(TestBed.inject(CanvasView), 'getCanvasPosition');
        component.onActivation(new MouseEvent('click', { detail: 0 }));
        bounds.mockReturnValue({ left: 0, top: 0, width: 0, height: 0 } as DOMRect);
        component.onActivation(new MouseEvent('click', { detail: 0 }));
        expect(position).not.toHaveBeenCalled();
    });
});
