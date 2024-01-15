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

import { GraphControls } from './graph-controls.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../state/flow/flow.reducer';
import { NavigationControl } from './navigation-control/navigation-control.component';
import { OperationControl } from './operation-control/operation-control.component';
import { Component } from '@angular/core';
import { selectBreadcrumbs } from '../../../state/flow/flow.selectors';
import { Birdseye } from './navigation-control/birdseye/birdseye.component';
import { BreadcrumbEntity } from '../../../state/shared';

describe('GraphControls', () => {
    let component: GraphControls;
    let fixture: ComponentFixture<GraphControls>;

    @Component({
        selector: 'birdseye',
        standalone: true,
        template: ''
    })
    class MockBirdseye {}

    beforeEach(() => {
        const breadcrumbEntity: BreadcrumbEntity = {
            id: '',
            permissions: {
                canRead: false,
                canWrite: false
            },
            versionedFlowState: '',
            breadcrumb: {
                id: '',
                name: ''
            }
        };

        TestBed.configureTestingModule({
            imports: [GraphControls, NavigationControl, OperationControl, MockBirdseye],
            providers: [
                provideMockStore({
                    initialState,
                    selectors: [
                        {
                            selector: selectBreadcrumbs,
                            value: breadcrumbEntity
                        }
                    ]
                })
            ]
        }).overrideComponent(NavigationControl, {
            remove: {
                imports: [Birdseye]
            },
            add: {
                imports: [MockBirdseye]
            }
        });

        fixture = TestBed.createComponent(GraphControls);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
