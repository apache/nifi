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

import { FooterComponent } from './footer.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState as initialFlowState } from '../../../state/flow/flow.reducer';
import { flowFeatureKey } from '../../../state/flow';
import { canvasFeatureKey } from '../../../state';
import { selectBreadcrumbs } from '../../../state/flow/flow.selectors';
import { RouterModule } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { BreadcrumbEntity } from '../../../state/shared';
import { initialState as initialErrorState } from '../../../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../../../state/error';

describe('FooterComponent', () => {
    let component: FooterComponent;
    let fixture: ComponentFixture<FooterComponent>;

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
            imports: [RouterModule, RouterTestingModule],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialFlowState
                        }
                    },
                    selectors: [
                        {
                            selector: selectBreadcrumbs,
                            value: breadcrumbEntity
                        }
                    ]
                })
            ]
        });
        fixture = TestBed.createComponent(FooterComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
