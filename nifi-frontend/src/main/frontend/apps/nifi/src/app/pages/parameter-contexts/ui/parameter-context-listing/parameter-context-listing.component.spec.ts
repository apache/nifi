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

import { ParameterContextListing } from './parameter-context-listing.component';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/parameter-context-listing/parameter-context-listing.reducer';
import { parameterContextListingFeatureKey } from '../../state/parameter-context-listing';
import { parameterContextsFeatureKey } from '../../state';
import { selectSingleEditedParameterContext } from '../../state/parameter-context-listing/parameter-context-listing.selectors';
import { getEffectiveParameterContextAndOpenDialog } from '../../state/parameter-context-listing/parameter-context-listing.actions';
import { Navigation, Router } from '@angular/router';
import { ParameterContextEntity } from '../../../../state/shared';

describe('ParameterContextListing', () => {
    let component: ParameterContextListing;
    let fixture: ComponentFixture<ParameterContextListing>;

    const parameterContext: ParameterContextEntity = {
        revision: {
            version: 0
        },
        id: '1234',
        uri: 'https://localhost:4200/nifi-api/parameter-contexts/1234',
        permissions: {
            canRead: true,
            canWrite: true
        },
        component: {
            name: 'params 1',
            description: '',
            parameters: [],
            boundProcessGroups: [],
            inheritedParameterContexts: [],
            id: '1234'
        }
    };

    let lastSuccessfulNavigation: Navigation | null = null;

    const configureTestBed = (editedParameterContextId: string | null, parameterContexts: ParameterContextEntity[]) => {
        TestBed.configureTestingModule({
            imports: [ParameterContextListing],
            providers: [
                provideMockStore({
                    initialState: {
                        [parameterContextsFeatureKey]: {
                            [parameterContextListingFeatureKey]: {
                                ...initialState,
                                parameterContexts
                            }
                        }
                    }
                }),
                {
                    provide: Router,
                    useValue: {
                        lastSuccessfulNavigation: () => lastSuccessfulNavigation
                    }
                }
            ]
        });

        const store: MockStore = TestBed.inject(MockStore);
        store.overrideSelector(selectSingleEditedParameterContext, editedParameterContextId);

        return store;
    };

    beforeEach(() => {
        lastSuccessfulNavigation = null;
    });

    it('should create', () => {
        configureTestBed(null, []);
        fixture = TestBed.createComponent(ParameterContextListing);
        component = fixture.componentInstance;
        fixture.detectChanges();

        expect(component).toBeTruthy();
    });

    describe('when the edit route is active', () => {
        it('should open the dialog with the highlighted parameter from the navigation state', () => {
            lastSuccessfulNavigation = {
                extras: {
                    state: {
                        highlightedParameterName: 'param A'
                    }
                }
            } as unknown as Navigation;

            const store = configureTestBed(parameterContext.id, [parameterContext]);
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            fixture = TestBed.createComponent(ParameterContextListing);
            fixture.detectChanges();

            expect(dispatchSpy).toHaveBeenCalledWith(
                getEffectiveParameterContextAndOpenDialog({
                    request: {
                        id: parameterContext.id,
                        highlightedParameterName: 'param A'
                    }
                })
            );
        });

        it('should open the dialog without a highlighted parameter when the navigation state is absent', () => {
            const store = configureTestBed(parameterContext.id, [parameterContext]);
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            fixture = TestBed.createComponent(ParameterContextListing);
            fixture.detectChanges();

            expect(dispatchSpy).toHaveBeenCalledWith(
                getEffectiveParameterContextAndOpenDialog({
                    request: {
                        id: parameterContext.id,
                        highlightedParameterName: undefined
                    }
                })
            );
        });

        it('should not open the dialog until the parameter context is loaded', () => {
            const store = configureTestBed(parameterContext.id, []);
            const dispatchSpy = vi.spyOn(store, 'dispatch');

            fixture = TestBed.createComponent(ParameterContextListing);
            fixture.detectChanges();

            expect(dispatchSpy).not.toHaveBeenCalledWith(
                expect.objectContaining({ type: getEffectiveParameterContextAndOpenDialog.type })
            );
        });
    });
});
