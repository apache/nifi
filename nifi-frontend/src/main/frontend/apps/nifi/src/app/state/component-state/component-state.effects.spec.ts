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
import { provideMockActions } from '@ngrx/effects/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { Action } from '@ngrx/store';
import { Observable, of, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';

import { ComponentStateEffects } from './component-state.effects';
import { ComponentStateService } from '../../service/component-state.service';
import { ErrorHelper } from '../../service/error-helper.service';
import * as ComponentStateActions from './component-state.actions';
import * as ErrorActions from '../error/error.actions';
import { ComponentState, ComponentStateEntity, ClearStateEntryRequest } from './index';
import { selectComponentType, selectComponentId, selectComponentState } from './component-state.selectors';
import { ErrorContextKey } from '../error';
import { ComponentType } from '@nifi/shared';
import { initialState as initialErrorState } from '../error/error.reducer';
import { errorFeatureKey } from '../error';
import { initialState as initialCurrentUserState } from '../current-user/current-user.reducer';
import { currentUserFeatureKey } from '../current-user';

describe('ComponentStateEffects', () => {
    let actions$: Observable<Action>;
    let effects: ComponentStateEffects;
    let componentStateService: jest.Mocked<ComponentStateService>;
    let store: MockStore;
    let errorHelper: jest.Mocked<ErrorHelper>;

    const mockComponentState: ComponentState = {
        componentId: 'test-component-id',
        stateDescription: 'Test state description',
        localState: {
            scope: 'LOCAL',
            state: [
                { key: 'local-key1', value: 'local-value1' },
                { key: 'local-key2', value: 'local-value2' }
            ],
            totalEntryCount: 2
        },
        clusterState: {
            scope: 'CLUSTER',
            state: [{ key: 'cluster-key1', value: 'cluster-value1' }],
            totalEntryCount: 1
        },
        dropStateKeySupported: true
    };

    const mockComponentStateEntity: ComponentStateEntity = {
        componentState: mockComponentState
    };

    beforeEach(() => {
        const componentStateServiceSpy = {
            getComponentState: jest.fn(),
            clearComponentState: jest.fn(),
            clearComponentStateEntry: jest.fn()
        };
        const errorHelperSpy = {
            getErrorString: jest.fn()
        };

        TestBed.configureTestingModule({
            providers: [
                ComponentStateEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState
                    }
                }),
                { provide: ComponentStateService, useValue: componentStateServiceSpy },
                { provide: ErrorHelper, useValue: errorHelperSpy }
            ]
        });

        effects = TestBed.inject(ComponentStateEffects);
        store = TestBed.inject(MockStore);
        componentStateService = TestBed.inject(ComponentStateService) as jest.Mocked<ComponentStateService>;
        errorHelper = TestBed.inject(ErrorHelper) as jest.Mocked<ErrorHelper>;
    });

    describe('getComponentStateAndOpenDialog$', () => {
        it('should load component state successfully', (done) => {
            const request = {
                componentName: 'Test Component',
                componentType: ComponentType.Processor,
                componentId: 'test-id',
                canClear: true
            };

            componentStateService.getComponentState.mockReturnValue(of(mockComponentStateEntity));

            actions$ = of(ComponentStateActions.getComponentStateAndOpenDialog({ request }));

            effects.getComponentStateAndOpenDialog$.subscribe((action) => {
                expect(action).toEqual(
                    ComponentStateActions.loadComponentStateSuccess({
                        response: { componentState: mockComponentState }
                    })
                );
                expect(componentStateService.getComponentState).toHaveBeenCalledWith({
                    componentType: request.componentType,
                    componentId: request.componentId
                });
                done();
            });
        });

        it('should handle error when loading component state fails', (done) => {
            const request = {
                componentName: 'Test Component',
                componentType: ComponentType.Processor,
                componentId: 'test-id',
                canClear: true
            };

            const errorResponse = new HttpErrorResponse({
                error: 'Not Found',
                status: 404,
                statusText: 'Not Found'
            });

            errorHelper.getErrorString.mockReturnValue('Failed to get the component state for Test Component.');
            componentStateService.getComponentState.mockReturnValue(throwError(() => errorResponse));

            actions$ = of(ComponentStateActions.getComponentStateAndOpenDialog({ request }));

            effects.getComponentStateAndOpenDialog$.subscribe((action) => {
                expect(action).toEqual(
                    ErrorActions.snackBarError({
                        error: 'Failed to get the component state for Test Component.'
                    })
                );
                expect(errorHelper.getErrorString).toHaveBeenCalledWith(
                    errorResponse,
                    'Failed to get the component state for Test Component.'
                );
                done();
            });
        });
    });

    describe('loadComponentStateSuccess$', () => {
        it('should trigger open dialog action', (done) => {
            const response = { componentState: mockComponentState };

            actions$ = of(ComponentStateActions.loadComponentStateSuccess({ response }));

            effects.loadComponentStateSuccess$.subscribe((action) => {
                expect(action).toEqual(ComponentStateActions.openComponentStateDialog());
                done();
            });
        });
    });

    describe('clearComponentState$', () => {
        it('should clear component state successfully', (done) => {
            const componentType = ComponentType.Processor;
            const componentId = 'test-id';

            store.overrideSelector(selectComponentType, componentType);
            store.overrideSelector(selectComponentId, componentId);
            componentStateService.clearComponentState.mockReturnValue(of({}));

            actions$ = of(ComponentStateActions.clearComponentState());

            effects.clearComponentState$.subscribe((action) => {
                expect(action).toEqual(ComponentStateActions.reloadComponentState());
                expect(componentStateService.clearComponentState).toHaveBeenCalledWith({ componentType, componentId });
                done();
            });
        });

        it('should handle error when clearing component state fails', (done) => {
            const componentType = ComponentType.Processor;
            const componentId = 'test-id';
            const errorResponse = new HttpErrorResponse({
                error: 'Internal Server Error',
                status: 500,
                statusText: 'Internal Server Error'
            });

            store.overrideSelector(selectComponentType, componentType);
            store.overrideSelector(selectComponentId, componentId);
            errorHelper.getErrorString.mockReturnValue('Failed to clear the component state.');
            componentStateService.clearComponentState.mockReturnValue(throwError(() => errorResponse));

            actions$ = of(ComponentStateActions.clearComponentState());

            effects.clearComponentState$.subscribe((action) => {
                expect(action).toEqual(
                    ComponentStateActions.clearComponentStateFailure({
                        errorContext: {
                            errors: ['Failed to clear the component state.'],
                            context: ErrorContextKey.COMPONENT_STATE
                        }
                    })
                );
                expect(errorHelper.getErrorString).toHaveBeenCalledWith(
                    errorResponse,
                    'Failed to clear the component state.'
                );
                done();
            });
        });
    });

    describe('reloadComponentState$', () => {
        it('should reload component state successfully', (done) => {
            const componentType = ComponentType.Processor;
            const componentId = 'test-id';

            store.overrideSelector(selectComponentType, componentType);
            store.overrideSelector(selectComponentId, componentId);
            componentStateService.getComponentState.mockReturnValue(of(mockComponentStateEntity));

            actions$ = of(ComponentStateActions.reloadComponentState());

            effects.reloadComponentState$.subscribe((action) => {
                expect(action).toEqual(
                    ComponentStateActions.reloadComponentStateSuccess({
                        response: { componentState: mockComponentState }
                    })
                );
                expect(componentStateService.getComponentState).toHaveBeenCalledWith({ componentType, componentId });
                done();
            });
        });

        it('should handle error when reloading component state fails', (done) => {
            const componentType = ComponentType.Processor;
            const componentId = 'test-id';
            const errorResponse = new HttpErrorResponse({
                error: 'Service Unavailable',
                status: 503,
                statusText: 'Service Unavailable'
            });

            store.overrideSelector(selectComponentType, componentType);
            store.overrideSelector(selectComponentId, componentId);
            errorHelper.getErrorString.mockReturnValue('Failed to reload the component state.');
            componentStateService.getComponentState.mockReturnValue(throwError(() => errorResponse));

            actions$ = of(ComponentStateActions.reloadComponentState());

            effects.reloadComponentState$.subscribe((action) => {
                expect(action).toEqual(
                    ErrorActions.addBannerError({
                        errorContext: {
                            errors: ['Failed to reload the component state.'],
                            context: ErrorContextKey.COMPONENT_STATE
                        }
                    })
                );
                expect(errorHelper.getErrorString).toHaveBeenCalledWith(
                    errorResponse,
                    'Failed to reload the component state.'
                );
                done();
            });
        });
    });

    describe('clearComponentStateEntry$', () => {
        it('should clear local state entry successfully', (done) => {
            const componentType = ComponentType.Processor;
            const componentId = 'test-id';
            const request: ClearStateEntryRequest = {
                keyToDelete: 'local-key1',
                scope: 'LOCAL'
            };

            const updatedState: ComponentState = {
                ...mockComponentState,
                localState: {
                    ...mockComponentState.localState!,
                    state: mockComponentState.localState!.state.filter((entry) => entry.key !== 'local-key1')
                }
            };

            const expectedComponentStateEntity: ComponentStateEntity = {
                componentState: updatedState
            };

            store.overrideSelector(selectComponentType, componentType);
            store.overrideSelector(selectComponentId, componentId);
            store.overrideSelector(selectComponentState, mockComponentState);
            componentStateService.clearComponentStateEntry.mockReturnValue(of({}));

            actions$ = of(ComponentStateActions.clearComponentStateEntry({ request }));

            effects.clearComponentStateEntry$.subscribe((action) => {
                expect(action).toEqual(ComponentStateActions.reloadComponentState());
                expect(componentStateService.clearComponentStateEntry).toHaveBeenCalledWith(
                    componentType,
                    componentId,
                    expectedComponentStateEntity
                );
                done();
            });
        });

        it('should clear cluster state entry successfully', (done) => {
            const componentType = ComponentType.Processor;
            const componentId = 'test-id';
            const request: ClearStateEntryRequest = {
                keyToDelete: 'cluster-key1',
                scope: 'CLUSTER'
            };

            const updatedState: ComponentState = {
                ...mockComponentState,
                clusterState: {
                    ...mockComponentState.clusterState!,
                    state: mockComponentState.clusterState!.state.filter((entry) => entry.key !== 'cluster-key1')
                }
            };

            const expectedComponentStateEntity: ComponentStateEntity = {
                componentState: updatedState
            };

            store.overrideSelector(selectComponentType, componentType);
            store.overrideSelector(selectComponentId, componentId);
            store.overrideSelector(selectComponentState, mockComponentState);
            componentStateService.clearComponentStateEntry.mockReturnValue(of({}));

            actions$ = of(ComponentStateActions.clearComponentStateEntry({ request }));

            effects.clearComponentStateEntry$.subscribe((action) => {
                expect(action).toEqual(ComponentStateActions.reloadComponentState());
                expect(componentStateService.clearComponentStateEntry).toHaveBeenCalledWith(
                    componentType,
                    componentId,
                    expectedComponentStateEntity
                );
                done();
            });
        });

        it('should handle error when clearing state entry fails', (done) => {
            const componentType = ComponentType.Processor;
            const componentId = 'test-id';
            const request: ClearStateEntryRequest = {
                keyToDelete: 'local-key1',
                scope: 'LOCAL'
            };

            const errorResponse = new HttpErrorResponse({
                error: 'Bad Request',
                status: 400,
                statusText: 'Bad Request'
            });

            store.overrideSelector(selectComponentType, componentType);
            store.overrideSelector(selectComponentId, componentId);
            store.overrideSelector(selectComponentState, mockComponentState);
            errorHelper.getErrorString.mockReturnValue('Failed to clear state entry: local-key1.');
            componentStateService.clearComponentStateEntry.mockReturnValue(throwError(() => errorResponse));

            actions$ = of(ComponentStateActions.clearComponentStateEntry({ request }));

            effects.clearComponentStateEntry$.subscribe((action) => {
                expect(action).toEqual(
                    ComponentStateActions.clearComponentStateFailure({
                        errorContext: {
                            errors: ['Failed to clear state entry: local-key1.'],
                            context: ErrorContextKey.COMPONENT_STATE
                        }
                    })
                );
                expect(errorHelper.getErrorString).toHaveBeenCalledWith(
                    errorResponse,
                    'Failed to clear state entry: local-key1.'
                );
                done();
            });
        });

        it('should handle state without the specified scope', (done) => {
            const componentType = ComponentType.Processor;
            const componentId = 'test-id';
            const request: ClearStateEntryRequest = {
                keyToDelete: 'nonexistent-key',
                scope: 'LOCAL'
            };

            const stateWithoutLocal: ComponentState = {
                ...mockComponentState,
                localState: undefined
            };

            store.overrideSelector(selectComponentType, componentType);
            store.overrideSelector(selectComponentId, componentId);
            store.overrideSelector(selectComponentState, stateWithoutLocal);
            componentStateService.clearComponentStateEntry.mockReturnValue(of({}));

            actions$ = of(ComponentStateActions.clearComponentStateEntry({ request }));

            effects.clearComponentStateEntry$.subscribe((action) => {
                expect(action).toEqual(ComponentStateActions.reloadComponentState());
                expect(componentStateService.clearComponentStateEntry).toHaveBeenCalledWith(
                    componentType,
                    componentId,
                    {
                        componentState: stateWithoutLocal
                    }
                );
                done();
            });
        });
    });

    describe('clearComponentStateFailure$', () => {
        it('should convert failure to banner error', (done) => {
            const errorContext = {
                errors: ['Test error message'],
                context: ErrorContextKey.COMPONENT_STATE
            };

            actions$ = of(ComponentStateActions.clearComponentStateFailure({ errorContext }));

            effects.clearComponentStateFailure$.subscribe((action) => {
                expect(action).toEqual(ErrorActions.addBannerError({ errorContext }));
                done();
            });
        });
    });
});
