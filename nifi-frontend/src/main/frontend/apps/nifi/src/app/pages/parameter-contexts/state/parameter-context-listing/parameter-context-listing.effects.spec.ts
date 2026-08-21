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

import { EventEmitter } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { Action } from '@ngrx/store';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { provideMockActions } from '@ngrx/effects/testing';
import { ReplaySubject, Subject, of, throwError } from 'rxjs';
import { take } from 'rxjs/operators';
import type { Mock, Mocked } from 'vitest';

import { ParameterContextListingEffects } from './parameter-context-listing.effects';
import * as ParameterContextListingActions from './parameter-context-listing.actions';
import { ParameterContextService } from '../../service/parameter-contexts.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { Storage } from '@nifi/shared';
import { initialState } from './parameter-context-listing.reducer';
import { parameterContextsFeatureKey } from '../';
import { parameterContextListingFeatureKey } from './index';
import { ParameterContextUpdateRequest, ParameterContextUpdateRequestEntity } from '../../../../state/shared';
import { HttpErrorResponse } from '@angular/common/http';
import { EditParameterContext } from '../../../../ui/common/parameter-context/edit-parameter-context/edit-parameter-context.component';
import { SaveParameterContextChangesDialog } from '../../../../ui/common/parameter-context/save-parameter-context-changes-dialog/save-parameter-context-changes-dialog.component';
import { EditParameterContextUpdate } from '../../../../ui/common/parameter-context';

describe('ParameterContextListingEffects', () => {
    interface SetupOptions {
        updateRequest?: ParameterContextUpdateRequestEntity | null;
        updateRequestParameterContextId?: string | null;
        deleteUpdateRequestInitiated?: boolean;
        listStateOverride?: any;
        postUpdateNavigation?: string[] | null;
        postUpdateNavigationBoundary?: string[] | null;
        postUpdateNavigationState?: { highlightedParameterName?: string } | null;
        formDirty?: boolean;
        formValid?: boolean;
    }

    let action$: ReplaySubject<Action>;

    function createMockUpdateRequest(): ParameterContextUpdateRequest {
        return {
            requestId: 'test-request-id',
            uri: 'http://localhost:8080/test-uri',
            lastUpdated: '2023-01-01T00:00:00Z',
            complete: false,
            failureReason: undefined,
            percentComponent: 50,
            state: 'In Progress',
            updateSteps: [],
            parameterContext: {} as any,
            referencingComponents: []
        };
    }

    function createParameterContext(id = 'pc-source') {
        return {
            id,
            uri: `/parameter-contexts/${id}`,
            revision: { version: 1 },
            permissions: { canRead: true, canWrite: true },
            component: {
                id,
                name: 'Source Context',
                description: '',
                parameters: [],
                boundProcessGroups: [],
                inheritedParameterContexts: []
            }
        };
    }

    async function setup({
        updateRequest = null,
        updateRequestParameterContextId = null,
        deleteUpdateRequestInitiated = false,
        listStateOverride,
        postUpdateNavigation = null,
        postUpdateNavigationBoundary = null,
        postUpdateNavigationState = null,
        formDirty = false,
        formValid = true
    }: SetupOptions = {}) {
        const editParameterContext = new EventEmitter<EditParameterContextUpdate>();
        const continuePostUpdateNavigation = new EventEmitter<void>();
        const cancelUpdateRequest = new EventEmitter<void>();
        const editDialogAfterClosed$ = new Subject<string | undefined>();
        const saveChangesAfterClosed$ = new Subject<void>();
        const save = new EventEmitter<void>();
        const discard = new EventEmitter<void>();
        const submitForm = vi.fn();

        const editParameterContextForm = new FormGroup({
            name: new FormControl(formValid ? 'Source Context' : '', Validators.required),
            description: new FormControl(''),
            parameters: new FormControl([]),
            inheritedParameterContexts: new FormControl([])
        });
        if (formDirty) {
            editParameterContextForm.markAsDirty();
        }

        const editDialogRef = {
            componentInstance: {
                updateRequest: undefined as unknown,
                availableParameterContexts$: undefined as unknown,
                saving$: undefined as unknown,
                hasPendingPostUpdateNavigation$: undefined as unknown,
                goToParameter: undefined as unknown,
                createNewParameter: undefined as unknown,
                editParameter: undefined as unknown,
                editParameterContext,
                continuePostUpdateNavigation,
                cancelUpdateRequest,
                editParameterContextForm,
                submitForm
            },
            afterClosed: () => editDialogAfterClosed$.asObservable()
        };

        const saveChangesDialogRef = {
            componentInstance: { save, discard },
            afterClosed: () => saveChangesAfterClosed$.asObservable()
        };

        const dialogOpen = vi.fn((component: unknown) => {
            if (component === SaveParameterContextChangesDialog) {
                return saveChangesDialogRef;
            }
            return editDialogRef;
        });

        await TestBed.configureTestingModule({
            providers: [
                ParameterContextListingEffects,
                provideMockActions(() => action$),
                provideMockStore({
                    initialState: {
                        [parameterContextsFeatureKey]: {
                            [parameterContextListingFeatureKey]: {
                                ...initialState,
                                ...listStateOverride,
                                updateRequestEntity: updateRequest,
                                updateRequestParameterContextId,
                                deleteUpdateRequestInitiated,
                                postUpdateNavigation,
                                postUpdateNavigationBoundary,
                                postUpdateNavigationState
                            }
                        }
                    }
                }),
                {
                    provide: ParameterContextService,
                    useValue: {
                        deleteParameterContextUpdate: vi.fn(),
                        pollParameterContextUpdate: vi.fn(),
                        getParameterContexts: vi.fn()
                    }
                },
                { provide: MatDialog, useValue: { open: dialogOpen } },
                { provide: Router, useValue: { navigate: vi.fn(() => Promise.resolve(true)) } },
                {
                    provide: ErrorHelper,
                    useValue: { getErrorString: vi.fn(), handleLoadingError: vi.fn(), fullScreenError: vi.fn() }
                },
                { provide: Storage, useValue: { setItem: vi.fn() } }
            ]
        }).compileComponents();

        const effects = TestBed.inject(ParameterContextListingEffects);
        const parameterContextService = TestBed.inject(ParameterContextService) as Mocked<ParameterContextService>;
        const dialog = TestBed.inject(MatDialog) as Mocked<MatDialog>;
        const router = TestBed.inject(Router) as Mocked<Router>;
        const store = TestBed.inject(MockStore);
        const dispatchSpy = vi.spyOn(store, 'dispatch');
        action$ = new ReplaySubject<Action>();

        const errorHelper = TestBed.inject(ErrorHelper);
        return {
            effects,
            parameterContextService,
            errorHelper,
            dialog,
            dialogOpen,
            router,
            store,
            dispatchSpy,
            editDialogRef,
            saveChangesDialogRef,
            save,
            discard,
            editParameterContext,
            continuePostUpdateNavigation,
            submitForm,
            editDialogAfterClosed$,
            saveChangesAfterClosed$
        };
    }

    async function openEditDialog(
        effects: ParameterContextListingEffects,
        parameterContext = createParameterContext()
    ) {
        const subscription = effects.openParameterContextDialog$.subscribe();
        action$.next(
            ParameterContextListingActions.openParameterContextDialog({
                request: { parameterContext }
            })
        );
        await Promise.resolve();
        return subscription;
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    afterEach(() => {
        if (action$) {
            action$.complete();
        }
        TestBed.resetTestingModule();
    });

    it('should create', async () => {
        const { effects } = await setup();
        expect(effects).toBeTruthy();
    });

    describe('loadParameterContexts$', () => {
        it('loads successfully', async () => {
            const { effects, parameterContextService } = await setup();

            action$.next(ParameterContextListingActions.loadParameterContexts());

            (parameterContextService.getParameterContexts as Mock).mockReturnValueOnce(
                of({ parameterContexts: [], currentTime: 't' })
            );

            const result = await new Promise((resolve) =>
                effects.loadParameterContexts$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ParameterContextListingActions.loadParameterContextsSuccess({
                    response: { parameterContexts: [], loadedTimestamp: 't' }
                })
            );
        });

        it('errors on initial load (hasExistingData=false)', async () => {
            const { effects, parameterContextService } = await setup();

            action$.next(ParameterContextListingActions.loadParameterContexts());

            const error = new HttpErrorResponse({ status: 500 });
            (parameterContextService.getParameterContexts as Mock).mockImplementationOnce(() =>
                throwError(() => error)
            );

            const result = await new Promise((resolve) =>
                effects.loadParameterContexts$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ParameterContextListingActions.loadParameterContextsError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
        });

        it('errors on refresh (hasExistingData=true)', async () => {
            const stateWithData = { loadedTimestamp: 'prev' };
            const { effects, parameterContextService } = await setup({ listStateOverride: stateWithData });

            action$.next(ParameterContextListingActions.loadParameterContexts());

            const error = new HttpErrorResponse({ status: 500 });
            (parameterContextService.getParameterContexts as Mock).mockImplementationOnce(() =>
                throwError(() => error)
            );

            const result = await new Promise((resolve) =>
                effects.loadParameterContexts$.pipe(take(1)).subscribe(resolve)
            );

            expect(result).toEqual(
                ParameterContextListingActions.loadParameterContextsError({
                    errorResponse: error,
                    loadedTimestamp: stateWithData.loadedTimestamp,
                    status: 'success'
                })
            );
        });
    });

    describe('loadParameterContextsError$', () => {
        it('should handle parameter contexts error for initial load', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ParameterContextListingActions.parameterContextListingBannerApiError({
                error: 'Error message'
            });
            action$.next(
                ParameterContextListingActions.loadParameterContextsError({
                    errorResponse: error,
                    loadedTimestamp: initialState.loadedTimestamp,
                    status: 'pending'
                })
            );
            vi.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadParameterContextsError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(false, error);
            expect(result).toEqual(errorAction);
        });

        it('should handle parameter contexts error for refresh', async () => {
            const { effects, errorHelper } = await setup();

            const error = new HttpErrorResponse({ status: 500 });
            const errorAction = ParameterContextListingActions.parameterContextListingBannerApiError({
                error: 'Error message'
            });
            action$.next(
                ParameterContextListingActions.loadParameterContextsError({
                    errorResponse: error,
                    loadedTimestamp: 'prev',
                    status: 'success'
                })
            );
            vi.spyOn(errorHelper, 'handleLoadingError').mockReturnValueOnce(errorAction);

            const result = await new Promise((resolve) =>
                effects.loadParameterContextsError$.pipe(take(1)).subscribe(resolve)
            );

            expect(errorHelper.handleLoadingError).toHaveBeenCalledWith(true, error);
            expect(result).toEqual(errorAction);
        });
    });

    describe('stopPollingParameterContextUpdateRequest$', () => {
        it('should dispatch deleteParameterContextUpdateRequest when triggered', async () => {
            const { effects } = await setup();

            action$.next(ParameterContextListingActions.stopPollingParameterContextUpdateRequest());

            effects.stopPollingParameterContextUpdateRequest$.subscribe((action) => {
                expect(action).toEqual(ParameterContextListingActions.deleteParameterContextUpdateRequest());
            });
        });
    });

    describe('deleteParameterContextUpdateRequest$', () => {
        it('should call service when deleteUpdateRequestInitiated is false', async () => {
            const mockUpdateRequest = createMockUpdateRequest();
            const mockResponse = { request: mockUpdateRequest };
            const parameterContextId = 'test-parameter-context-id';

            const { effects, parameterContextService } = await setup({
                updateRequest: { request: mockUpdateRequest, parameterContextRevision: { version: 1 } },
                updateRequestParameterContextId: parameterContextId,
                deleteUpdateRequestInitiated: false
            });

            parameterContextService.deleteParameterContextUpdate.mockReturnValue(of(mockResponse));

            action$.next(ParameterContextListingActions.deleteParameterContextUpdateRequest());

            effects.deleteParameterContextUpdateRequest$.subscribe(() => {
                expect(parameterContextService.deleteParameterContextUpdate).toHaveBeenCalledWith(
                    parameterContextId,
                    mockUpdateRequest.requestId
                );
            });
        });

        it('should call service when deleteUpdateRequestInitiated is true', async () => {
            const mockUpdateRequest = createMockUpdateRequest();
            const mockResponse = { request: mockUpdateRequest };
            const parameterContextId = 'test-parameter-context-id';

            const { effects, parameterContextService } = await setup({
                updateRequest: { request: mockUpdateRequest, parameterContextRevision: { version: 1 } },
                updateRequestParameterContextId: parameterContextId,
                deleteUpdateRequestInitiated: true
            });

            parameterContextService.deleteParameterContextUpdate.mockReturnValue(of(mockResponse));

            action$.next(ParameterContextListingActions.deleteParameterContextUpdateRequest());

            effects.deleteParameterContextUpdateRequest$.subscribe(() => {
                expect(parameterContextService.deleteParameterContextUpdate).toHaveBeenCalledWith(
                    parameterContextId,
                    mockUpdateRequest.requestId
                );
            });
        });
    });

    describe('pollParameterContextUpdateRequestSuccess$', () => {
        it('should dispatch stopPolling when request is complete', async () => {
            const completeUpdateRequest = createMockUpdateRequest();
            completeUpdateRequest.complete = true;

            const response = {
                requestEntity: {
                    request: completeUpdateRequest,
                    parameterContextRevision: { version: 1 }
                }
            };

            const { effects } = await setup();

            action$.next(ParameterContextListingActions.pollParameterContextUpdateRequestSuccess({ response }));

            effects.pollParameterContextUpdateRequestSuccess$.subscribe((action) => {
                expect(action).toEqual(ParameterContextListingActions.stopPollingParameterContextUpdateRequest());
            });
        });

        it('should not dispatch when request is incomplete', async () => {
            const incompleteUpdateRequest = createMockUpdateRequest();
            incompleteUpdateRequest.complete = false;

            const response = {
                requestEntity: {
                    request: incompleteUpdateRequest,
                    parameterContextRevision: { version: 1 }
                }
            };

            const { effects } = await setup();

            const emissions: any[] = [];
            effects.pollParameterContextUpdateRequestSuccess$.subscribe((action) => {
                emissions.push(action);
            });

            action$.next(ParameterContextListingActions.pollParameterContextUpdateRequestSuccess({ response }));

            expect(emissions).toEqual([]);
        });
    });

    describe('openParameterContextDialog$ goToParameter', () => {
        it('should navigate immediately when the form is clean', async () => {
            const { effects, dialogOpen, router, editDialogRef } = await setup({ formDirty: false });
            const subscription = await openEditDialog(effects);

            expect(dialogOpen).toHaveBeenCalledWith(EditParameterContext, expect.anything());

            (editDialogRef.componentInstance.goToParameter as (id: string, name: string) => void)(
                'pc-inherited',
                'inherited-param'
            );

            expect(router.navigate).toHaveBeenCalledWith(['/parameter-contexts', 'pc-inherited', 'edit'], {
                state: {
                    backNavigation: {
                        route: ['/parameter-contexts', 'pc-source', 'edit'],
                        routeBoundary: ['/parameter-contexts'],
                        context: 'Parameter Context'
                    },
                    highlightedParameterName: 'inherited-param'
                }
            });

            subscription.unsubscribe();
        });

        it('should open the save-changes dialog with canSave true when the form is dirty and valid', async () => {
            const { effects, dialogOpen, editDialogRef } = await setup({ formDirty: true, formValid: true });
            const subscription = await openEditDialog(effects);

            (editDialogRef.componentInstance.goToParameter as (id: string, name: string) => void)(
                'pc-inherited',
                'inherited-param'
            );

            expect(dialogOpen).toHaveBeenCalledWith(
                SaveParameterContextChangesDialog,
                expect.objectContaining({
                    data: {
                        destination: 'Parameter',
                        canSave: true
                    }
                })
            );
            subscription.unsubscribe();
        });

        it('should open the save-changes dialog with canSave false when the form is dirty and invalid', async () => {
            const { effects, dialogOpen, editDialogRef } = await setup({ formDirty: true, formValid: false });
            const subscription = await openEditDialog(effects);

            (editDialogRef.componentInstance.goToParameter as (id: string, name: string) => void)(
                'pc-inherited',
                'inherited-param'
            );

            expect(dialogOpen).toHaveBeenCalledWith(
                SaveParameterContextChangesDialog,
                expect.objectContaining({
                    data: {
                        destination: 'Parameter',
                        canSave: false
                    }
                })
            );
            subscription.unsubscribe();
        });

        it('should submitForm with postUpdateNavigation when dirty and Save is chosen', async () => {
            const { effects, editDialogRef, submitForm, save, router } = await setup({
                formDirty: true,
                formValid: true
            });
            const subscription = await openEditDialog(effects);

            (editDialogRef.componentInstance.goToParameter as (id: string, name: string) => void)(
                'pc-inherited',
                'inherited-param'
            );
            save.next();

            expect(submitForm).toHaveBeenCalledWith(
                ['/parameter-contexts', 'pc-inherited', 'edit'],
                ['/parameter-contexts'],
                { highlightedParameterName: 'inherited-param' }
            );
            expect(router.navigate).not.toHaveBeenCalled();
            subscription.unsubscribe();
        });

        it('should not submitForm when dirty, invalid, and Save is emitted', async () => {
            const { effects, editDialogRef, submitForm, save, router } = await setup({
                formDirty: true,
                formValid: false
            });
            const subscription = await openEditDialog(effects);

            (editDialogRef.componentInstance.goToParameter as (id: string, name: string) => void)(
                'pc-inherited',
                'inherited-param'
            );
            save.next();

            expect(submitForm).not.toHaveBeenCalled();
            expect(router.navigate).not.toHaveBeenCalled();
            subscription.unsubscribe();
        });

        it("should navigate without submit when dirty and Don't Save is chosen", async () => {
            const { effects, editDialogRef, submitForm, discard, router } = await setup({
                formDirty: true,
                formValid: true
            });
            const subscription = await openEditDialog(effects);

            (editDialogRef.componentInstance.goToParameter as (id: string, name: string) => void)(
                'pc-inherited',
                'inherited-param'
            );
            discard.next();

            expect(submitForm).not.toHaveBeenCalled();
            expect(router.navigate).toHaveBeenCalledWith(['/parameter-contexts', 'pc-inherited', 'edit'], {
                state: {
                    backNavigation: {
                        route: ['/parameter-contexts', 'pc-source', 'edit'],
                        routeBoundary: ['/parameter-contexts'],
                        context: 'Parameter Context'
                    },
                    highlightedParameterName: 'inherited-param'
                }
            });
            subscription.unsubscribe();
        });

        it("should navigate without submit when dirty, invalid, and Don't Save is chosen", async () => {
            const { effects, editDialogRef, submitForm, discard, router } = await setup({
                formDirty: true,
                formValid: false
            });
            const subscription = await openEditDialog(effects);

            (editDialogRef.componentInstance.goToParameter as (id: string, name: string) => void)(
                'pc-inherited',
                'inherited-param'
            );
            discard.next();

            expect(submitForm).not.toHaveBeenCalled();
            expect(router.navigate).toHaveBeenCalledWith(['/parameter-contexts', 'pc-inherited', 'edit'], {
                state: {
                    backNavigation: {
                        route: ['/parameter-contexts', 'pc-source', 'edit'],
                        routeBoundary: ['/parameter-contexts'],
                        context: 'Parameter Context'
                    },
                    highlightedParameterName: 'inherited-param'
                }
            });
            subscription.unsubscribe();
        });

        it('should neither submit nor navigate when dirty and the save-changes dialog is closed without an action', async () => {
            const { effects, editDialogRef, submitForm, router, saveChangesAfterClosed$ } = await setup({
                formDirty: true,
                formValid: true
            });
            const subscription = await openEditDialog(effects);

            (editDialogRef.componentInstance.goToParameter as (id: string, name: string) => void)(
                'pc-inherited',
                'inherited-param'
            );
            saveChangesAfterClosed$.next();

            expect(submitForm).not.toHaveBeenCalled();
            expect(router.navigate).not.toHaveBeenCalled();
            subscription.unsubscribe();
        });

        it('should unwrap editParameterContext emit into submitParameterContextUpdateRequest', async () => {
            const { effects, editDialogRef, dispatchSpy } = await setup();
            const subscription = await openEditDialog(effects);

            const update: EditParameterContextUpdate = {
                payload: { id: 'pc-source', component: { id: 'pc-source' } },
                postUpdateNavigation: ['/parameter-contexts', 'pc-inherited', 'edit'],
                postUpdateNavigationBoundary: ['/parameter-contexts'],
                postUpdateNavigationState: { highlightedParameterName: 'inherited-param' }
            };
            editDialogRef.componentInstance.editParameterContext.next(update);

            expect(dispatchSpy).toHaveBeenCalledWith(
                ParameterContextListingActions.submitParameterContextUpdateRequest({
                    request: {
                        id: 'pc-source',
                        payload: update.payload,
                        postUpdateNavigation: update.postUpdateNavigation,
                        postUpdateNavigationBoundary: update.postUpdateNavigationBoundary,
                        postUpdateNavigationState: update.postUpdateNavigationState
                    }
                })
            );
            subscription.unsubscribe();
        });

        it('should navigate when continuePostUpdateNavigation is emitted with pending navigation', async () => {
            const { effects, continuePostUpdateNavigation, router } = await setup({
                postUpdateNavigation: ['/parameter-contexts', 'pc-inherited', 'edit'],
                postUpdateNavigationBoundary: ['/parameter-contexts'],
                postUpdateNavigationState: { highlightedParameterName: 'inherited-param' }
            });
            const subscription = await openEditDialog(effects);

            continuePostUpdateNavigation.next();
            await Promise.resolve();

            expect(router.navigate).toHaveBeenCalledWith(['/parameter-contexts', 'pc-inherited', 'edit'], {
                state: {
                    backNavigation: {
                        route: ['/parameter-contexts', 'pc-source', 'edit'],
                        routeBoundary: ['/parameter-contexts'],
                        context: 'Parameter Context'
                    },
                    highlightedParameterName: 'inherited-param'
                }
            });
            subscription.unsubscribe();
        });

        it('should not navigate when continuePostUpdateNavigation is emitted without pending navigation', async () => {
            const { effects, continuePostUpdateNavigation, router } = await setup({
                postUpdateNavigation: null
            });
            const subscription = await openEditDialog(effects);

            continuePostUpdateNavigation.next();
            await Promise.resolve();

            expect(router.navigate).not.toHaveBeenCalled();
            subscription.unsubscribe();
        });
    });
});
