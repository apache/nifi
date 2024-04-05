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

import { Injectable } from '@angular/core';
import { Actions, concatLatestFrom, createEffect, ofType } from '@ngrx/effects';
import * as FlowAnalysisRuleActions from './flow-analysis-rules.actions';
import { catchError, from, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectFlowAnalysisRuleTypes } from '../../../../state/extension-types/extension-types.selectors';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { FlowAnalysisRuleService } from '../../service/flow-analysis-rule.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { CreateFlowAnalysisRule } from '../../ui/flow-analysis-rules/create-flow-analysis-rule/create-flow-analysis-rule.component';
import { Router } from '@angular/router';
import { selectSaving } from '../management-controller-services/management-controller-services.selectors';
import { UpdateControllerServiceRequest } from '../../../../state/shared';
import { EditFlowAnalysisRule } from '../../ui/flow-analysis-rules/edit-flow-analysis-rule/edit-flow-analysis-rule.component';
import { CreateFlowAnalysisRuleSuccess } from './index';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { selectStatus } from './flow-analysis-rules.selectors';
import { HttpErrorResponse } from '@angular/common/http';

@Injectable()
export class FlowAnalysisRulesEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private managementControllerServiceService: ManagementControllerServiceService,
        private flowAnalysisRuleService: FlowAnalysisRuleService,
        private errorHelper: ErrorHelper,
        private dialog: MatDialog,
        private router: Router,
        private propertyTableHelperService: PropertyTableHelperService
    ) {}

    loadFlowAnalysisRule$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisRuleActions.loadFlowAnalysisRules),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([, status]) =>
                from(this.flowAnalysisRuleService.getFlowAnalysisRule()).pipe(
                    map((response) =>
                        FlowAnalysisRuleActions.loadFlowAnalysisRulesSuccess({
                            response: {
                                flowAnalysisRules: response.flowAnalysisRules,
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.handleLoadingError(status, errorResponse))
                    )
                )
            )
        )
    );

    openNewFlowAnalysisRuleDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowAnalysisRuleActions.openNewFlowAnalysisRuleDialog),
                concatLatestFrom(() => this.store.select(selectFlowAnalysisRuleTypes)),
                tap(([, flowAnalysisRuleTypes]) => {
                    this.dialog.open(CreateFlowAnalysisRule, {
                        data: {
                            flowAnalysisRuleTypes
                        },
                        panelClass: 'medium-dialog'
                    });
                })
            ),
        { dispatch: false }
    );

    createFlowAnalysisRule$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisRuleActions.createFlowAnalysisRule),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowAnalysisRuleService.createFlowAnalysisRule(request)).pipe(
                    map((response) =>
                        FlowAnalysisRuleActions.createFlowAnalysisRuleSuccess({
                            response: {
                                flowAnalysisRule: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(
                            FlowAnalysisRuleActions.flowAnalysisRuleSnackbarApiError({ error: errorResponse.error })
                        );
                    })
                )
            )
        )
    );

    createFlowAnalysisRuleSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisRuleActions.createFlowAnalysisRuleSuccess),
            map((action) => action.response),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap((response: CreateFlowAnalysisRuleSuccess) =>
                of(
                    FlowAnalysisRuleActions.selectFlowAnalysisRule({
                        request: {
                            id: response.flowAnalysisRule.id
                        }
                    })
                )
            )
        )
    );

    flowAnalysisRuleBannerApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisRuleActions.flowAnalysisRuleBannerApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.addBannerError({ error })))
        )
    );

    flowAnalysisRuleSnackbarApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisRuleActions.flowAnalysisRuleSnackbarApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );

    promptFlowAnalysisRuleDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowAnalysisRuleActions.promptFlowAnalysisRuleDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Delete Flow Analysis Rule',
                            message: `Delete reporting task ${request.flowAnalysisRule.component.name}?`
                        },
                        panelClass: 'small-dialog'
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            FlowAnalysisRuleActions.deleteFlowAnalysisRule({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteFlowAnalysisRule$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisRuleActions.deleteFlowAnalysisRule),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowAnalysisRuleService.deleteFlowAnalysisRule(request)).pipe(
                    map((response) =>
                        FlowAnalysisRuleActions.deleteFlowAnalysisRuleSuccess({
                            response: {
                                flowAnalysisRule: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            FlowAnalysisRuleActions.flowAnalysisRuleSnackbarApiError({
                                error: errorResponse.error
                            })
                        )
                    )
                )
            )
        )
    );

    navigateToEditFlowAnalysisRule$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowAnalysisRuleActions.navigateToEditFlowAnalysisRule),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/settings', 'flow-analysis-rules', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    openConfigureFlowAnalysisRuleDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowAnalysisRuleActions.openConfigureFlowAnalysisRuleDialog),
                map((action) => action.request),
                tap((request) => {
                    const ruleId: string = request.id;

                    const editDialogReference = this.dialog.open(EditFlowAnalysisRule, {
                        data: {
                            flowAnalysisRule: request.flowAnalysisRule
                        },
                        id: ruleId,
                        panelClass: 'large-dialog'
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(request.id, this.flowAnalysisRuleService);

                    const goTo = (commands: string[], destination: string): void => {
                        if (editDialogReference.componentInstance.editFlowAnalysisRuleForm.dirty) {
                            const saveChangesDialogReference = this.dialog.open(YesNoDialog, {
                                data: {
                                    title: 'Flow Analysis Rule Configuration',
                                    message: `Save changes before going to this ${destination}?`
                                },
                                panelClass: 'small-dialog'
                            });

                            saveChangesDialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                                editDialogReference.componentInstance.submitForm(commands);
                            });

                            saveChangesDialogReference.componentInstance.no.pipe(take(1)).subscribe(() => {
                                editDialogReference.close('ROUTED');
                                this.router.navigate(commands);
                            });
                        } else {
                            editDialogReference.close('ROUTED');
                            this.router.navigate(commands);
                        }
                    };

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        const commands: string[] = ['/settings', 'management-controller-services', serviceId];
                        goTo(commands, 'Controller Service');
                    };

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            request.id,
                            this.managementControllerServiceService,
                            this.flowAnalysisRuleService
                        );

                    editDialogReference.componentInstance.editFlowAnalysisRule
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((updateControllerServiceRequest: UpdateControllerServiceRequest) => {
                            this.store.dispatch(
                                FlowAnalysisRuleActions.configureFlowAnalysisRule({
                                    request: {
                                        id: request.flowAnalysisRule.id,
                                        uri: request.flowAnalysisRule.uri,
                                        payload: updateControllerServiceRequest.payload,
                                        postUpdateNavigation: updateControllerServiceRequest.postUpdateNavigation
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        this.store.dispatch(ErrorActions.clearBannerErrors());

                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                FlowAnalysisRuleActions.selectFlowAnalysisRule({
                                    request: {
                                        id: ruleId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    configureFlowAnalysisRule$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisRuleActions.configureFlowAnalysisRule),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowAnalysisRuleService.updateFlowAnalysisRule(request)).pipe(
                    map((response) =>
                        FlowAnalysisRuleActions.configureFlowAnalysisRuleSuccess({
                            response: {
                                id: request.id,
                                flowAnalysisRule: response,
                                postUpdateNavigation: request.postUpdateNavigation
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            FlowAnalysisRuleActions.flowAnalysisRuleBannerApiError({
                                error: errorResponse.error
                            })
                        )
                    )
                )
            )
        )
    );

    configureFlowAnalysisRuleSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowAnalysisRuleActions.configureFlowAnalysisRuleSuccess),
                map((action) => action.response),
                tap((response) => {
                    if (response.postUpdateNavigation) {
                        this.router.navigate(response.postUpdateNavigation);
                        this.dialog.getDialogById(response.id)?.close('ROUTED');
                    } else {
                        this.dialog.closeAll();
                    }
                })
            ),
        { dispatch: false }
    );

    selectFlowAnalysisRule$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowAnalysisRuleActions.selectFlowAnalysisRule),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/settings', 'flow-analysis-rules', request.id]);
                })
            ),
        { dispatch: false }
    );

    enableFlowAnalysisRule$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisRuleActions.enableFlowAnalysisRule),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowAnalysisRuleService.setEnable(request, true)).pipe(
                    map((response) =>
                        FlowAnalysisRuleActions.enableFlowAnalysisRuleSuccess({
                            response: {
                                id: request.id,
                                flowAnalysisRule: response,
                                postUpdateNavigation: response.postUpdateNavigation
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            FlowAnalysisRuleActions.flowAnalysisRuleSnackbarApiError({
                                error: errorResponse.error
                            })
                        )
                    )
                )
            )
        )
    );

    enableFlowAnalysisRuleSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowAnalysisRuleActions.enableFlowAnalysisRuleSuccess),
                map((action) => action.response),
                tap((response) => {
                    if (response.postUpdateNavigation) {
                        this.router.navigate(response.postUpdateNavigation);
                    }
                })
            ),
        { dispatch: false }
    );

    disableFlowAnalysisRule$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisRuleActions.disableFlowAnalysisRule),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowAnalysisRuleService.setEnable(request, false)).pipe(
                    map((response) =>
                        FlowAnalysisRuleActions.disableFlowAnalysisRuleSuccess({
                            response: {
                                id: request.id,
                                flowAnalysisRule: response,
                                postUpdateNavigation: response.postUpdateNavigation
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            FlowAnalysisRuleActions.flowAnalysisRuleSnackbarApiError({
                                error: errorResponse.error
                            })
                        )
                    )
                )
            )
        )
    );

    disableFlowAnalysisRuleSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowAnalysisRuleActions.disableFlowAnalysisRuleSuccess),
                map((action) => action.response),
                tap((response) => {
                    if (response.postUpdateNavigation) {
                        this.router.navigate(response.postUpdateNavigation);
                    }
                })
            ),
        { dispatch: false }
    );
}
