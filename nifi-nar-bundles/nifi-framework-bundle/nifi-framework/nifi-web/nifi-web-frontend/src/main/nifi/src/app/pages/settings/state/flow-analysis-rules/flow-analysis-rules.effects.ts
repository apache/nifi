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
import { catchError, from, map, NEVER, Observable, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectFlowAnalysisRuleTypes } from '../../../../state/extension-types/extension-types.selectors';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { FlowAnalysisRuleService } from '../../service/flow-analysis-rule.service';
import { Client } from '../../../../service/client.service';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { CreateFlowAnalysisRule } from '../../ui/flow-analysis-rules/create-flow-analysis-rule/create-flow-analysis-rule.component';
import { Router } from '@angular/router';
import { selectSaving } from '../management-controller-services/management-controller-services.selectors';
import {
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    NewPropertyDialogRequest,
    NewPropertyDialogResponse,
    Property,
    PropertyDescriptor,
    UpdateControllerServiceRequest
} from '../../../../state/shared';
import { EditFlowAnalysisRule } from '../../ui/flow-analysis-rules/edit-flow-analysis-rule/edit-flow-analysis-rule.component';
import { CreateFlowAnalysisRuleSuccess } from './index';
import { NewPropertyDialog } from '../../../../ui/common/new-property-dialog/new-property-dialog.component';
import { CreateControllerService } from '../../../../ui/common/controller-service/create-controller-service/create-controller-service.component';
import { ExtensionTypesService } from '../../../../service/extension-types.service';

@Injectable()
export class FlowAnalysisRulesEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private managementControllerServiceService: ManagementControllerServiceService,
        private extensionTypesService: ExtensionTypesService,
        private flowAnalysisRuleService: FlowAnalysisRuleService,
        private dialog: MatDialog,
        private router: Router
    ) {}

    loadFlowAnalysisRule$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowAnalysisRuleActions.loadFlowAnalysisRules),
            switchMap(() =>
                from(this.flowAnalysisRuleService.getFlowAnalysisRule()).pipe(
                    map((response) =>
                        FlowAnalysisRuleActions.loadFlowAnalysisRulesSuccess({
                            response: {
                                flowAnalysisRules: response.flowAnalysisRules,
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            FlowAnalysisRuleActions.flowAnalysisRuleApiError({
                                error: error.error
                            })
                        )
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
                tap(([action, flowAnalysisRuleTypes]) => {
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
                    catchError((error) =>
                        of(
                            FlowAnalysisRuleActions.flowAnalysisRuleApiError({
                                error: error.error
                            })
                        )
                    )
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
                    catchError((error) =>
                        of(
                            FlowAnalysisRuleActions.flowAnalysisRuleApiError({
                                error: error.error
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

                    editDialogReference.componentInstance.createNewProperty = (
                        existingProperties: string[],
                        allowsSensitive: boolean
                    ): Observable<Property> => {
                        const dialogRequest: NewPropertyDialogRequest = { existingProperties, allowsSensitive };
                        const newPropertyDialogReference = this.dialog.open(NewPropertyDialog, {
                            data: dialogRequest,
                            panelClass: 'small-dialog'
                        });

                        return newPropertyDialogReference.componentInstance.newProperty.pipe(
                            take(1),
                            switchMap((dialogResponse: NewPropertyDialogResponse) => {
                                return this.flowAnalysisRuleService
                                    .getPropertyDescriptor(request.id, dialogResponse.name, dialogResponse.sensitive)
                                    .pipe(
                                        take(1),
                                        map((response) => {
                                            newPropertyDialogReference.close();

                                            return {
                                                property: dialogResponse.name,
                                                value: null,
                                                descriptor: response.propertyDescriptor
                                            };
                                        })
                                    );
                            })
                        );
                    };

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

                    editDialogReference.componentInstance.createNewService = (
                        request: InlineServiceCreationRequest
                    ): Observable<InlineServiceCreationResponse> => {
                        const descriptor: PropertyDescriptor = request.descriptor;

                        // fetch all services that implement the requested service api
                        return this.extensionTypesService
                            .getImplementingControllerServiceTypes(
                                // @ts-ignore
                                descriptor.identifiesControllerService,
                                descriptor.identifiesControllerServiceBundle
                            )
                            .pipe(
                                take(1),
                                switchMap((implementingTypesResponse) => {
                                    // show the create controller service dialog with the types that implemented the interface
                                    const createServiceDialogReference = this.dialog.open(CreateControllerService, {
                                        data: {
                                            controllerServiceTypes: implementingTypesResponse.controllerServiceTypes
                                        },
                                        panelClass: 'medium-dialog'
                                    });

                                    return createServiceDialogReference.componentInstance.createControllerService.pipe(
                                        take(1),
                                        switchMap((controllerServiceType) => {
                                            // typically this sequence would be implemented with ngrx actions, however we are
                                            // currently in an edit session and we need to return both the value (new service id)
                                            // and updated property descriptor so the table renders correctly
                                            return this.managementControllerServiceService
                                                .createControllerService({
                                                    revision: {
                                                        clientId: this.client.getClientId(),
                                                        version: 0
                                                    },
                                                    controllerServiceType: controllerServiceType.type,
                                                    controllerServiceBundle: controllerServiceType.bundle
                                                })
                                                .pipe(
                                                    take(1),
                                                    switchMap((createResponse) => {
                                                        // fetch an updated property descriptor
                                                        return this.flowAnalysisRuleService
                                                            .getPropertyDescriptor(ruleId, descriptor.name, false)
                                                            .pipe(
                                                                take(1),
                                                                map((descriptorResponse) => {
                                                                    createServiceDialogReference.close();

                                                                    return {
                                                                        value: createResponse.id,
                                                                        descriptor:
                                                                            descriptorResponse.propertyDescriptor
                                                                    };
                                                                })
                                                            );
                                                    }),
                                                    catchError((error) => {
                                                        // TODO - show error
                                                        return NEVER;
                                                    })
                                                );
                                        })
                                    );
                                })
                            );
                    };

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
                    catchError((error) =>
                        of(
                            FlowAnalysisRuleActions.flowAnalysisRuleApiError({
                                error: error.error
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
                    catchError((error) =>
                        of(
                            FlowAnalysisRuleActions.flowAnalysisRuleApiError({
                                error: error.error
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
                    catchError((error) =>
                        of(
                            FlowAnalysisRuleActions.flowAnalysisRuleApiError({
                                error: error.error
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
