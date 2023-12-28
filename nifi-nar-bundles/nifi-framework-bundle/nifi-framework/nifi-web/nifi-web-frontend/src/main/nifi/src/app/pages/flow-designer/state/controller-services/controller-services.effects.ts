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
import { Actions, createEffect, ofType } from '@ngrx/effects';
import * as ControllerServicesActions from './controller-services.actions';
import {
    catchError,
    combineLatest,
    filter,
    from,
    map,
    NEVER,
    Observable,
    of,
    switchMap,
    take,
    takeUntil,
    tap,
    withLatestFrom
} from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectControllerServiceTypes } from '../../../../state/extension-types/extension-types.selectors';
import { CreateControllerService } from '../../../../ui/common/controller-service/create-controller-service/create-controller-service.component';
import { Client } from '../../../../service/client.service';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { EditControllerService } from '../../../../ui/common/controller-service/edit-controller-service/edit-controller-service.component';
import {
    ComponentType,
    ControllerServiceReferencingComponent,
    EditParameterRequest,
    EditParameterResponse,
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    NewPropertyDialogRequest,
    NewPropertyDialogResponse,
    Parameter,
    ParameterEntity,
    Property,
    PropertyDescriptor,
    UpdateControllerServiceRequest
} from '../../../../state/shared';
import { NewPropertyDialog } from '../../../../ui/common/new-property-dialog/new-property-dialog.component';
import { Router } from '@angular/router';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { selectCurrentProcessGroupId, selectSaving } from './controller-services.selectors';
import { ControllerServiceService } from '../../service/controller-service.service';
import { selectCurrentParameterContext } from '../flow/flow.selectors';
import { FlowService } from '../../service/flow.service';
import { EditParameterDialog } from '../../../../ui/common/edit-parameter-dialog/edit-parameter-dialog.component';
import { selectParameterSaving } from '../parameter/parameter.selectors';
import * as ParameterActions from '../parameter/parameter.actions';
import { ParameterService } from '../../service/parameter.service';
import { EnableControllerService } from '../../../../ui/common/controller-service/enable-controller-service/enable-controller-service.component';
import { DisableControllerService } from '../../../../ui/common/controller-service/disable-controller-service/disable-controller-service.component';

@Injectable()
export class ControllerServicesEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private controllerServiceService: ControllerServiceService,
        private flowService: FlowService,
        private parameterService: ParameterService,
        private extensionTypesService: ExtensionTypesService,
        private dialog: MatDialog,
        private router: Router
    ) {}

    loadControllerServices$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.loadControllerServices),
            map((action) => action.request),
            switchMap((request) =>
                combineLatest([
                    this.controllerServiceService.getControllerServices(request.processGroupId),
                    this.controllerServiceService.getBreadcrumbs(request.processGroupId)
                ]).pipe(
                    map(([controllerServicesResponse, breadcrumbsResponse]) =>
                        ControllerServicesActions.loadControllerServicesSuccess({
                            response: {
                                processGroupId: breadcrumbsResponse.id,
                                controllerServices: controllerServicesResponse.controllerServices,
                                loadedTimestamp: controllerServicesResponse.currentTime,
                                breadcrumb: breadcrumbsResponse
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ControllerServicesActions.controllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    openNewControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openNewControllerServiceDialog),
                withLatestFrom(
                    this.store.select(selectControllerServiceTypes),
                    this.store.select(selectCurrentProcessGroupId)
                ),
                tap(([action, controllerServiceTypes, processGroupId]) => {
                    const dialogReference = this.dialog.open(CreateControllerService, {
                        data: {
                            controllerServiceTypes
                        },
                        panelClass: 'medium-dialog'
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.createControllerService
                        .pipe(take(1))
                        .subscribe((controllerServiceType) => {
                            this.store.dispatch(
                                ControllerServicesActions.createControllerService({
                                    request: {
                                        revision: {
                                            clientId: this.client.getClientId(),
                                            version: 0
                                        },
                                        processGroupId,
                                        controllerServiceType: controllerServiceType.type,
                                        controllerServiceBundle: controllerServiceType.bundle
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    createControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.createControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.createControllerService(request)).pipe(
                    map((response) =>
                        ControllerServicesActions.createControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ControllerServicesActions.controllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    createControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.createControllerServiceSuccess),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    navigateToEditService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.navigateToEditService),
                map((action) => action.id),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([id, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, 'controller-services', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    openConfigureControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openConfigureControllerServiceDialog),
                map((action) => action.request),
                withLatestFrom(
                    this.store.select(selectCurrentParameterContext),
                    this.store.select(selectCurrentProcessGroupId)
                ),
                tap(([request, parameterContext, processGroupId]) => {
                    const serviceId: string = request.id;

                    const editDialogReference = this.dialog.open(EditControllerService, {
                        data: {
                            controllerService: request.controllerService
                        },
                        id: serviceId,
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
                                return this.controllerServiceService
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
                        if (editDialogReference.componentInstance.editControllerServiceForm.dirty) {
                            const saveChangesDialogReference = this.dialog.open(YesNoDialog, {
                                data: {
                                    title: 'Controller Service Configuration',
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

                    editDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        const route: string[] = this.getRouteForReference(component);
                        goTo(route, component.referenceType);
                    };

                    if (parameterContext != null) {
                        editDialogReference.componentInstance.getParameters = (sensitive: boolean) => {
                            return this.flowService.getParameterContext(parameterContext.id).pipe(
                                take(1),
                                map((response) => response.component.parameters),
                                map((parameterEntities) => {
                                    return parameterEntities
                                        .map((parameterEntity: ParameterEntity) => parameterEntity.parameter)
                                        .filter((parameter: Parameter) => parameter.sensitive == sensitive);
                                })
                            );
                        };

                        editDialogReference.componentInstance.parameterContext = parameterContext;
                        editDialogReference.componentInstance.goToParameter = (parameter: string) => {
                            const commands: string[] = ['/parameter-contexts', parameterContext.id];
                            goTo(commands, 'Parameter');
                        };

                        editDialogReference.componentInstance.convertToParameter = (
                            name: string,
                            sensitive: boolean,
                            value: string | null
                        ) => {
                            return this.parameterService.getParameterContext(parameterContext.id, false).pipe(
                                switchMap((parameterContextEntity) => {
                                    const existingParameters: string[] =
                                        parameterContextEntity.component.parameters.map(
                                            (parameterEntity: ParameterEntity) => parameterEntity.parameter.name
                                        );
                                    const convertToParameterDialogRequest: EditParameterRequest = {
                                        parameter: {
                                            name,
                                            value,
                                            sensitive,
                                            description: ''
                                        },
                                        existingParameters
                                    };
                                    const convertToParameterDialogReference = this.dialog.open(EditParameterDialog, {
                                        data: convertToParameterDialogRequest,
                                        panelClass: 'medium-dialog'
                                    });

                                    convertToParameterDialogReference.componentInstance.saving$ =
                                        this.store.select(selectParameterSaving);

                                    convertToParameterDialogReference.componentInstance.cancel.pipe(
                                        takeUntil(convertToParameterDialogReference.afterClosed()),
                                        tap(() => ParameterActions.stopPollingParameterContextUpdateRequest())
                                    );

                                    return convertToParameterDialogReference.componentInstance.editParameter.pipe(
                                        takeUntil(convertToParameterDialogReference.afterClosed()),
                                        switchMap((dialogResponse: EditParameterResponse) => {
                                            this.store.dispatch(
                                                ParameterActions.submitParameterContextUpdateRequest({
                                                    request: {
                                                        id: parameterContext.id,
                                                        payload: {
                                                            revision: this.client.getRevision(parameterContextEntity),
                                                            component: {
                                                                id: parameterContextEntity.id,
                                                                parameters: [{ parameter: dialogResponse.parameter }]
                                                            }
                                                        }
                                                    }
                                                })
                                            );

                                            return this.store.select(selectParameterSaving).pipe(
                                                takeUntil(convertToParameterDialogReference.afterClosed()),
                                                filter((parameterSaving) => parameterSaving === false),
                                                map(() => {
                                                    convertToParameterDialogReference.close();
                                                    return `#{${dialogResponse.parameter.name}}`;
                                                })
                                            );
                                        })
                                    );
                                }),
                                catchError((error) => {
                                    // TODO handle error
                                    return NEVER;
                                })
                            );
                        };
                    }

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        this.controllerServiceService.getControllerService(serviceId).subscribe({
                            next: (serviceEntity) => {
                                const commands: string[] = [
                                    '/process-groups',
                                    serviceEntity.component.parentGroupId,
                                    'controller-services',
                                    serviceEntity.id
                                ];
                                goTo(commands, 'Controller Service');
                            },
                            error: () => {
                                // TODO - handle error
                            }
                        });
                    };

                    editDialogReference.componentInstance.createNewService = (
                        serviceRequest: InlineServiceCreationRequest
                    ): Observable<InlineServiceCreationResponse> => {
                        const descriptor: PropertyDescriptor = serviceRequest.descriptor;

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
                                            return this.controllerServiceService
                                                .createControllerService({
                                                    revision: {
                                                        clientId: this.client.getClientId(),
                                                        version: 0
                                                    },
                                                    processGroupId,
                                                    controllerServiceType: controllerServiceType.type,
                                                    controllerServiceBundle: controllerServiceType.bundle
                                                })
                                                .pipe(
                                                    take(1),
                                                    switchMap((createResponse) => {
                                                        // dispatch an inline create service success action so the new service is in the state
                                                        this.store.dispatch(
                                                            ControllerServicesActions.inlineCreateControllerServiceSuccess(
                                                                {
                                                                    response: {
                                                                        controllerService: createResponse
                                                                    }
                                                                }
                                                            )
                                                        );

                                                        // fetch an updated property descriptor
                                                        return this.controllerServiceService
                                                            .getPropertyDescriptor(serviceId, descriptor.name, false)
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

                    editDialogReference.componentInstance.editControllerService
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((updateControllerServiceRequest: UpdateControllerServiceRequest) => {
                            this.store.dispatch(
                                ControllerServicesActions.configureControllerService({
                                    request: {
                                        id: request.controllerService.id,
                                        uri: request.controllerService.uri,
                                        payload: updateControllerServiceRequest.payload,
                                        postUpdateNavigation: updateControllerServiceRequest.postUpdateNavigation
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ControllerServicesActions.selectControllerService({
                                    request: {
                                        processGroupId,
                                        id: serviceId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    configureControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.configureControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.updateControllerService(request)).pipe(
                    map((response) =>
                        ControllerServicesActions.configureControllerServiceSuccess({
                            response: {
                                id: request.id,
                                controllerService: response,
                                postUpdateNavigation: request.postUpdateNavigation
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ControllerServicesActions.controllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    configureControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.configureControllerServiceSuccess),
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

    openEnableControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openEnableControllerServiceDialog),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([request, currentProcessGroupId]) => {
                    const serviceId: string = request.id;

                    const enableDialogReference = this.dialog.open(EnableControllerService, {
                        data: request,
                        id: serviceId,
                        panelClass: 'large-dialog'
                    });

                    enableDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        enableDialogReference.close('ROUTED');

                        const route: string[] = this.getRouteForReference(component);
                        this.router.navigate(route);
                    };

                    enableDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ControllerServicesActions.loadControllerServices({
                                    request: {
                                        processGroupId: currentProcessGroupId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    openDisableControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openDisableControllerServiceDialog),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([request, currentProcessGroupId]) => {
                    const serviceId: string = request.id;

                    const enableDialogReference = this.dialog.open(DisableControllerService, {
                        data: request,
                        id: serviceId,
                        panelClass: 'large-dialog'
                    });

                    enableDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        enableDialogReference.close('ROUTED');

                        const route: string[] = this.getRouteForReference(component);
                        this.router.navigate(route);
                    };

                    enableDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ControllerServicesActions.loadControllerServices({
                                    request: {
                                        processGroupId: currentProcessGroupId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    promptControllerServiceDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.promptControllerServiceDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Delete Controller Service',
                            message: `Delete controller service ${request.controllerService.component.name}?`
                        },
                        panelClass: 'small-dialog'
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            ControllerServicesActions.deleteControllerService({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.deleteControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.deleteControllerService(request)).pipe(
                    map((response) =>
                        ControllerServicesActions.deleteControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ControllerServicesActions.controllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    selectControllerService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.selectControllerService),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate([
                        '/process-groups',
                        request.processGroupId,
                        'controller-services',
                        request.id
                    ]);
                })
            ),
        { dispatch: false }
    );

    private getRouteForReference(reference: ControllerServiceReferencingComponent): string[] {
        if (reference.referenceType == 'ControllerService') {
            if (reference.groupId == null) {
                return ['/settings', 'management-controller-services', reference.id];
            } else {
                return ['/process-groups', reference.groupId, 'controller-services', reference.id];
            }
        } else if (reference.referenceType == 'ReportingTask') {
            return ['/settings', 'reporting-tasks', reference.id];
        } else if (reference.referenceType == 'Processor') {
            return ['/process-groups', reference.groupId, ComponentType.Processor, reference.id];
        } else if (reference.referenceType == 'FlowAnalysisRule') {
            return ['/settings', 'flow-analysis-rules', reference.id];
        } else if (reference.referenceType == 'ParameterProvider') {
            return ['/settings', 'parameter-providers', reference.id];
        } else {
            return ['/settings', 'registry-clients', reference.id];
        }
    }
}
