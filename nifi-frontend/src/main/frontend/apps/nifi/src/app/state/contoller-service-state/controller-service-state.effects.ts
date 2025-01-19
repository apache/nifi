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
import { concatLatestFrom } from '@ngrx/operators';
import * as ControllerServiceActions from './controller-service-state.actions';
import { asyncScheduler, catchError, filter, from, interval, map, of, switchMap, takeUntil, tap } from 'rxjs';
import { Store } from '@ngrx/store';
import { NiFiState } from '../index';
import { selectControllerService, selectControllerServiceSetEnableRequest } from './controller-service-state.selectors';
import { OkDialog } from '../../ui/common/ok-dialog/ok-dialog.component';
import { MatDialog } from '@angular/material/dialog';
import { ControllerServiceStateService } from '../../service/controller-service-state.service';
import { ControllerServiceEntity, ControllerServiceReferencingComponentEntity } from '../shared';
import { SetEnableRequest, SetEnableStep } from './index';
import { isDefinedAndNotNull, MEDIUM_DIALOG } from '@nifi/shared';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../service/error-helper.service';

@Injectable()
export class ControllerServiceStateEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private dialog: MatDialog,
        private controllerServiceStateService: ControllerServiceStateService,
        private errorHelper: ErrorHelper
    ) {}

    submitEnableRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.submitEnableRequest),
            map((action) => action.request),
            concatLatestFrom(() => this.store.select(selectControllerService).pipe(isDefinedAndNotNull())),
            switchMap(([request, controllerService]) => {
                if (
                    request.scope === 'SERVICE_AND_REFERENCING_COMPONENTS' &&
                    this.hasUnauthorizedReferences(controllerService.component.referencingComponents)
                ) {
                    return of(
                        ControllerServiceActions.setEnableStepFailure({
                            response: {
                                step: SetEnableStep.EnableService,
                                error: 'Unable to enable due to unauthorized referencing components.'
                            }
                        })
                    );
                } else {
                    return of(ControllerServiceActions.setEnableControllerService());
                }
            })
        )
    );

    submitDisableRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.submitDisableRequest),
            concatLatestFrom(() => this.store.select(selectControllerService).pipe(isDefinedAndNotNull())),
            switchMap(([, controllerService]) => {
                if (this.hasUnauthorizedReferences(controllerService.component.referencingComponents)) {
                    return of(
                        ControllerServiceActions.setEnableStepFailure({
                            response: {
                                step: SetEnableStep.StopReferencingComponents,
                                error: 'Unable to disable due to unauthorized referencing components.'
                            }
                        })
                    );
                } else {
                    return of(ControllerServiceActions.updateReferencingComponents());
                }
            })
        )
    );

    setEnableControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.setEnableControllerService),
            concatLatestFrom(() => [
                this.store.select(selectControllerService).pipe(isDefinedAndNotNull()),
                this.store.select(selectControllerServiceSetEnableRequest)
            ]),
            switchMap(([, controllerService, setEnableRequest]) =>
                from(this.controllerServiceStateService.setEnable(controllerService, setEnableRequest.enable)).pipe(
                    map((response) =>
                        ControllerServiceActions.setEnableControllerServiceSuccess({
                            response: {
                                controllerService: response,
                                currentStep: setEnableRequest.currentStep
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ControllerServiceActions.setEnableStepFailure({
                                response: {
                                    step: setEnableRequest.currentStep,
                                    error: this.errorHelper.getErrorString(errorResponse)
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    setEnableControllerServiceSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.setEnableControllerServiceSuccess),
            map((action) => action.response),
            // if the current step is DisableService, it's the end of a disable request and there is no need to start polling
            filter((response) => response.currentStep !== SetEnableStep.DisableService),
            switchMap(() => of(ControllerServiceActions.startPollingControllerService()))
        )
    );

    startPollingControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.startPollingControllerService),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(ControllerServiceActions.stopPollingControllerService)))
                )
            ),
            switchMap(() => of(ControllerServiceActions.pollControllerService()))
        )
    );

    pollControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.pollControllerService),
            concatLatestFrom(() => [
                this.store.select(selectControllerService).pipe(isDefinedAndNotNull()),
                this.store.select(selectControllerServiceSetEnableRequest)
            ]),
            switchMap(([, controllerService, setEnableRequest]) =>
                from(this.controllerServiceStateService.getControllerService(controllerService.id)).pipe(
                    map((response) =>
                        ControllerServiceActions.pollControllerServiceSuccess({
                            response: {
                                controllerService: response,
                                currentStep: this.getNextStep(setEnableRequest, controllerService)
                            },
                            previousStep: setEnableRequest.currentStep
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ControllerServiceActions.setEnableStepFailure({
                                response: {
                                    step: setEnableRequest.currentStep,
                                    error: this.errorHelper.getErrorString(errorResponse)
                                }
                            })
                        )
                    )
                )
            )
        )
    );

    setEnableStepComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.pollControllerServiceSuccess),
            filter((action) => {
                const response = action.response;
                const previousStep = action.previousStep;

                // if the state hasn't transitioned we don't want trigger the next action
                return response.currentStep !== previousStep;
            }),
            switchMap((action) => {
                const response = action.response;

                // the request in the store will drive whether the action will enable or disable
                switch (response.currentStep) {
                    case SetEnableStep.DisableReferencingServices:
                    case SetEnableStep.EnableReferencingServices:
                        return of(ControllerServiceActions.updateReferencingServices());
                    case SetEnableStep.DisableService:
                    case SetEnableStep.EnableService:
                        return of(ControllerServiceActions.setEnableControllerService());
                    case SetEnableStep.StopReferencingComponents:
                    case SetEnableStep.StartReferencingComponents:
                        return of(ControllerServiceActions.updateReferencingComponents());
                    case SetEnableStep.Completed:
                    default:
                        // if the sequence is complete or if it's an unexpected step stop polling
                        return of(ControllerServiceActions.stopPollingControllerService());
                }
            })
        )
    );

    updateReferencingServices$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.updateReferencingServices),
            concatLatestFrom(() => [
                this.store.select(selectControllerService),
                this.store.select(selectControllerServiceSetEnableRequest)
            ]),
            switchMap(([, controllerService, setEnableRequest]) => {
                if (controllerService) {
                    return from(
                        this.controllerServiceStateService.updateReferencingServices(
                            controllerService,
                            setEnableRequest.enable
                        )
                    ).pipe(
                        map((response) =>
                            ControllerServiceActions.updateReferencingServicesSuccess({
                                response: {
                                    referencingComponents: response.controllerServiceReferencingComponents,
                                    currentStep: setEnableRequest.currentStep
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(
                                ControllerServiceActions.setEnableStepFailure({
                                    response: {
                                        step: setEnableRequest.currentStep,
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    }
                                })
                            )
                        )
                    );
                } else {
                    return of(
                        ControllerServiceActions.showOkDialog({
                            title: 'Enable Service',
                            message: 'Controller Service not initialized'
                        })
                    );
                }
            })
        )
    );

    updateReferencingComponents$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.updateReferencingComponents),
            concatLatestFrom(() => [
                this.store.select(selectControllerService),
                this.store.select(selectControllerServiceSetEnableRequest)
            ]),
            switchMap(([, controllerService, setEnableRequest]) => {
                if (controllerService) {
                    return from(
                        this.controllerServiceStateService.updateReferencingSchedulableComponents(
                            controllerService,
                            setEnableRequest.enable
                        )
                    ).pipe(
                        map((response) =>
                            ControllerServiceActions.updateReferencingComponentsSuccess({
                                response: {
                                    referencingComponents: response.controllerServiceReferencingComponents,
                                    currentStep: setEnableRequest.currentStep
                                }
                            })
                        ),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(
                                ControllerServiceActions.setEnableStepFailure({
                                    response: {
                                        step: setEnableRequest.currentStep,
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    }
                                })
                            )
                        )
                    );
                } else {
                    return of(
                        ControllerServiceActions.showOkDialog({
                            title: 'Enable Service',
                            message: 'Controller Service not initialized'
                        })
                    );
                }
            })
        )
    );

    updateReferencingComponentsSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.updateReferencingComponentsSuccess),
            map((action) => action.response),
            // if the current step is StartReferencingComponents, it's the end of an enable request and there is no need to start polling
            filter((response) => response.currentStep !== SetEnableStep.StartReferencingComponents),
            switchMap(() => of(ControllerServiceActions.startPollingControllerService()))
        )
    );

    setEnableStepFailure$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceActions.setEnableStepFailure),
            switchMap(() => of(ControllerServiceActions.stopPollingControllerService()))
        )
    );

    showOkDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServiceActions.showOkDialog),
                tap((request) => {
                    this.dialog.open(OkDialog, {
                        ...MEDIUM_DIALOG,
                        data: {
                            title: request.title,
                            message: request.message
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    hasUnauthorizedReferences(referencingComponents: ControllerServiceReferencingComponentEntity[]): boolean {
        // if there are no referencing components there is nothing unauthorized
        if (referencingComponents.length === 0) {
            return false;
        }

        // identify any unauthorized referencing components
        const unauthorized: boolean = referencingComponents.some((referencingComponentEntity) => {
            return !referencingComponentEntity.permissions.canRead || !referencingComponentEntity.permissions.canWrite;
        });

        // if any are unauthorized there is no need to check further
        if (unauthorized) {
            return true;
        }

        // consider the components that are referencing the referencingServices
        const referencingServices = referencingComponents.filter((referencingComponent) => {
            return referencingComponent.component.referenceType === 'ControllerService';
        });

        if (referencingServices.length === 0) {
            // if there are no more nested services all references are authorized
            return false;
        } else {
            // if there are nested services, check if they have any referencing components are unauthorized
            return referencingServices.some((referencingService) => {
                return this.hasUnauthorizedReferences(referencingService.component.referencingComponents);
            });
        }
    }

    getNextStep(request: SetEnableRequest, controllerServiceEntity: ControllerServiceEntity): SetEnableStep {
        switch (request.currentStep) {
            case SetEnableStep.EnableService:
                return this.isServiceActionComplete(controllerServiceEntity, ['ENABLED', 'ENABLING'])
                    ? request.scope === 'SERVICE_ONLY'
                        ? SetEnableStep.Completed
                        : SetEnableStep.EnableReferencingServices
                    : request.currentStep;
            case SetEnableStep.EnableReferencingServices:
                return this.isReferencingServicesActionComplete(
                    controllerServiceEntity.component.referencingComponents,
                    ['ENABLED', 'ENABLING']
                )
                    ? SetEnableStep.StartReferencingComponents
                    : request.currentStep;
            case SetEnableStep.StartReferencingComponents:
                // since we are starting components, there is no condition to wait for
                return SetEnableStep.Completed;
            case SetEnableStep.StopReferencingComponents:
                return this.areReferencingComponentsStopped(controllerServiceEntity.component.referencingComponents)
                    ? SetEnableStep.DisableReferencingServices
                    : request.currentStep;
            case SetEnableStep.DisableReferencingServices:
                return this.isReferencingServicesActionComplete(
                    controllerServiceEntity.component.referencingComponents,
                    ['DISABLED']
                )
                    ? SetEnableStep.DisableService
                    : request.currentStep;
            case SetEnableStep.DisableService:
                return this.isServiceActionComplete(controllerServiceEntity, ['DISABLED'])
                    ? SetEnableStep.Completed
                    : request.currentStep;
            default:
                return request.currentStep;
        }
    }

    isServiceActionComplete(controllerServiceEntity: ControllerServiceEntity, acceptedRunStatus: string[]): boolean {
        return acceptedRunStatus.includes(controllerServiceEntity.status.runStatus);
    }

    isReferencingServicesActionComplete(
        referencingComponents: ControllerServiceReferencingComponentEntity[],
        acceptedRunStatus: string[]
    ): boolean {
        const referencingServices = referencingComponents.filter((referencingComponent) => {
            return referencingComponent.component.referenceType === 'ControllerService';
        });

        if (referencingServices.length === 0) {
            return true;
        }

        return referencingServices.some((referencingService) => {
            const isEnabled: boolean = acceptedRunStatus.includes(referencingService.component.state);

            if (isEnabled) {
                // if this service isn't enabled, there is no need to check further...
                return this.isReferencingServicesActionComplete(
                    referencingService.component.referencingComponents,
                    acceptedRunStatus
                );
            }

            return isEnabled;
        });
    }

    areReferencingComponentsStopped(referencingComponents: ControllerServiceReferencingComponentEntity[]): boolean {
        // consider the schedulable components in the referencingComponents
        const referencingScheduleableComponents = referencingComponents.filter((referencingComponent) => {
            return (
                referencingComponent.component.referenceType === 'Processor' ||
                referencingComponent.component.referenceType === 'ReportingTask'
            );
        });
        const stillRunning: boolean = referencingScheduleableComponents.some((referencingComponentEntity) => {
            const referencingComponent = referencingComponentEntity.component;
            return (
                referencingComponent.state === 'RUNNING' ||
                (referencingComponent.activeThreadCount && referencingComponent.activeThreadCount > 0)
            );
        });

        // if any are still running, there is no need to check further...
        if (stillRunning) {
            return false;
        }

        // consider the scheduleable components that are referencing the referencingServices
        const referencingServices = referencingComponents.filter((referencingComponent) => {
            return referencingComponent.component.referenceType === 'ControllerService';
        });

        if (referencingServices.length === 0) {
            // if there are no more nested services all schedulable components have stopped
            return true;
        } else {
            // if there are nested services, check if they have any referencing components that are still running
            return referencingServices.some((referencingService) => {
                return this.areReferencingComponentsStopped(referencingService.component.referencingComponents);
            });
        }
    }
}
