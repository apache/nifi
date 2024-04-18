/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Injectable } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { catchError, EMPTY, filter, map, Observable, switchMap, take, takeUntil, tap } from 'rxjs';
import { Store } from '@ngrx/store';
import { HttpErrorResponse } from '@angular/common/http';
import { NiFiState } from '../../../state';
import { ParameterService } from './parameter.service';
import { Client } from '../../../service/client.service';
import { EditParameterRequest, EditParameterResponse, Parameter, ParameterEntity } from '../../../state/shared';
import { EditParameterDialog } from '../../../ui/common/edit-parameter-dialog/edit-parameter-dialog.component';
import { selectParameterSaving, selectParameterState } from '../state/parameter/parameter.selectors';
import { ParameterState } from '../state/parameter';
import * as ErrorActions from '../../../state/error/error.actions';
import * as ParameterActions from '../state/parameter/parameter.actions';
import { FlowService } from './flow.service';
import { MEDIUM_DIALOG } from '../../../index';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';

@Injectable({
    providedIn: 'root'
})
export class ParameterHelperService {
    constructor(
        private dialog: MatDialog,
        private store: Store<NiFiState>,
        private flowService: FlowService,
        private parameterService: ParameterService,
        private clusterConnectionService: ClusterConnectionService,
        private client: Client
    ) {}

    /**
     * Returns a function that can be used to pass into a PropertyTable to retrieve available Parameters.
     *
     * @param parameterContextId the current Parameter Context id
     */
    getParameters(parameterContextId: string): (sensitive: boolean) => Observable<Parameter[]> {
        return (sensitive: boolean) => {
            return this.flowService.getParameterContext(parameterContextId).pipe(
                take(1),
                catchError((errorResponse: HttpErrorResponse) => {
                    this.store.dispatch(ErrorActions.snackBarError({ error: errorResponse.error }));

                    // consider the error handled and allow the user to reattempt the action
                    return EMPTY;
                }),
                map((response) => response.component.parameters),
                map((parameterEntities) => {
                    return parameterEntities
                        .map((parameterEntity: ParameterEntity) => parameterEntity.parameter)
                        .filter((parameter: Parameter) => parameter.sensitive == sensitive);
                })
            );
        };
    }

    /**
     * Returns a function that can be used to pass into a PropertyTable to convert a Property into a Parameter, inline.
     *
     * @param parameterContextId the current Parameter Context id
     */
    convertToParameter(
        parameterContextId: string
    ): (name: string, sensitive: boolean, value: string | null) => Observable<string> {
        return (name: string, sensitive: boolean, value: string | null) => {
            return this.parameterService.getParameterContext(parameterContextId, false).pipe(
                catchError((errorResponse: HttpErrorResponse) => {
                    this.store.dispatch(ErrorActions.snackBarError({ error: errorResponse.error }));

                    // consider the error handled and allow the user to reattempt the action
                    return EMPTY;
                }),
                switchMap((parameterContextEntity) => {
                    const existingParameters: string[] = parameterContextEntity.component.parameters.map(
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
                        ...MEDIUM_DIALOG,
                        data: convertToParameterDialogRequest
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
                                        id: parameterContextId,
                                        payload: {
                                            revision: this.client.getRevision(parameterContextEntity),
                                            disconnectedNodeAcknowledged:
                                                this.clusterConnectionService.isDisconnectionAcknowledged(),
                                            component: {
                                                id: parameterContextEntity.id,
                                                parameters: [{ parameter: dialogResponse.parameter }]
                                            }
                                        }
                                    }
                                })
                            );

                            return this.store.select(selectParameterState).pipe(
                                takeUntil(convertToParameterDialogReference.afterClosed()),
                                tap((parameterState: ParameterState) => {
                                    if (parameterState.error) {
                                        // if the convert to parameter sequence stores an error,
                                        // throw it to avoid the completion mapping logic below
                                        throw new Error(parameterState.error);
                                    }
                                }),
                                filter((parameterState: ParameterState) => !parameterState.saving),
                                map(() => {
                                    convertToParameterDialogReference.close();
                                    return `#{${dialogResponse.parameter.name}}`;
                                }),
                                catchError((error) => {
                                    convertToParameterDialogReference.close();

                                    // show the error in the snack and complete the edit to reset it's state
                                    this.store.dispatch(ErrorActions.snackBarError({ error: error.message }));
                                    this.store.dispatch(ParameterActions.editParameterContextComplete());

                                    // consider the error handled and allow the user to reattempt the action
                                    return EMPTY;
                                })
                            );
                        })
                    );
                })
            );
        };
    }
}
