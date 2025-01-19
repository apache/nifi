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
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { ConfigurationAnalysisResponse, PropertyVerificationState } from './index';
import { Store } from '@ngrx/store';
import { MatDialog } from '@angular/material/dialog';
import { ErrorHelper } from '../../service/error-helper.service';
import * as VerificationActions from './property-verification.actions';
import * as ErrorActions from '../error/error.actions';
import { asyncScheduler, catchError, filter, from, interval, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { PropertyVerificationService } from '../../service/property-verification.service';
import { HttpErrorResponse } from '@angular/common/http';
import { concatLatestFrom } from '@ngrx/operators';
import {
    selectActivePropertyVerificationRequest,
    selectPropertyVerificationAttributes,
    selectPropertyVerificationRequestContext
} from './property-verification.selectors';
import { PropertyVerificationProgress } from '../../ui/common/property-verification/common/property-verification-progress/property-verification-progress.component';
import { ReferencedAttributesDialog } from '../../ui/common/property-verification/common/referenced-attributes-dialog/referenced-attributes-dialog.component';
import { PropertyTableHelperService } from '../../service/property-table-helper.service';
import { isDefinedAndNotNull, MEDIUM_DIALOG, SMALL_DIALOG, MapTableHelperService, MapTableEntry } from '@nifi/shared';

@Injectable()
export class PropertyVerificationEffects {
    constructor(
        private actions$: Actions,
        private store: Store<PropertyVerificationState>,
        private dialog: MatDialog,
        private errorHelper: ErrorHelper,
        private propertyVerificationService: PropertyVerificationService,
        private propertyTableHelperService: PropertyTableHelperService,
        private mapTableHelperService: MapTableHelperService
    ) {}

    verifyProperties$ = createEffect(() =>
        this.actions$.pipe(
            ofType(VerificationActions.verifyProperties),
            map((action) => action.request),
            switchMap((request) => {
                // get the configuration analysis
                return from(this.propertyVerificationService.getAnalysis(request)).pipe(
                    map((response) =>
                        VerificationActions.getConfigurationAnalysisSuccess({
                            response: {
                                requestContext: request,
                                configurationAnalysis: response.configurationAnalysis
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) }))
                    )
                );
            })
        )
    );

    getConfigurationAnalysisSuccess_noExtraVerification$ = createEffect(() =>
        this.actions$.pipe(
            ofType(VerificationActions.getConfigurationAnalysisSuccess),
            map((action) => action.response),
            filter((response) => !response.configurationAnalysis.supportsVerification),
            switchMap((response) => {
                return of(VerificationActions.initiatePropertyVerification({ response }));
            })
        )
    );

    getConfigurationAnalysisSuccess_extraVerification$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(VerificationActions.getConfigurationAnalysisSuccess),
                map((action) => action.response),
                filter((response) => response.configurationAnalysis.supportsVerification),
                concatLatestFrom(() => this.store.select(selectPropertyVerificationAttributes)),
                tap(([response, previousAttributes]) => {
                    let referencedAttributes: MapTableEntry[] = [];
                    if (previousAttributes) {
                        referencedAttributes = Object.entries(previousAttributes).map(([key, value]) => {
                            return {
                                name: key,
                                value: value
                            } as MapTableEntry;
                        });
                    }
                    const dialogRef = this.dialog.open(ReferencedAttributesDialog, {
                        ...MEDIUM_DIALOG,
                        data: {
                            attributes: referencedAttributes
                        }
                    });

                    dialogRef.componentInstance.createNew = this.mapTableHelperService.createNewEntry('Attribute');

                    dialogRef.componentInstance.verify
                        .pipe(takeUntil(dialogRef.afterClosed()))
                        .subscribe((formData) => {
                            const attributesArray: MapTableEntry[] = formData.attributes || [];
                            const attributesMap: { [key: string]: string | null } = {};
                            attributesArray.forEach((entry: MapTableEntry) => {
                                attributesMap[entry.name] = entry.value;
                            });
                            const responseWithAttributes: ConfigurationAnalysisResponse = {
                                ...response,
                                configurationAnalysis: {
                                    ...response.configurationAnalysis,
                                    referencedAttributes: attributesMap
                                }
                            };
                            this.store.dispatch(
                                VerificationActions.initiatePropertyVerification({ response: responseWithAttributes })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    initiatePropertyVerification$ = createEffect(() =>
        this.actions$.pipe(
            ofType(VerificationActions.initiatePropertyVerification),
            map((action) => action.response),
            switchMap((response) => {
                this.store.dispatch(VerificationActions.openPropertyVerificationProgressDialog());
                // if the component does not support additional verification there is no need to prompt for attribute values
                return from(
                    this.propertyVerificationService
                        .initiatePropertyVerification({
                            request: {
                                request: {
                                    properties: response.configurationAnalysis.properties,
                                    componentId: response.configurationAnalysis.componentId,
                                    attributes: response.configurationAnalysis.referencedAttributes
                                }
                            },
                            uri: response.requestContext.entity.uri
                        })
                        .pipe(
                            map((response) => {
                                return VerificationActions.verifyPropertiesSuccess({ response });
                            }),
                            catchError((errorResponse: HttpErrorResponse) =>
                                of(
                                    ErrorActions.snackBarError({
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    })
                                )
                            )
                        )
                );
            })
        )
    );

    openPropertyVerificationProgressDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(VerificationActions.openPropertyVerificationProgressDialog),
                tap(() => {
                    const dialogRef = this.dialog.open(PropertyVerificationProgress, {
                        ...SMALL_DIALOG
                    });
                    const verificationRequest$ = this.store
                        .select(selectActivePropertyVerificationRequest)
                        .pipe(isDefinedAndNotNull());
                    dialogRef.componentInstance.verificationRequest$ = verificationRequest$;

                    verificationRequest$
                        .pipe(
                            takeUntil(dialogRef.afterClosed()),
                            isDefinedAndNotNull(),
                            filter((request) => request.complete)
                        )
                        .subscribe((request) => {
                            if (request.failureReason) {
                                this.store.dispatch(ErrorActions.snackBarError({ error: request.failureReason }));
                            }
                            // close the dialog now that it is complete
                            dialogRef.close();
                        });

                    dialogRef.componentInstance.stopVerification.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(VerificationActions.stopPollingPropertyVerification());
                    });
                })
            ),
        { dispatch: false }
    );

    verifyPropertiesSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(VerificationActions.verifyPropertiesSuccess),
            map((action) => action.response),
            filter((response) => !response.request.complete),
            switchMap(() => {
                return of(VerificationActions.startPollingPropertyVerification());
            })
        )
    );

    startPollingPropertyVerification$ = createEffect(() =>
        this.actions$.pipe(
            ofType(VerificationActions.startPollingPropertyVerification),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(VerificationActions.stopPollingPropertyVerification)))
                )
            ),
            switchMap(() => of(VerificationActions.pollPropertyVerification()))
        )
    );

    pollPropertyVerification$ = createEffect(() =>
        this.actions$.pipe(
            ofType(VerificationActions.pollPropertyVerification),
            concatLatestFrom(() => [
                this.store.select(selectPropertyVerificationRequestContext).pipe(isDefinedAndNotNull()),
                this.store.select(selectActivePropertyVerificationRequest).pipe(isDefinedAndNotNull())
            ]),
            switchMap(([, requestContext, verifyRequest]) => {
                return from(
                    this.propertyVerificationService
                        .getPropertyVerificationRequest(verifyRequest.requestId, requestContext.entity.uri)
                        .pipe(
                            map((response) => VerificationActions.pollPropertyVerificationSuccess({ response })),
                            catchError((errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(VerificationActions.stopPollingPropertyVerification());
                                return of(
                                    ErrorActions.snackBarError({
                                        error: this.errorHelper.getErrorString(errorResponse)
                                    })
                                );
                            })
                        )
                );
            })
        )
    );

    pollPropertyVerificationSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(VerificationActions.pollPropertyVerificationSuccess),
            map((action) => action.response),
            filter((response) => response.request.complete),
            switchMap(() => of(VerificationActions.stopPollingPropertyVerification()))
        )
    );

    stopPollingPropertyVerification$ = createEffect(() =>
        this.actions$.pipe(
            ofType(VerificationActions.stopPollingPropertyVerification),
            concatLatestFrom(() => [
                this.store.select(selectPropertyVerificationRequestContext).pipe(isDefinedAndNotNull()),
                this.store.select(selectActivePropertyVerificationRequest).pipe(isDefinedAndNotNull())
            ]),
            switchMap(([, requestContext, verifyRequest]) =>
                from(
                    this.propertyVerificationService.deletePropertyVerificationRequest(
                        verifyRequest.requestId,
                        requestContext.entity.uri
                    )
                ).pipe(
                    map(() => VerificationActions.propertyVerificationComplete()),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ErrorActions.snackBarError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );
}
