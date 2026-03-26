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

import { Injectable, inject } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import * as ConnectorDefinitionActions from './connector-definition.actions';
import { catchError, from, map, mergeMap, of, switchMap } from 'rxjs';
import { DocumentationService } from '../../service/documentation.service';
import { ConnectorDefinition } from './index';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';

@Injectable()
export class ConnectorDefinitionEffects {
    private actions$ = inject(Actions);
    private documentationService = inject(DocumentationService);
    private errorHelper = inject(ErrorHelper);

    loadConnectorDefinition$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorDefinitionActions.loadConnectorDefinition),
            map((action) => action.coordinates),
            switchMap((coordinates) =>
                from(this.documentationService.getConnectorDefinition(coordinates)).pipe(
                    map((connectorDefinition: ConnectorDefinition) =>
                        ConnectorDefinitionActions.loadConnectorDefinitionSuccess({
                            connectorDefinition
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ConnectorDefinitionActions.connectorDefinitionApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    loadStepDocumentation$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorDefinitionActions.loadStepDocumentation),
            mergeMap((action) =>
                from(this.documentationService.getStepDocumentation(action.coordinates, action.stepName)).pipe(
                    map((response) =>
                        ConnectorDefinitionActions.loadStepDocumentationSuccess({
                            stepName: action.stepName,
                            documentation: response.stepDocumentation
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ConnectorDefinitionActions.stepDocumentationApiError({
                                stepName: action.stepName,
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );
}
