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
import * as ControllerServiceDefinitionActions from './controller-service-definition.actions';
import { catchError, from, map, of, switchMap } from 'rxjs';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { DocumentationService } from '../../service/documentation.service';
import { ControllerServiceDefinition } from './index';

@Injectable()
export class ControllerServiceDefinitionEffects {
    private actions$ = inject(Actions);
    private documentationService = inject(DocumentationService);
    private errorHelper = inject(ErrorHelper);

    loadControllerServiceDefinition$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServiceDefinitionActions.loadControllerServiceDefinition),
            map((action) => action.coordinates),
            switchMap((coordinates) =>
                from(this.documentationService.getControllerServiceDefinition(coordinates)).pipe(
                    map((controllerServiceDefinition: ControllerServiceDefinition) =>
                        ControllerServiceDefinitionActions.loadControllerServiceDefinitionSuccess({
                            controllerServiceDefinition
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ControllerServiceDefinitionActions.controllerServiceDefinitionApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );
}
