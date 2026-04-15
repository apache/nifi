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

import { Injectable, inject } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import { Store } from '@ngrx/store';
import { catchError, filter, map, of, switchMap } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import * as ConnectorCanvasActions from '../connector-canvas/connector-canvas.actions';
import { selectConnectorCanvasEntity } from './connector-canvas-entity.selectors';
import {
    loadConnectorEntity,
    loadConnectorEntitySuccess,
    loadConnectorEntityFailure
} from './connector-canvas-entity.actions';

@Injectable()
export class ConnectorCanvasEntityEffects {
    private actions$ = inject(Actions);
    private store = inject(Store);
    private connectorService = inject(ConnectorService);
    private errorHelper = inject(ErrorHelper);

    triggerEntityLoad$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ConnectorCanvasActions.loadConnectorFlow),
            concatLatestFrom(() => this.store.select(selectConnectorCanvasEntity)),
            filter(([action, existingEntity]) => existingEntity == null || existingEntity.id !== action.connectorId),
            map(([action]) => loadConnectorEntity({ connectorId: action.connectorId }))
        )
    );

    loadConnectorEntity$ = createEffect(() =>
        this.actions$.pipe(
            ofType(loadConnectorEntity),
            switchMap((action) =>
                this.connectorService.getConnector(action.connectorId).pipe(
                    map((connectorEntity) => loadConnectorEntitySuccess({ connectorEntity })),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(loadConnectorEntityFailure({ error: this.errorHelper.getErrorString(errorResponse) }))
                    )
                )
            )
        )
    );
}
