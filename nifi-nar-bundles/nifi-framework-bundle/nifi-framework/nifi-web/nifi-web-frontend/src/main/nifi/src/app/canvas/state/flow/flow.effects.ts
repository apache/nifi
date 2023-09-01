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
import { FlowService } from '../../service/flow.service';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import * as FlowActions from './flow.actions';
import { catchError, from, map, of, switchMap } from 'rxjs';
import {
    EnterProcessGroupRequest,
    EnterProcessGroupResponse,
    UpdateComponentFailure,
    UpdateComponentPositionResponse,
    UpdateComponentResponse
} from '../index';

@Injectable()
export class FlowEffects {
    constructor(
        private actions$: Actions,
        private flowService: FlowService
    ) {}

    loadFlow$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.enterProcessGroup),
            map((action) => action.request),
            switchMap((request: EnterProcessGroupRequest) =>
                from(this.flowService.getFlow(request.id)).pipe(
                    map((flow) =>
                        FlowActions.enterProcessGroupSuccess({
                            response: {
                                id: request.id,
                                selection: request.selection,
                                flow: flow
                            }
                        })
                    ),
                    catchError((error) => of(FlowActions.flowApiError({ error })))
                )
            )
        )
    );

    enterProcessGroupSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.enterProcessGroupSuccess),
            map((action) => action.response),
            switchMap((response: EnterProcessGroupResponse) => {
                return of(
                    FlowActions.enterProcessGroupComplete({
                        response: response
                    })
                );
            })
        )
    );

    enterProcessGroupComplete$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.enterProcessGroupComplete),
            switchMap(() => {
                return of(FlowActions.setRenderRequired({ renderRequired: false }));
            })
        )
    );

    updateComponent$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updateComponent),
            map((action) => action.request),
            switchMap((request) =>
                from(this.flowService.updateComponent(request)).pipe(
                    map((response) => {
                        const updateComponentResponse: UpdateComponentResponse = {
                            id: request.id,
                            type: request.type,
                            response: response
                        };
                        return FlowActions.updateComponentSuccess({ response: updateComponentResponse });
                    }),
                    catchError((error) => {
                        const updateComponentFailure: UpdateComponentFailure = {
                            id: request.id,
                            type: request.type,
                            restoreOnFailure: request.restoreOnFailure,
                            error: error
                        };
                        return of(FlowActions.updateComponentFailure({ response: updateComponentFailure }));
                    })
                )
            )
        )
    );

    updatePositions$ = createEffect(() =>
        this.actions$.pipe(
            ofType(FlowActions.updatePosition),
            map((action) => action.positionUpdate),
            switchMap((positionUpdate) =>
                from(this.flowService.updateComponentPosition(positionUpdate)).pipe(
                    map((updatePositionResponse) => {
                        const response: UpdateComponentPositionResponse = {
                            id: positionUpdate.id,
                            type: positionUpdate.type,
                            response: updatePositionResponse
                        };
                        return FlowActions.updatePositionSuccess({ positionUpdateResponse: response });
                    }),
                    catchError((error) => of(FlowActions.flowApiError({ error })))
                )
            )
        )
    );

    updatePositionsSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(FlowActions.updatePositionSuccess),
                switchMap((positionUpdates) => {
                    // TODO - refresh connections
                    return of(positionUpdates);
                })
            ),
        { dispatch: false }
    );
}
