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

@Injectable()
export class FlowEffects {

  constructor(
    private actions$: Actions,
    private flowService: FlowService
  ) {}

  loadFlow$ = createEffect(() =>
    this.actions$.pipe(
      ofType(FlowActions.loadFlow),
      switchMap(() =>
        from(this.flowService.getFlow()).pipe(
          map((flow) => FlowActions.loadFlowSuccess({ flow: flow })),
          catchError((error) => of(FlowActions.loadFlowFailure({ error })))
        )
      )
    )
  );

  loadFlowSuccess$ = createEffect(() =>
    this.actions$.pipe(
        ofType(FlowActions.loadFlowSuccess),
        switchMap(() => {
            // flow loading is complete, reset the transition flag
            return of(FlowActions.setTransition({ transition: false }));
        })
    )
  );

  updatePositions$ = createEffect(() =>
    this.actions$.pipe(
      ofType(FlowActions.updatePositions),
      switchMap(({ positionUpdates }) =>
        from(this.flowService.updatePositions(positionUpdates)).pipe(
          map((updatePositionResponse) => FlowActions.updatePositionSuccess({ positionUpdates: updatePositionResponse })),
          catchError((error) => of(FlowActions.loadFlowFailure({ error })))
        )
      )
    )
  );

  updatePositionsSuccess$ = createEffect(() =>
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
