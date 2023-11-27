/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { Injectable } from "@angular/core";
import { Actions, createEffect, ofType } from "@ngrx/effects";
import { NiFiState } from "../../../../state";
import { Store } from "@ngrx/store";
import { Router } from "@angular/router";
import * as CounterListingActions from './counter-listing.actions';
import { catchError, from, map, of, switchMap, take, tap } from "rxjs";
import { CountersService } from "../../service/counters.service";
import { MatDialog } from "@angular/material/dialog";
import { YesNoDialog } from "../../../../ui/common/yes-no-dialog/yes-no-dialog.component";

@Injectable()
export class CounterListingEffects {
    constructor(
       private actions$: Actions,
       private store: Store<NiFiState>,
       private router: Router,
       private countersService: CountersService,
       private dialog: MatDialog
    ) {}

    loadCounters$ = createEffect(() =>
        this.actions$.pipe(
            ofType(CounterListingActions.loadCounters),
            switchMap(() =>
                from(this.countersService.getCounters()).pipe(
                    map((response) =>
                        CounterListingActions.loadCountersSuccess({
                            response: {
                                counters: response.counters.aggregateSnapshot.counters,
                                loadedTimestamp: response.counters.aggregateSnapshot.generated
                            }
                        })
                    ),
                    catchError((error) =>
                        of(CounterListingActions.counterListingApiError({
                            error: error.error
                        }))
                    )
                )
            )
        )
    );

    promptCounterReset$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(CounterListingActions.promptCounterReset),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Reset Counter',
                            message: `Reset counter '${request.counter.name}' to default value?`
                        },
                        panelClass: 'small-dialog'
                    });

                    dialogReference.componentInstance.yes.pipe(
                        take(1)
                    ).subscribe(() => {
                        this.store.dispatch(
                            CounterListingActions.resetCounter({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    resetCounter$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(CounterListingActions.resetCounter),
                map((action) => action.request),
                switchMap((request) =>
                    from(this.countersService.resetCounter(request)).pipe(
                        map((response) =>
                            CounterListingActions.resetCounterSuccess({
                                response
                            })
                        ),
                        catchError((error) =>
                            of(CounterListingActions.counterListingApiError({ error: error.error }))
                        )
                    )
                )
            )
        );
}
