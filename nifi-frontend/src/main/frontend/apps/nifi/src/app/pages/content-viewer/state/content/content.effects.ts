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
import * as ContentActions from './content.actions';
import { map, tap } from 'rxjs';
import { NavigationExtras, Router } from '@angular/router';
import { NiFiCommon } from '@nifi/shared';

@Injectable()
export class ContentEffects {
    constructor(
        private actions$: Actions,
        private router: Router,
        private nifiCommon: NiFiCommon
    ) {}

    navigateToBundledContentViewer$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ContentActions.navigateToBundledContentViewer),
                map((action) => action.route),
                tap((route) => {
                    const extras: NavigationExtras = {
                        queryParamsHandling: 'preserve',
                        replaceUrl: true
                    };
                    const commands = route.split('/').filter((segment) => !this.nifiCommon.isBlank(segment));
                    this.router.navigate(commands, extras);
                })
            ),
        { dispatch: false }
    );
}
