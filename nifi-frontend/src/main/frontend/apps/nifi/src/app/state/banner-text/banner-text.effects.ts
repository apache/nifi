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
import * as BannerTextActions from './banner-text.actions';
import { filter, from, map, switchMap } from 'rxjs';
import { BannerTextService } from '../../service/banner-text.service';
import { NiFiState } from '../index';
import { Store } from '@ngrx/store';
import { concatLatestFrom } from '@ngrx/operators';
import { selectBannerText } from './banner-text.selectors';

@Injectable()
export class BannerTextEffects {
    constructor(
        private actions$: Actions,
        private bannerTextService: BannerTextService,
        private store: Store<NiFiState>
    ) {}

    loadBannerText$ = createEffect(() =>
        this.actions$.pipe(
            ofType(BannerTextActions.loadBannerText),
            concatLatestFrom(() => this.store.select(selectBannerText)),
            filter(([, bannerText]) => bannerText === null),
            switchMap(() =>
                from(
                    this.bannerTextService.getBannerText().pipe(
                        map((response) =>
                            BannerTextActions.loadBannerTextSuccess({
                                response: {
                                    bannerText: response.banners
                                }
                            })
                        )
                    )
                )
            )
        )
    );
}
