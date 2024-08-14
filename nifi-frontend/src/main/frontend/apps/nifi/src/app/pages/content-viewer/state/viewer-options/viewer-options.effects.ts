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
import * as ViewerOptionsActions from './viewer-options.actions';
import { catchError, from, map, of, switchMap } from 'rxjs';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ContentViewerService } from '../../service/content-viewer.service';
import { ContentViewerEntity } from './index';

@Injectable()
export class ViewerOptionsEffects {
    constructor(
        private actions$: Actions,
        private contentViewerService: ContentViewerService,
        private errorHelper: ErrorHelper
    ) {}

    loadContentViewerOptions$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ViewerOptionsActions.loadContentViewerOptions),
            switchMap(() =>
                from(this.contentViewerService.getContentViewers()).pipe(
                    map((response: ContentViewerEntity) =>
                        ViewerOptionsActions.loadContentViewerOptionsSuccess({ response })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(this.errorHelper.fullScreenError(errorResponse, true))
                    )
                )
            )
        )
    );
}
