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
import { HttpErrorResponse, HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from '@angular/common/http';
import { Observable, tap } from 'rxjs';
import { AuthStorage } from '../auth-storage.service';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../state';
import { fullScreenError } from '../../state/error/error.actions';
import { NiFiCommon } from '../nifi-common.service';

@Injectable({
    providedIn: 'root'
})
export class AuthInterceptor implements HttpInterceptor {
    routedToFullScreenError = false;

    constructor(
        private authStorage: AuthStorage,
        private store: Store<NiFiState>,
        private nifiCommon: NiFiCommon
    ) {}

    intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
        return next.handle(request).pipe(
            tap({
                error: (errorResponse) => {
                    if (errorResponse instanceof HttpErrorResponse) {
                        if (errorResponse.status === 401) {
                            if (this.authStorage.hasToken()) {
                                this.routedToFullScreenError = true;

                                this.authStorage.removeToken();

                                let message: string = errorResponse.error;
                                if (this.nifiCommon.isBlank(message)) {
                                    message = 'Your session has expired. Please navigate home to log in again.';
                                } else {
                                    message += '. Please navigate home to log in again.';
                                }

                                this.store.dispatch(
                                    fullScreenError({
                                        errorDetail: {
                                            title: 'Unauthorized',
                                            message
                                        }
                                    })
                                );
                            } else if (!this.routedToFullScreenError) {
                                // the user has never logged in, redirect them to do so
                                window.location.href = './login';
                            }
                        }
                    }
                }
            })
        );
    }
}
