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

import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import {
    BrowserAnimationsModule,
    provideAnimations,
    provideNoopAnimations
} from '@angular/platform-browser/animations';
import { StoreModule } from '@ngrx/store';
import { StoreDevtoolsModule } from '@ngrx/store-devtools';
import { environment } from '../environments/environment';
import { provideHttpClient, withInterceptors, withXsrfConfiguration } from '@angular/common/http';
import { NavigationActionTiming, RouterState, StoreRouterConnectingModule } from '@ngrx/router-store';
import { rootReducers } from './state';
import { EffectsModule } from '@ngrx/effects';
import { MAT_FORM_FIELD_DEFAULT_OPTIONS } from '@angular/material/form-field';

const entry = localStorage.getItem('disable-animations');
let disableAnimations: string = entry !== null ? JSON.parse(entry).item : '';

// honor OS settings if user has not explicitly disabled animations for the application
if (disableAnimations !== 'true' && disableAnimations !== 'false') {
    disableAnimations = window.matchMedia('(prefers-reduced-motion: reduce)').matches.toString();
}

@NgModule({
    declarations: [AppComponent],
    bootstrap: [AppComponent],
    imports: [
        BrowserModule,
        AppRoutingModule,
        BrowserAnimationsModule,
        StoreModule.forRoot(rootReducers),
        StoreRouterConnectingModule.forRoot({
            routerState: RouterState.Minimal,
            navigationActionTiming: NavigationActionTiming.PostActivation
        }),
        EffectsModule.forRoot(),
        StoreDevtoolsModule.instrument({
            maxAge: 25,
            logOnly: environment.production,
            autoPause: true
        })
    ],
    providers: [
        disableAnimations === 'true' ? provideNoopAnimations() : provideAnimations(),
        { provide: MAT_FORM_FIELD_DEFAULT_OPTIONS, useValue: { appearance: 'outline' } },
        provideHttpClient(
            withInterceptors([]),
            withXsrfConfiguration({
                cookieName: '__Secure-Request-Token',
                headerName: 'Request-Token'
            })
        )
    ]
})
export class AppModule {}
