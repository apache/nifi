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
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { StoreDevtoolsModule } from '@ngrx/store-devtools';
import { environment } from './environments/environment';
import { HTTP_INTERCEPTORS, HttpClientModule, HttpClientXsrfModule } from '@angular/common/http';
import { NavigationActionTiming, RouterState, StoreRouterConnectingModule } from '@ngrx/router-store';
import { rootReducers } from './state';
import { CurrentUserEffects } from './state/current-user/current-user.effects';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { LoadingInterceptor } from './service/interceptors/loading.interceptor';
import { AuthInterceptor } from './service/interceptors/auth.interceptor';
import { ExtensionTypesEffects } from './state/extension-types/extension-types.effects';
import { PollingInterceptor } from './service/interceptors/polling.interceptor';
import { MAT_FORM_FIELD_DEFAULT_OPTIONS } from '@angular/material/form-field';
import { MatNativeDateModule } from '@angular/material/core';
import { AboutEffects } from './state/about/about.effects';
import { StatusHistoryEffects } from './state/status-history/status-history.effects';
import { MatDialogModule } from '@angular/material/dialog';
import { ControllerServiceStateEffects } from './state/contoller-service-state/controller-service-state.effects';
import { SystemDiagnosticsEffects } from './state/system-diagnostics/system-diagnostics.effects';
import { FlowConfigurationEffects } from './state/flow-configuration/flow-configuration.effects';
import { ComponentStateEffects } from './state/component-state/component-state.effects';
import { ErrorEffects } from './state/error/error.effects';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { PipesModule } from './pipes/pipes.module';
import { DocumentationEffects } from './state/documentation/documentation.effects';
import { ClusterSummaryEffects } from './state/cluster-summary/cluster-summary.effects';

@NgModule({
    declarations: [AppComponent],
    imports: [
        BrowserModule,
        AppRoutingModule,
        BrowserAnimationsModule,
        HttpClientModule,
        HttpClientXsrfModule.withOptions({
            cookieName: '__Secure-Request-Token',
            headerName: 'Request-Token'
        }),
        StoreModule.forRoot(rootReducers),
        StoreRouterConnectingModule.forRoot({
            routerState: RouterState.Minimal,
            navigationActionTiming: NavigationActionTiming.PostActivation
        }),
        EffectsModule.forRoot(
            ErrorEffects,
            CurrentUserEffects,
            ExtensionTypesEffects,
            AboutEffects,
            FlowConfigurationEffects,
            StatusHistoryEffects,
            ControllerServiceStateEffects,
            SystemDiagnosticsEffects,
            ComponentStateEffects,
            DocumentationEffects,
            ClusterSummaryEffects
        ),
        StoreDevtoolsModule.instrument({
            maxAge: 25,
            logOnly: environment.production,
            autoPause: true
        }),
        MatProgressSpinnerModule,
        MatNativeDateModule,
        MatDialogModule,
        MatSnackBarModule,
        PipesModule
    ],
    providers: [
        {
            provide: HTTP_INTERCEPTORS,
            useClass: LoadingInterceptor,
            multi: true
        },
        {
            provide: HTTP_INTERCEPTORS,
            useClass: AuthInterceptor,
            multi: true
        },
        {
            provide: HTTP_INTERCEPTORS,
            useClass: PollingInterceptor,
            multi: true
        },
        { provide: MAT_FORM_FIELD_DEFAULT_OPTIONS, useValue: { appearance: 'outline' } }
    ],
    bootstrap: [AppComponent]
})
export class AppModule {}
