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
import { EffectsModule } from '@ngrx/effects';
import { StoreDevtoolsModule } from '@ngrx/store-devtools';
import { environment } from '../environments/environment';
import { provideHttpClient, withInterceptors, withXsrfConfiguration } from '@angular/common/http';
import { NavigationActionTiming, RouterState, StoreRouterConnectingModule } from '@ngrx/router-store';
import { rootReducers } from './state';
import { CurrentUserEffects } from './state/current-user/current-user.effects';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { authInterceptor } from './service/interceptors/auth.interceptor';
import { ExtensionTypesEffects } from './state/extension-types/extension-types.effects';
import { pollingInterceptor } from './service/interceptors/polling.interceptor';
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
import { CopyButtonComponent } from '@nifi/shared';
import { DocumentationEffects } from './state/documentation/documentation.effects';
import { ClusterSummaryEffects } from './state/cluster-summary/cluster-summary.effects';
import { PropertyVerificationEffects } from './state/property-verification/property-verification.effects';
import { loadingInterceptor } from './service/interceptors/loading.interceptor';
import { LoginConfigurationEffects } from './state/login-configuration/login-configuration.effects';
import { BannerTextEffects } from './state/banner-text/banner-text.effects';
import { MAT_TOOLTIP_DEFAULT_OPTIONS, MatTooltipDefaultOptions } from '@angular/material/tooltip';
import { CLIPBOARD_OPTIONS, provideMarkdown } from 'ngx-markdown';
import { CopyEffects } from './state/copy/copy.effects';

const entry = localStorage.getItem('disable-animations');
let disableAnimations: string = entry !== null ? JSON.parse(entry).item : '';

// honor OS settings if user has not explicitly disabled animations for the application
if (disableAnimations !== 'true' && disableAnimations !== 'false') {
    disableAnimations = window.matchMedia('(prefers-reduced-motion: reduce)').matches.toString();
}

export const customTooltipDefaults: MatTooltipDefaultOptions = {
    showDelay: 2000,
    hideDelay: 0,
    touchendHideDelay: 1000
};

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
        EffectsModule.forRoot(
            ErrorEffects,
            CurrentUserEffects,
            ExtensionTypesEffects,
            AboutEffects,
            BannerTextEffects,
            FlowConfigurationEffects,
            LoginConfigurationEffects,
            StatusHistoryEffects,
            ControllerServiceStateEffects,
            SystemDiagnosticsEffects,
            ComponentStateEffects,
            DocumentationEffects,
            ClusterSummaryEffects,
            PropertyVerificationEffects,
            CopyEffects
        ),
        StoreDevtoolsModule.instrument({
            maxAge: 25,
            logOnly: environment.production,
            autoPause: true
        }),
        MatProgressSpinnerModule,
        MatNativeDateModule,
        MatDialogModule,
        MatSnackBarModule
    ],
    providers: [
        disableAnimations === 'true' ? provideNoopAnimations() : provideAnimations(),
        { provide: MAT_FORM_FIELD_DEFAULT_OPTIONS, useValue: { appearance: 'outline' } },
        provideHttpClient(
            withInterceptors([authInterceptor, loadingInterceptor, pollingInterceptor]),
            withXsrfConfiguration({
                cookieName: '__Secure-Request-Token',
                headerName: 'Request-Token'
            })
        ),
        { provide: MAT_TOOLTIP_DEFAULT_OPTIONS, useValue: customTooltipDefaults },
        provideMarkdown({
            clipboardOptions: {
                provide: CLIPBOARD_OPTIONS,
                useValue: {
                    buttonComponent: CopyButtonComponent
                }
            }
        })
    ]
})
export class AppModule {}
