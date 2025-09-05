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

import { Component, OnDestroy, OnInit } from '@angular/core';
import { NgOptimizedImage } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatDividerModule } from '@angular/material/divider';
import { MatMenuModule } from '@angular/material/menu';
import { getNodeStatusHistoryAndOpenDialog } from '../../../state/status-history/status-history.actions';
import { getSystemDiagnosticsAndOpenDialog } from '../../../state/system-diagnostics/system-diagnostics.actions';
import { Store } from '@ngrx/store';
import { CurrentUser } from '../../../state/current-user';
import { RouterLink } from '@angular/router';
import { selectCurrentUser } from '../../../state/current-user/current-user.selectors';
import { MatButtonModule } from '@angular/material/button';
import { NiFiState } from '../../../state';
import { selectFlowConfiguration } from '../../../state/flow-configuration/flow-configuration.selectors';
import { Storage, DARK_THEME, LIGHT_THEME, OS_SETTING, ThemingService } from '@nifi/shared';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { loadFlowConfiguration } from '../../../state/flow-configuration/flow-configuration.actions';
import {
    logout,
    startCurrentUserPolling,
    stopCurrentUserPolling
} from '../../../state/current-user/current-user.actions';
import { loadAbout, openAboutDialog } from '../../../state/about/about.actions';
import {
    loadClusterSummary,
    startClusterSummaryPolling,
    stopClusterSummaryPolling
} from '../../../state/cluster-summary/cluster-summary.actions';
import { selectClusterSummary } from '../../../state/cluster-summary/cluster-summary.selectors';
import { selectLoginConfiguration } from '../../../state/login-configuration/login-configuration.selectors';
import { selectBackNavigation } from '../../../state/navigation/navigation.selectors';
import { popBackNavigation } from '../../../state/navigation/navigation.actions';

@Component({
    selector: 'navigation',
    providers: [Storage],
    imports: [
        NgOptimizedImage,
        MatDividerModule,
        MatMenuModule,
        RouterLink,
        MatButtonModule,
        FormsModule,
        MatCheckboxModule
    ],
    templateUrl: './navigation.component.html',
    styleUrls: ['./navigation.component.scss']
})
export class Navigation implements OnInit, OnDestroy {
    theme: any | undefined;
    darkModeOn: boolean | undefined;
    LIGHT_THEME: string = LIGHT_THEME;
    DARK_THEME: string = DARK_THEME;
    OS_SETTING: string = OS_SETTING;
    disableAnimations: string | null;

    currentUser = this.store.selectSignal(selectCurrentUser);
    flowConfiguration = this.store.selectSignal(selectFlowConfiguration);
    loginConfiguration = this.store.selectSignal(selectLoginConfiguration);
    clusterSummary = this.store.selectSignal(selectClusterSummary);
    backNavigation = this.store.selectSignal(selectBackNavigation);

    constructor(
        private store: Store<NiFiState>,
        private storage: Storage,
        private themingService: ThemingService
    ) {
        this.darkModeOn = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
        this.theme = this.storage.getItem('theme');
        this.disableAnimations = this.storage.getItem('disable-animations');

        // Watch for changes of the preference
        window.matchMedia('(prefers-color-scheme: dark)').addListener((e) => {
            this.darkModeOn = e.matches;
            this.theme = this.storage.getItem('theme');
        });
    }

    ngOnInit(): void {
        this.store.dispatch(loadAbout());
        this.store.dispatch(loadFlowConfiguration());
        this.store.dispatch(loadClusterSummary());
        this.store.dispatch(startCurrentUserPolling());
        this.store.dispatch(startClusterSummaryPolling());
    }

    ngOnDestroy(): void {
        this.store.dispatch(stopCurrentUserPolling());
        this.store.dispatch(stopClusterSummaryPolling());
    }

    allowLogin(user: CurrentUser): boolean {
        const loginConfig = this.loginConfiguration();
        if (loginConfig) {
            return user.anonymous && loginConfig.loginSupported;
        } else {
            return false;
        }
    }

    logout(): void {
        this.store.dispatch(logout());
    }

    viewNodeStatusHistory(): void {
        this.store.dispatch(
            getNodeStatusHistoryAndOpenDialog({
                request: {
                    source: 'menu'
                }
            })
        );
    }

    viewSystemDiagnostics() {
        this.store.dispatch(
            getSystemDiagnosticsAndOpenDialog({
                request: {
                    nodewise: false
                }
            })
        );
    }

    viewAbout() {
        this.store.dispatch(openAboutDialog());
    }

    getCanvasLink(): string {
        const canvasRoute = this.storage.getItem<string>('current-canvas-route');
        return canvasRoute || '/';
    }

    toggleTheme(theme: string) {
        this.theme = theme;
        this.storage.setItem('theme', theme);
        this.themingService.toggleTheme(!!this.darkModeOn, theme);
    }

    toggleAnimations(disableAnimations: string = '') {
        this.disableAnimations = disableAnimations;
        this.storage.setItem('disable-animations', this.disableAnimations.toString());
        window.location.reload();
    }

    popBackNavigation(): void {
        this.store.dispatch(popBackNavigation());
    }
}
