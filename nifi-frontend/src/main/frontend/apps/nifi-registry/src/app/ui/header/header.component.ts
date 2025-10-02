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

import { Component, inject, OnInit } from '@angular/core';
import { AsyncPipe, NgOptimizedImage } from '@angular/common';
import { Router, RouterModule } from '@angular/router';
import { MatButton } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';
import { DARK_THEME, LIGHT_THEME, OS_SETTING, Storage, ThemingService } from '@nifi/shared';
import { Store } from '@ngrx/store';
import { NiFiRegistryState } from '../../state';
import { selectCurrentUser, selectLogoutSupported } from '../../state/current-user/current-user.selectors';
import { logout } from '../../state/current-user/current-user.actions';
import { CurrentUser } from '../../state/current-user';
import { loadAbout, openAboutDialog } from '../../state/about/about.actions';
import { selectAbout } from '../../state/about/about.selectors';

@Component({
    selector: 'app-header',
    standalone: true,
    imports: [NgOptimizedImage, RouterModule, MatButton, MatMenu, MatMenuItem, MatMenuTrigger, AsyncPipe],
    templateUrl: './header.component.html',
    styleUrl: './header.component.scss'
})
export class HeaderComponent implements OnInit {
    private storage = inject(Storage);
    private themingService = inject(ThemingService);
    private router = inject(Router);
    private store = inject<Store<NiFiRegistryState>>(Store);

    readonly documentationUrl = 'https://nifi.apache.org/docs/nifi-registry-docs/index.html';

    theme: any | undefined;
    darkModeOn: boolean | undefined;
    LIGHT_THEME: string = LIGHT_THEME;
    DARK_THEME: string = DARK_THEME;
    OS_SETTING: string = OS_SETTING;
    disableAnimations: string | null;
    currentUser = this.store.selectSignal(selectCurrentUser);
    logoutSupported = this.store.selectSignal(selectLogoutSupported);
    about$ = this.store.select(selectAbout);

    constructor() {
        this.darkModeOn = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
        this.theme = this.storage.getItem('theme') ? this.storage.getItem('theme') : OS_SETTING;
        this.disableAnimations = this.storage.getItem('disable-animations');

        if (window.matchMedia) {
            // Watch for changes of the preference
            window.matchMedia('(prefers-color-scheme: dark)').addListener((e) => {
                this.darkModeOn = e.matches;
                this.theme = this.storage.getItem('theme') ? this.storage.getItem('theme') : OS_SETTING;
            });
        }
    }

    ngOnInit(): void {
        this.store.dispatch(loadAbout());
    }

    navigateToResources() {
        this.router.navigateByUrl('/explorer');
    }

    navigateToWorkflow() {
        this.router.navigateByUrl('/buckets');
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

    viewAbout(): void {
        this.store.dispatch(openAboutDialog());
    }

    allowLogin(user: CurrentUser | null): boolean {
        if (!user) {
            return false;
        }

        return user.anonymous && user.loginSupported;
    }

    login(): void {
        this.router.navigateByUrl('/login');
    }

    logout(): void {
        this.store.dispatch(logout());
    }
}
