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

import { Component } from '@angular/core';
import { CommonModule, NgOptimizedImage } from '@angular/common';
import { Router, RouterModule } from '@angular/router';
import { MatButton } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';
import { DARK_THEME, LIGHT_THEME, OS_SETTING, Storage, ThemingService } from '@nifi/shared';

@Component({
    selector: 'app-header',
    standalone: true,
    imports: [CommonModule, NgOptimizedImage, RouterModule, MatButton, MatMenu, MatMenuItem, MatMenuTrigger],
    templateUrl: './header.component.html',
    styleUrl: './header.component.scss'
})
export class HeaderComponent {
    theme: any | undefined;
    darkModeOn: boolean | undefined;
    LIGHT_THEME: string = LIGHT_THEME;
    DARK_THEME: string = DARK_THEME;
    OS_SETTING: string = OS_SETTING;
    disableAnimations: string | null;

    constructor(
        private storage: Storage,
        private themingService: ThemingService,
        private router: Router
    ) {
        this.darkModeOn = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
        this.theme = this.storage.getItem('theme');
        this.disableAnimations = this.storage.getItem('disable-animations');

        if (window.matchMedia) {
            // Watch for changes of the preference
            window.matchMedia('(prefers-color-scheme: dark)').addListener((e) => {
                this.darkModeOn = e.matches;
                this.theme = this.storage.getItem('theme');
            });
        }
    }

    goHome() {
        this.router.navigateByUrl('/resources');
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
}
