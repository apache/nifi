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

import { Component, Inject } from '@angular/core';
import { GuardsCheckEnd, GuardsCheckStart, NavigationCancel, Router } from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { DOCUMENT } from '@angular/common';
import { Storage } from './service/storage.service';

@Component({
    selector: 'nifi',
    templateUrl: './app.component.html',
    styleUrls: ['./app.component.scss']
})
export class AppComponent {
    title = 'nifi';
    guardLoading = true;

    constructor(
        private router: Router,
        private storage: Storage,
        @Inject(DOCUMENT) private _document: Document
    ) {
        this.router.events.pipe(takeUntilDestroyed()).subscribe((event) => {
            if (event instanceof GuardsCheckStart) {
                this.guardLoading = true;
            }
            if (event instanceof GuardsCheckEnd || event instanceof NavigationCancel) {
                this.guardLoading = false;
            }
        });

        let overrideTheme = this.storage.getItem('override-theme');

        // Initially check if dark mode is enabled on system
        const darkModeOn = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;

        // If dark mode is enabled then directly switch to the dark-theme
        if (darkModeOn) {
            this._document.body.classList.toggle('dark-theme', !overrideTheme);
        } else if (!darkModeOn) {
            this._document.body.classList.toggle('dark-theme', overrideTheme);
        }

        // Watch for changes of the preference
        window.matchMedia('(prefers-color-scheme: dark)').addListener((e) => {
            overrideTheme = this.storage.getItem('override-theme');
            const newColorScheme = e.matches ? 'dark' : 'light';
            if (newColorScheme === 'dark' && overrideTheme) {
                this._document.body.classList.toggle('dark-theme', false);
            } else if (newColorScheme === 'dark' && !overrideTheme) {
                this._document.body.classList.toggle('dark-theme', true);
            } else if (newColorScheme === 'light' && overrideTheme) {
                this._document.body.classList.toggle('dark-theme', true);
            } else {
                this._document.body.classList.toggle('dark-theme', false);
            }
        });
    }
}
