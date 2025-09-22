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
import { ThemingService, Storage } from '@nifi/shared';

@Component({
    selector: 'nifi-registry-app-root',
    templateUrl: './app.component.html',
    styleUrl: './app.component.scss',
    standalone: false
})
export class AppComponent {
    title = 'nifi-registry';

    constructor(
        private storage: Storage,
        private themingService: ThemingService
    ) {
        let theme = this.storage.getItem('theme');

        // Initially check if dark mode is enabled on system
        const darkModeOn = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;

        // If dark mode is enabled then directly switch to the dark-theme
        this.themingService.toggleTheme(darkModeOn, theme);

        if (window.matchMedia) {
            // Watch for changes of the preference
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
                theme = this.storage.getItem('theme');
                this.themingService.toggleTheme(e.matches, theme);
            });
        }
    }
}
