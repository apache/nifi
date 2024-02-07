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

import { Inject, Injectable } from '@angular/core';
import { DOCUMENT } from '@angular/common';

export const DARK_THEME = 'DARK_THEME';
export const LIGHT_THEME = 'LIGHT_THEME';
export const OS_SETTING = 'OS_SETTING';

@Injectable({ providedIn: 'root' })
export class ThemingService {
    constructor(@Inject(DOCUMENT) private _document: Document) {}

    toggleTheme(darkModeOn: boolean, theme: any) {
        if (darkModeOn) {
            if (theme === DARK_THEME) {
                this._document.body.classList.toggle('dark-theme', true);
            } else if (theme === LIGHT_THEME) {
                this._document.body.classList.toggle('dark-theme', false);
            } else {
                this._document.body.classList.toggle('dark-theme', true);
            }
        } else {
            if (theme === DARK_THEME) {
                this._document.body.classList.toggle('dark-theme', true);
            } else if (theme === LIGHT_THEME) {
                this._document.body.classList.toggle('dark-theme', false);
            } else {
                this._document.body.classList.toggle('dark-theme', false);
            }
        }
    }
}
