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

import { Component, Input } from '@angular/core';
import { RouterLink } from '@angular/router';
import { selectLogoutSupported } from '../../../state/current-user/current-user.selectors';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../state';
import { setRoutedToFullScreenError } from '../../../state/error/error.actions';
import { logout } from '../../../state/current-user/current-user.actions';

@Component({
    selector: 'page-content',
    templateUrl: './page-content.component.html',
    imports: [RouterLink],
    styleUrls: ['./page-content.component.scss']
})
export class PageContent {
    @Input() title = '';

    logoutSupported = this.store.selectSignal(selectLogoutSupported);

    constructor(private store: Store<NiFiState>) {}

    resetRoutedToFullScreenError(): void {
        this.store.dispatch(setRoutedToFullScreenError({ routedToFullScreenError: false }));
    }

    logout(): void {
        this.store.dispatch(logout());
    }
}
