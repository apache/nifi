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
import { AsyncPipe, DOCUMENT, NgIf, NgOptimizedImage } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatDividerModule } from '@angular/material/divider';
import { MatMenuModule } from '@angular/material/menu';
import { getNodeStatusHistoryAndOpenDialog } from '../../../state/status-history/status-history.actions';
import { getSystemDiagnosticsAndOpenDialog } from '../../../state/system-diagnostics/system-diagnostics.actions';
import { Store } from '@ngrx/store';
import { AuthStorage } from '../../../service/auth-storage.service';
import { AuthService } from '../../../service/auth.service';
import { CurrentUser } from '../../../state/current-user';
import { RouterLink } from '@angular/router';
import { selectCurrentUser } from '../../../state/current-user/current-user.selectors';
import { MatButtonModule } from '@angular/material/button';
import { NiFiState } from '../../../state';
import { selectFlowConfiguration } from '../../../state/flow-configuration/flow-configuration.selectors';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { Storage } from '../../../service/storage.service';

@Component({
    selector: 'navigation',
    standalone: true,
    imports: [
        NgOptimizedImage,
        AsyncPipe,
        MatDividerModule,
        MatMenuModule,
        NgIf,
        RouterLink,
        MatButtonModule,
        MatSlideToggleModule,
        FormsModule
    ],
    templateUrl: './navigation.component.html',
    styleUrls: ['./navigation.component.scss']
})
export class Navigation {
    currentUser$ = this.store.select(selectCurrentUser);
    flowConfiguration$ = this.store.select(selectFlowConfiguration);
    isDarkMode = false;

    constructor(
        private store: Store<NiFiState>,
        private authStorage: AuthStorage,
        private authService: AuthService,
        private storage: Storage,
        @Inject(DOCUMENT) private _document: Document
    ) {}

    allowLogin(user: CurrentUser): boolean {
        return user.anonymous && location.protocol === 'https:';
    }

    hasToken(): boolean {
        return this.authStorage.hasToken();
    }

    logout(): void {
        this.authService.logout();
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

    getCanvasLink(): string {
        const canvasRoute = this.storage.getItem('current-canvas-route');
        return canvasRoute || '/';
    }

    toggleTheme(value = !this.isDarkMode) {
        this.isDarkMode = value;
        this._document.body.classList.toggle('dark-theme', value);

        // TODO: save to local storage or read OS settings???
    }
}
