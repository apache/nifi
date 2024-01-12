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
import { ComponentType } from '../../../../../state/shared';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../state';
import {
    selectClusterSummary,
    selectControllerBulletins,
    selectControllerStatus,
    selectCurrentProcessGroupId,
    selectLastRefreshed
} from '../../../state/flow/flow.selectors';
import { selectCurrentUser } from '../../../../../state/current-user/current-user.selectors';
import { CurrentUser } from '../../../../../state/current-user';
import { AuthStorage } from '../../../../../service/auth-storage.service';
import { AuthService } from '../../../../../service/auth.service';
import { LoadingService } from '../../../../../service/loading.service';
import { NewCanvasItem } from './new-canvas-item/new-canvas-item.component';
import { MatButtonModule } from '@angular/material/button';
import { MatMenuModule } from '@angular/material/menu';
import { AsyncPipe, NgIf, NgOptimizedImage } from '@angular/common';
import { MatDividerModule } from '@angular/material/divider';
import { RouterLink } from '@angular/router';
import { FlowStatus } from './flow-status/flow-status.component';
import { getNodeStatusHistoryAndOpenDialog } from '../../../../../state/status-history/status-history.actions';
import { getSystemDiagnosticsAndOpenDialog } from '../../../../../state/system-diagnostics/system-diagnostics.actions';
import { selectFlowConfiguration } from '../../../../../state/flow-configuration/flow-configuration.selectors';

@Component({
    selector: 'fd-header',
    standalone: true,
    templateUrl: './header.component.html',
    imports: [
        NewCanvasItem,
        MatButtonModule,
        MatMenuModule,
        AsyncPipe,
        MatDividerModule,
        RouterLink,
        NgIf,
        FlowStatus,
        NgOptimizedImage
    ],
    styleUrls: ['./header.component.scss']
})
export class HeaderComponent {
    protected readonly ComponentType = ComponentType;

    controllerStatus$ = this.store.select(selectControllerStatus);
    lastRefreshed$ = this.store.select(selectLastRefreshed);
    clusterSummary$ = this.store.select(selectClusterSummary);
    controllerBulletins$ = this.store.select(selectControllerBulletins);
    currentUser$ = this.store.select(selectCurrentUser);
    flowConfiguration$ = this.store.select(selectFlowConfiguration);
    currentProcessGroupId$ = this.store.select(selectCurrentProcessGroupId);

    constructor(
        private store: Store<CanvasState>,
        private authStorage: AuthStorage,
        private authService: AuthService,
        public loadingService: LoadingService
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
}
