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
import { ComponentType } from '../../state/shared';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import {
    selectClusterSummary,
    selectControllerBulletins,
    selectControllerStatus,
    selectCurrentProcessGroupId,
    selectLastRefreshed
} from '../../state/flow/flow.selectors';
import { selectUser } from '../../../state/user/user.selectors';
import { User } from '../../../state/user';

@Component({
    selector: 'fd-header',
    templateUrl: './header.component.html',
    styleUrls: ['./header.component.scss']
})
export class HeaderComponent {
    protected readonly ComponentType = ComponentType;

    controllerStatus$ = this.store.select(selectControllerStatus);
    lastRefreshed$ = this.store.select(selectLastRefreshed);
    clusterSummary$ = this.store.select(selectClusterSummary);
    controllerBulletins$ = this.store.select(selectControllerBulletins);
    currentUser$ = this.store.select(selectUser);
    currentProcessGroupId$ = this.store.select(selectCurrentProcessGroupId);

    constructor(private store: Store<CanvasState>) {}

    allowLogin(user: User): boolean {
        return user.anonymous && location.protocol === 'https:';
    }

    allowLogout(user: User): boolean {
        // TODO - once auth service/token storage is availabe we need to update
        return true;
    }
}
