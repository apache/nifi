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

import { Component, OnInit, Signal } from '@angular/core';
import { CommonModule, NgOptimizedImage } from '@angular/common';
import { Store } from '@ngrx/store';
import { selectCurrentUser } from '../../state/current-user/current-user.selectors';
import { CurrentUser } from '../../state/current-user';
import { loadCurrentUser, startCurrentUserPolling } from '../../state/current-user/current-user.actions';
import { RouterModule } from '@angular/router';

@Component({
    selector: 'app-header',
    standalone: true,
    imports: [CommonModule, NgOptimizedImage, RouterModule],
    templateUrl: './header.component.html',
    styleUrl: './header.component.scss'
})
export class HeaderComponent implements OnInit {
    currentUser: Signal<CurrentUser>;

    constructor(private store: Store) {
        this.currentUser = this.store.selectSignal(selectCurrentUser);
    }

    ngOnInit(): void {
        this.store.dispatch(loadCurrentUser());
        this.store.dispatch(startCurrentUserPolling());
    }
}
