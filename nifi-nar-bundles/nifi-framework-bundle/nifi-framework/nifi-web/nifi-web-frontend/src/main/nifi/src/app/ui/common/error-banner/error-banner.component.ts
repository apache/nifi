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
import { Store } from '@ngrx/store';
import { AsyncPipe } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { NiFiState } from '../../../state';
import { selectBannerErrors } from '../../../state/error/error.selectors';
import { clearBannerErrors } from '../../../state/error/error.actions';

@Component({
    selector: 'error-banner',
    standalone: true,
    imports: [MatButtonModule, AsyncPipe],
    templateUrl: './error-banner.component.html',
    styleUrls: ['./error-banner.component.scss']
})
export class ErrorBanner {
    messages$ = this.store.select(selectBannerErrors);

    constructor(private store: Store<NiFiState>) {}

    dismiss(): void {
        this.store.dispatch(clearBannerErrors());
    }
}
