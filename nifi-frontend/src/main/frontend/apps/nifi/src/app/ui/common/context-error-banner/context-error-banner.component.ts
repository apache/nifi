/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Component, DestroyRef, inject, Input, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NiFiState } from '../../../state';
import { Store } from '@ngrx/store';
import { clearBannerErrors } from '../../../state/error/error.actions';
import { Observable } from 'rxjs';
import { selectBannerErrors } from '../../../state/error/error.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ErrorBanner } from '../error-banner/error-banner.component';
import { ErrorContextKey } from '../../../state/error';

@Component({
    selector: 'context-error-banner',
    imports: [CommonModule, ErrorBanner],
    templateUrl: './context-error-banner.component.html',
    styleUrl: './context-error-banner.component.scss'
})
export class ContextErrorBanner implements OnDestroy {
    private _context!: ErrorContextKey;
    @Input({ required: true }) set context(context: ErrorContextKey) {
        this._context = context;
        this.messages$ = this.store.select(selectBannerErrors(this._context)).pipe(takeUntilDestroyed(this.destroyRef));
    }

    get context() {
        return this._context;
    }

    messages$: Observable<string[]> | null = null;
    private destroyRef: DestroyRef = inject(DestroyRef);

    constructor(private store: Store<NiFiState>) {}

    ngOnDestroy(): void {
        this.store.dispatch(clearBannerErrors({ context: this.context }));
    }

    dismiss() {
        this.store.dispatch(clearBannerErrors({ context: this.context }));
    }
}
