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

import { Component, OnDestroy, OnInit, SecurityContext } from '@angular/core';
import { NiFiState } from '../../../state';
import { Store } from '@ngrx/store';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { HttpParams } from '@angular/common/http';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Navigation } from '../navigation/navigation.component';
import { selectRouteData } from '../../../state/router/router.selectors';
import { AdvancedUiParams, isDefinedAndNotNull } from '../../../state/shared';
import {
    loadClusterSummary,
    startClusterSummaryPolling,
    stopClusterSummaryPolling
} from '../../../state/cluster-summary/cluster-summary.actions';
import { selectDisconnectionAcknowledged } from '../../../state/cluster-summary/cluster-summary.selectors';

@Component({
    selector: 'advanced-ui',
    standalone: true,
    templateUrl: './advanced-ui.component.html',
    imports: [Navigation],
    styleUrls: ['./advanced-ui.component.scss']
})
export class AdvancedUi implements OnInit, OnDestroy {
    frameSource!: SafeResourceUrl | null;

    private params: AdvancedUiParams | null = null;

    constructor(
        private store: Store<NiFiState>,
        private domSanitizer: DomSanitizer
    ) {
        this.store
            .select(selectRouteData)
            .pipe(takeUntilDestroyed(), isDefinedAndNotNull())
            .subscribe((data) => {
                if (data['advancedUiParams']) {
                    // clone the params to handle reloading based on cluster connection state changes
                    this.params = {
                        ...data['advancedUiParams']
                    };
                    this.frameSource = this.getFrameSource(data['advancedUiParams']);
                }
            });

        this.store
            .select(selectDisconnectionAcknowledged)
            .pipe(takeUntilDestroyed())
            .subscribe((disconnectionAcknowledged) => {
                if (this.params) {
                    // limit reloading advanced ui to only when necessary (when the user has acknowledged disconnection)
                    if (
                        disconnectionAcknowledged &&
                        this.params.disconnectedNodeAcknowledged != disconnectionAcknowledged
                    ) {
                        this.params.disconnectedNodeAcknowledged = disconnectionAcknowledged;
                        this.frameSource = this.getFrameSource(this.params);
                    }
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadClusterSummary());
        this.store.dispatch(startClusterSummaryPolling());
    }

    ngOnDestroy(): void {
        this.store.dispatch(stopClusterSummaryPolling());
    }

    private getFrameSource(params: AdvancedUiParams): SafeResourceUrl | null {
        const queryParams: string = new HttpParams()
            .set('id', params.id)
            .set('revision', params.revision)
            .set('clientId', params.clientId)
            .set('editable', params.editable)
            .set('disconnectedNodeAcknowledged', params.disconnectedNodeAcknowledged)
            .toString();
        const url = `${params.url}?${queryParams}`;

        const sanitizedUrl = this.domSanitizer.sanitize(SecurityContext.URL, url);

        if (sanitizedUrl) {
            return this.domSanitizer.bypassSecurityTrustResourceUrl(sanitizedUrl);
        }

        return null;
    }
}
