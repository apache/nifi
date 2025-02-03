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

import { Component, OnDestroy, SecurityContext } from '@angular/core';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { selectRef } from '../../state/content/content.selectors';
import { isDefinedAndNotNull } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { HttpParams } from '@angular/common/http';
import { selectExternalViewerRequest } from '../../state/external-viewer/external-viewer.selectors';
import { ExternalViewerRequest } from '../../state/external-viewer';
import { resetExternalViewerRequest } from '../../state/external-viewer/external-viewer.actions';
import { RecreateViewDirective } from '../recreate-view.directive';

@Component({
    selector: 'external-viewer',
    templateUrl: './external-viewer.component.html',
    imports: [RecreateViewDirective],
    styleUrls: ['./external-viewer.component.scss']
})
export class ExternalViewer implements OnDestroy {
    private ref: string | null = null;
    private request: ExternalViewerRequest | null = null;

    frameSource: SafeResourceUrl | null = null;

    constructor(
        private store: Store<NiFiState>,
        private domSanitizer: DomSanitizer
    ) {
        this.store
            .select(selectRef)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((ref) => {
                this.ref = ref;
                this.frameSource = this.getFrameSource();
            });

        this.store
            .select(selectExternalViewerRequest)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((request) => {
                this.request = request;
                this.frameSource = this.getFrameSource();
            });
    }

    private getFrameSource(): SafeResourceUrl | null {
        if (this.ref && this.request) {
            let queryParams: HttpParams = new HttpParams()
                .set('ref', this.ref)
                .set('mimeTypeDisplayName', this.request.mimeTypeDisplayName);

            if (this.request.clientId) {
                queryParams = queryParams.set('clientId', this.request.clientId);
            }

            const urlWithParams = `${this.request.url}/?${queryParams.toString()}`;

            const sanitizedUrl = this.domSanitizer.sanitize(SecurityContext.URL, urlWithParams);

            if (sanitizedUrl) {
                return this.domSanitizer.bypassSecurityTrustResourceUrl(sanitizedUrl);
            }
        }

        return null;
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetExternalViewerRequest());
    }
}
