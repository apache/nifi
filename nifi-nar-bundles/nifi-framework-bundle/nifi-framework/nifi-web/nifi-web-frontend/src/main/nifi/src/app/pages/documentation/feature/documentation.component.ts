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
import { NiFiState } from '../../../state';
import { Store } from '@ngrx/store';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { HttpParams } from '@angular/common/http';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { selectDocumentationParameters } from '../../../state/documentation/documentation.selectors';
import { DocumentationParameters } from '../../../state/documentation';
import { clearDocumentationParameters } from '../../../state/documentation/documentation.actions';

@Component({
    selector: 'documentation',
    templateUrl: './documentation.component.html',
    styleUrls: ['./documentation.component.scss']
})
export class Documentation implements OnDestroy {
    frameSource!: SafeResourceUrl | null;

    constructor(
        private store: Store<NiFiState>,
        private domSanitizer: DomSanitizer
    ) {
        this.store
            .select(selectDocumentationParameters)
            .pipe(takeUntilDestroyed())
            .subscribe((params) => {
                this.frameSource = this.getFrameSource(params);
            });
    }

    private getFrameSource(params: DocumentationParameters | null): SafeResourceUrl | null {
        let url = '../nifi-docs/documentation';

        if (params) {
            if (Object.keys(params).length > 0) {
                const queryParams: string = new HttpParams({ fromObject: params }).toString();
                url = `${url}?${queryParams}`;
            }

            const sanitizedUrl = this.domSanitizer.sanitize(SecurityContext.URL, url);

            if (sanitizedUrl) {
                return this.domSanitizer.bypassSecurityTrustResourceUrl(sanitizedUrl);
            }
        }

        return null;
    }

    ngOnDestroy(): void {
        this.store.dispatch(clearDocumentationParameters());
    }
}
