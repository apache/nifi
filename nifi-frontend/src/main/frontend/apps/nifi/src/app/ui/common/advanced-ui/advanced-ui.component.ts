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

import { Component, ElementRef, SecurityContext, ViewChild } from '@angular/core';
import { NiFiState } from '../../../state';
import { Store } from '@ngrx/store';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { HttpParams } from '@angular/common/http';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Navigation } from '../navigation/navigation.component';
import { isDefinedAndNotNull, selectRouteData } from '@nifi/shared';
import { AdvancedUiParams } from '../../../state/shared';
import { selectDisconnectionAcknowledged } from '../../../state/cluster-summary/cluster-summary.selectors';

@Component({
    selector: 'advanced-ui',
    templateUrl: './advanced-ui.component.html',
    imports: [Navigation],
    styleUrls: ['./advanced-ui.component.scss']
})
export class AdvancedUi {
    @ViewChild('iframeRef', { static: false }) iframeRef!: ElementRef<HTMLIFrameElement>;
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

    onIframeLoad(): void {
        if (this.iframeRef?.nativeElement) {
            const iframe = this.iframeRef.nativeElement;

            // Ensure the iframe's contentDocument is available
            if (iframe.contentDocument) {
                const iframeDoc = iframe.contentDocument;

                // Get the CSS variables from the first stylesheet (adjust index if needed)
                const styleSheet = document.styleSheets[0]; // First stylesheet
                let cssVariables: string = '';

                // Extract CSS variables from the :root selector
                if (styleSheet && styleSheet.cssRules) {
                    for (let i = 0; i < styleSheet.cssRules.length; i++) {
                        const rule = styleSheet.cssRules[i] as CSSStyleRule;

                        if (rule.selectorText === ':root') {
                            const rootStyles = rule.style; // Get styles for :root
                            for (let j = 0; j < rootStyles.length; j++) {
                                const propertyName = rootStyles[j];
                                if (propertyName.startsWith('--')) {
                                    const value = rootStyles.getPropertyValue(propertyName).trim();
                                    cssVariables += `${propertyName}: ${value}; `;
                                }
                            }
                        }
                    }
                }

                let darkThemeCssVariables: string = '';

                // Extract CSS variables from the :root selector
                if (styleSheet && styleSheet.cssRules) {
                    for (let i = 0; i < styleSheet.cssRules.length; i++) {
                        const rule = styleSheet.cssRules[i] as CSSStyleRule;

                        // Extract variables from .dark-theme if the parent document has the class
                        if (rule.selectorText === '.dark-theme') {
                            const darkThemeStyles = rule.style;
                            for (let j = 0; j < darkThemeStyles.length; j++) {
                                const propertyName = darkThemeStyles[j];
                                if (propertyName.startsWith('--')) {
                                    const value = darkThemeStyles.getPropertyValue(propertyName).trim();
                                    darkThemeCssVariables += `${propertyName}: ${value}; `;
                                }
                            }
                        }
                    }
                }

                // Append CSS variables as inline <style> to the end of the iframe's <head>
                const styleElement = iframeDoc.createElement('style');
                styleElement.type = 'text/css';
                styleElement.appendChild(
                    iframeDoc.createTextNode(`:root { ${cssVariables} } .dark-theme { ${darkThemeCssVariables} }`)
                );

                // Append to the end of the <head> to ensure it overrides existing styles
                iframeDoc.head.appendChild(styleElement);
            }
        }
    }

    private getFrameSource(params: AdvancedUiParams): SafeResourceUrl | null {
        const queryParams: string = new HttpParams()
            .set('id', params.id)
            .set('revision', params.revision)
            .set('clientId', params.clientId)
            .set('editable', params.editable)
            .set('disconnectedNodeAcknowledged', params.disconnectedNodeAcknowledged)
            .toString();
        const url = `${params.url}/?${queryParams}`;

        const sanitizedUrl = this.domSanitizer.sanitize(SecurityContext.URL, url);

        if (sanitizedUrl) {
            return this.domSanitizer.bypassSecurityTrustResourceUrl(sanitizedUrl);
        }

        return null;
    }
}
