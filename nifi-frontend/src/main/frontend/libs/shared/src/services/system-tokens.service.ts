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

import { Injectable, DOCUMENT, inject } from '@angular/core';

@Injectable({ providedIn: 'root' })
export class SystemTokensService {
    private _document = inject<Document>(DOCUMENT);

    appendStyleSheet(iframe: any): void {
        // Ensure the iframe's contentDocument is available
        if (iframe.contentDocument) {
            const iframeDoc = iframe.contentDocument;

            // Extract all CSS rules from the first stylesheet
            const allCSS = this.extractAllStyles();

            // Append CSS variables as inline <style> to the end of the iframe's <head>
            const styleElement = iframeDoc.createElement('style');
            styleElement.appendChild(iframeDoc.createTextNode(`${allCSS}`));

            // Append to the end of the <head> to ensure it overrides existing styles
            iframeDoc.head.appendChild(styleElement);
        }
    }

    extractAllStyles(): string {
        let cssText = '';

        // Iterate over all stylesheets in the document
        for (let i = 0; i < this._document.styleSheets.length; i++) {
            const styleSheet = this._document.styleSheets[i] as CSSStyleSheet;

            // Ensure we can access rules
            if (styleSheet.cssRules) {
                for (let j = 0; j < styleSheet.cssRules.length; j++) {
                    const rule = styleSheet.cssRules[j] as CSSRule;
                    cssText += rule.cssText + ' ';
                }
            }
        }

        return cssText;
    }
}
