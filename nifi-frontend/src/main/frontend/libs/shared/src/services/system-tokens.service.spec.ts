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

import { TestBed } from '@angular/core/testing';
import { DOCUMENT } from '@angular/common';

import { SystemTokensService } from './system-tokens.service';

describe('SystemTokensService', () => {
    let service: SystemTokensService;
    let mockDocument: Document;
    let mockStyleSheet: Partial<CSSStyleSheet>;

    beforeEach(() => {
        // Create a mock document
        mockDocument = document.implementation.createHTMLDocument();

        TestBed.configureTestingModule({
            providers: [{ provide: DOCUMENT, useValue: mockDocument }]
        });

        service = TestBed.inject(SystemTokensService);

        // Mock a CSSStyleSheet object
        mockStyleSheet = {
            cssRules: [
                { cssText: 'body { background-color: red; }' } as CSSRule,
                { cssText: '.container { display: flex; }' } as CSSRule,
                { cssText: ':root { --primary-color: #ff0000; }' } as CSSRule
            ] as unknown as CSSRuleList
        };

        // Mock `document.styleSheets`
        Object.defineProperty(mockDocument, 'styleSheets', {
            value: [mockStyleSheet],
            writable: true
        });
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });

    it('should append style sheet on iframe load', () => {
        const iframe = document.createElement('iframe');
        document.body.appendChild(iframe);
        iframe.contentDocument!.write('<html><head></head><body></body></html>');
        iframe.contentDocument!.close();

        service.appendStyleSheet(iframe);

        const styleElement = iframe.contentDocument!.head.querySelector('style');
        expect(styleElement).toBeTruthy();
        expect(styleElement!.textContent).toContain(
            'body { background-color: red; } .container { display: flex; } :root { --primary-color: #ff0000; } '
        );
    });

    it('should extract all CSS rules from document stylesheets', () => {
        const cssText = service.extractAllStyles();

        expect(cssText).toContain('body { background-color: red; }');
        expect(cssText).toContain('.container { display: flex; }');
        expect(cssText).toContain(':root { --primary-color: #ff0000; }');
    });

    it('should return an empty string if no stylesheets are present', () => {
        Object.defineProperty(mockDocument, 'styleSheets', { value: [], writable: true });

        const cssText = service.extractAllStyles();

        expect(cssText).toBe('');
    });

    it('should handle stylesheets without cssRules gracefully', () => {
        const emptyStyleSheet: Partial<CSSStyleSheet> = { cssRules: [] as unknown as CSSRuleList };
        Object.defineProperty(mockDocument, 'styleSheets', { value: [emptyStyleSheet], writable: true });

        const cssText = service.extractAllStyles();

        expect(cssText).toBe('');
    });
});
