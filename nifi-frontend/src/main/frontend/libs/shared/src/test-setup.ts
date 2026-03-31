/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import '@angular/compiler';
import '@analogjs/vitest-angular/setup-zone';

import { TestBed } from '@angular/core/testing';
import { BrowserDynamicTestingModule, platformBrowserDynamicTesting } from '@angular/platform-browser-dynamic/testing';

try {
    TestBed.initTestEnvironment(BrowserDynamicTestingModule, platformBrowserDynamicTesting(), {
        errorOnUnknownElements: true,
        errorOnUnknownProperties: true
    });
} catch (_e) {
    // Already initialized
}

afterEach(() => {
    TestBed.resetTestingModule();
});

// Mock ResizeObserver (not available in happy-dom)
if (typeof globalThis.ResizeObserver === 'undefined') {
    globalThis.ResizeObserver = class MockResizeObserver implements ResizeObserver {
        observe(_target: Element, _options?: ResizeObserverOptions) {
            // noop
        }
        unobserve(_target: Element) {
            // noop
        }
        disconnect() {
            // noop
        }
    };
}

// Mock MutationObserver (happy-dom's implementation may be incomplete for CodeMirror's DOMObserver)
globalThis.MutationObserver = class MockMutationObserver implements MutationObserver {
    constructor(_callback: MutationCallback) {
        // noop
    }
    observe(_target: Node, _options?: MutationObserverInit) {
        // noop
    }
    disconnect() {
        // noop
    }
    takeRecords(): MutationRecord[] {
        return [];
    }
};

// Mock IntersectionObserver (happy-dom's implementation may be incomplete for CodeMirror's DOMObserver)
globalThis.IntersectionObserver = class MockIntersectionObserver implements IntersectionObserver {
    readonly root: Element | Document | null = null;
    readonly rootMargin: string = '0px';
    readonly thresholds: ReadonlyArray<number> = [0];
    constructor(_callback: IntersectionObserverCallback, _options?: IntersectionObserverInit) {
        // noop
    }
    observe(_target: Element) {
        // noop
    }
    unobserve(_target: Element) {
        // noop
    }
    disconnect() {
        // noop
    }
    takeRecords(): IntersectionObserverEntry[] {
        return [];
    }
};
