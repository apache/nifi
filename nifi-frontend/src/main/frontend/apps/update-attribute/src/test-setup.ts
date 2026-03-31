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

globalThis.ResizeObserver = class ResizeObserver {
    observe() {
        // noop for happy-dom
    }
    unobserve() {
        // noop for happy-dom
    }
    disconnect() {
        // noop for happy-dom
    }
};

globalThis.MutationObserver = class MutationObserver {
    observe() {
        // noop for happy-dom
    }
    disconnect() {
        // noop for happy-dom
    }
    takeRecords() {
        return [];
    }
} as unknown as typeof MutationObserver;

globalThis.IntersectionObserver = class IntersectionObserver {
    readonly root = null;
    readonly rootMargin = '';
    readonly thresholds = [];
    observe() {
        // noop for happy-dom
    }
    unobserve() {
        // noop for happy-dom
    }
    disconnect() {
        // noop for happy-dom
    }
    takeRecords() {
        return [];
    }
} as unknown as typeof IntersectionObserver;
