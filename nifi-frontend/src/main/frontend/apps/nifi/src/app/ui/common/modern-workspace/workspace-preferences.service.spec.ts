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
import { Storage } from '@nifi/shared';
import { WorkspacePreferencesService } from './workspace-preferences.service';

describe('Workspace browser preference', () => {
    let saved: string | null;
    let storage: {
        getItem: ReturnType<typeof vi.fn>;
        setItem: ReturnType<typeof vi.fn>;
        removeItem: ReturnType<typeof vi.fn>;
    };
    beforeEach(() => {
        saved = null;
        storage = {
            getItem: vi.fn(() => saved),
            setItem: vi.fn((_key, value) => {
                saved = value;
            }),
            removeItem: vi.fn(() => {
                saved = null;
            })
        };
        TestBed.configureTestingModule({ providers: [{ provide: Storage, useValue: storage }] });
    });
    const create = () => TestBed.runInInjectionContext(() => new WorkspacePreferencesService());
    it('defaults to classic and ignores unrecognized stored formats', () => {
        expect(create().enabled()).toBe(false);
        saved = 'true';
        expect(create().enabled()).toBe(false);
    });
    it('restores the explicit preference after a new instance and clears it on classic return', () => {
        const first = create();
        first.toggle();
        const reloaded = create();
        expect(reloaded.enabled()).toBe(true);
        reloaded.toggle();
        expect(create().enabled()).toBe(false);
    });
    it('keeps the local toggle usable when storage read/write is unavailable', () => {
        storage.getItem.mockImplementation(() => {
            throw new Error('storage unavailable');
        });
        storage.setItem.mockImplementation(() => {
            throw new Error('storage unavailable');
        });
        const preference = create();
        expect(preference.enabled()).toBe(false);
        expect(() => preference.toggle()).not.toThrow();
        expect(preference.enabled()).toBe(true);
    });
});
