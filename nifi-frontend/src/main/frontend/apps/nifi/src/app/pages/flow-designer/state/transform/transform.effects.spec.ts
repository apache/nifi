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
import { provideMockActions } from '@ngrx/effects/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { ReplaySubject } from 'rxjs';
import { Action } from '@ngrx/store';

import { TransformEffects } from './transform.effects';
import * as TransformActions from './transform.actions';
import { selectCurrentProcessGroupId } from '../flow/flow.selectors';
import { Storage } from '@nifi/shared';
import { CanvasView } from '../../service/canvas-view.service';
import { BirdseyeView } from '../../service/birdseye-view.service';
import { LabelManager } from '../../service/manager/label-manager.service';

describe('TransformEffects — restoreViewport$', () => {
    let actions$: ReplaySubject<Action>;
    let effects: TransformEffects;
    let store: MockStore;
    let storageSpy: {
        getItem: ReturnType<typeof vi.fn>;
        removeItem: ReturnType<typeof vi.fn>;
        setItem: ReturnType<typeof vi.fn>;
    };
    let canvasViewSpy: { transform: ReturnType<typeof vi.fn> };
    let dispatchSpy: ReturnType<typeof vi.spyOn>;

    const PROCESS_GROUP_ID = 'root-pg';
    const VIEW_KEY = `nifi-view-${PROCESS_GROUP_ID}`;

    beforeEach(() => {
        actions$ = new ReplaySubject<Action>(1);

        storageSpy = {
            getItem: vi.fn(),
            removeItem: vi.fn(),
            setItem: vi.fn()
        };

        canvasViewSpy = { transform: vi.fn() };

        TestBed.configureTestingModule({
            providers: [
                TransformEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    selectors: [{ selector: selectCurrentProcessGroupId, value: PROCESS_GROUP_ID }]
                }),
                { provide: Storage, useValue: storageSpy },
                { provide: CanvasView, useValue: canvasViewSpy },
                { provide: BirdseyeView, useValue: { refresh: vi.fn() } },
                { provide: LabelManager, useValue: { render: vi.fn() } }
            ]
        });

        effects = TestBed.inject(TransformEffects);
        store = TestBed.inject(MockStore);
        dispatchSpy = vi.spyOn(store, 'dispatch');
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('restores a valid viewport transform from storage', () => {
        const validEntry = { scale: 1, translateX: 100, translateY: 200 };
        storageSpy.getItem.mockReturnValue(validEntry);

        effects.restoreViewport$.subscribe();
        actions$.next(TransformActions.restoreViewport());

        expect(canvasViewSpy.transform).toHaveBeenCalledWith([100, 200], 1);
        expect(storageSpy.removeItem).not.toHaveBeenCalled();
        expect(dispatchSpy).not.toHaveBeenCalledWith(expect.objectContaining({ type: TransformActions.zoomFit.type }));
    });

    it('evicts a catastrophic-finite storage entry and dispatches zoomFit (NIFI-16025 fingerprint)', () => {
        const corruptEntry = { scale: 1, translateX: 7.490388061926315e307, translateY: 0 };
        storageSpy.getItem.mockReturnValue(corruptEntry);

        effects.restoreViewport$.subscribe();
        actions$.next(TransformActions.restoreViewport());

        expect(storageSpy.removeItem).toHaveBeenCalledWith(VIEW_KEY);
        expect(canvasViewSpy.transform).not.toHaveBeenCalled();
        expect(dispatchSpy).toHaveBeenCalledWith(TransformActions.zoomFit({ transition: false }));
    });

    it('evicts an entry with an out-of-range scale (near-zero) and dispatches zoomFit', () => {
        const corruptEntry = { scale: 0.0001, translateX: 0, translateY: 0 };
        storageSpy.getItem.mockReturnValue(corruptEntry);

        effects.restoreViewport$.subscribe();
        actions$.next(TransformActions.restoreViewport());

        expect(storageSpy.removeItem).toHaveBeenCalledWith(VIEW_KEY);
        expect(canvasViewSpy.transform).not.toHaveBeenCalled();
        expect(dispatchSpy).toHaveBeenCalledWith(TransformActions.zoomFit({ transition: false }));
    });

    it('evicts an entry with Infinity and dispatches zoomFit', () => {
        const corruptEntry = { scale: 1, translateX: Infinity, translateY: 0 };
        storageSpy.getItem.mockReturnValue(corruptEntry);

        effects.restoreViewport$.subscribe();
        actions$.next(TransformActions.restoreViewport());

        expect(storageSpy.removeItem).toHaveBeenCalledWith(VIEW_KEY);
        expect(canvasViewSpy.transform).not.toHaveBeenCalled();
    });

    it('dispatches zoomFit (without eviction) when no storage entry exists', () => {
        storageSpy.getItem.mockReturnValue(null);

        effects.restoreViewport$.subscribe();
        actions$.next(TransformActions.restoreViewport());

        expect(storageSpy.removeItem).not.toHaveBeenCalled();
        expect(canvasViewSpy.transform).not.toHaveBeenCalled();
        expect(dispatchSpy).toHaveBeenCalledWith(TransformActions.zoomFit({ transition: false }));
    });
});
