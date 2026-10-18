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

import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ComponentType } from '@nifi/shared';
import { TestBed } from '@angular/core/testing';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { ComponentType } from '@nifi/shared';
import { OperationalOverviewComponent } from './operational-overview.component';
import { OperationalInspectorComponent } from './operational-inspector.component';
import {
    OperationalOverview,
    selectOperationalOverview,
    selectOperationalSelection
} from '../../../state/operational-overview/operational-overview.selectors';
import { selectClusterSummary } from '../../../../../state/cluster-summary/cluster-summary.selectors';
import {
    navigateToQueueListing,
    navigateToViewStatusHistoryForComponent,
    selectComponents
} from '../../../state/flow/flow.actions';

const overview: OperationalOverview = {
    status: 'ready',
    name: 'Test group',
    lastRefreshed: '14:00:00 ICT',
    refreshing: false,
    processors: { running: 1, stopped: 1, invalid: 1, disabled: 1, unknown: 0 },
    visibleProcessors: 4,
    visibleConnections: 1,
    restricted: false,
    queuedFlowFiles: 5,
    queuedBytes: 235,
    measuredConnections: 1,
    highestUtilization: { id: 'queue', name: 'Test queue', percent: 100 },
    controllerBulletins: []
};

describe('Operational overview', () => {
    let store: MockStore;
    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [OperationalOverviewComponent],
            providers: [
                provideMockStore({
                    selectors: [
                        { selector: selectOperationalOverview, value: overview },
                        { selector: selectClusterSummary, value: null }
                    ]
                })
            ]
        });
        store = TestBed.inject(MockStore);
    });
    afterEach(() => store.resetSelectors());

    it('keeps the overview opt-in and emits a local expand event', () => {
        const fixture = TestBed.createComponent(OperationalOverviewComponent);
        const changed = vi.fn();
        fixture.componentInstance.expandedChange.subscribe(changed);
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('article')).toBeNull();
        fixture.nativeElement.querySelector('button[aria-controls]').click();
        expect(changed).toHaveBeenCalledWith(true);
    });

    it('shows unavailable queue data instead of a fabricated zero', () => {
        store.overrideSelector(selectOperationalOverview, { ...overview, queuedFlowFiles: null, queuedBytes: null });
        const fixture = TestBed.createComponent(OperationalOverviewComponent);
        fixture.componentRef.setInput('expanded', true);
        fixture.detectChanges();
        const card = fixture.nativeElement.querySelector('[aria-label="Visible queued data"]');
        expect(card.textContent).toContain('Unavailable');
        expect(card.textContent).toContain('Bytes unavailable');
    });

    it('does not display cached metrics when access becomes forbidden', () => {
        store.overrideSelector(selectOperationalOverview, { ...overview, status: 'forbidden' });
        const fixture = TestBed.createComponent(OperationalOverviewComponent);
        fixture.componentRef.setInput('expanded', true);
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('article')).toBeNull();
        expect(fixture.nativeElement.textContent).not.toContain('Test queue');
        expect(fixture.nativeElement.textContent).toContain('do not have permission');
    });

    it('labels retained data after a refresh failure and hides cards on initial failure', () => {
        const selector = store.overrideSelector(selectOperationalOverview, { ...overview, refreshFailed: true });
        const fixture = TestBed.createComponent(OperationalOverviewComponent);
        fixture.componentRef.setInput('expanded', true);
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('[role="alert"]').textContent).toContain('last successful snapshot');
        expect(fixture.nativeElement.querySelector('article')).not.toBeNull();
        selector.setResult({ ...overview, status: 'error' });
        store.refreshState();
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('[role="alert"]').textContent).toContain('Unable to load');
        expect(fixture.nativeElement.querySelector('article')).toBeNull();
    });

    it('inspects a connection through existing selection actions rather than HTTP writes', () => {
        const fixture = TestBed.createComponent(OperationalOverviewComponent);
        const dispatch = vi.spyOn(store, 'dispatch');
        fixture.componentInstance.inspectConnection('queue');
        expect(dispatch).toHaveBeenCalledExactlyOnceWith(
            selectComponents({
                request: {
                    components: [{ id: 'queue', componentType: ComponentType.Connection }]
                }
            })
        );
    });
});

describe('Operational inspector', () => {
    let store: MockStore;
    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [OperationalInspectorComponent],
            providers: [
                provideHttpClient(),
                provideHttpClientTesting(),
                provideMockStore({
                    selectors: [
                        { selector: selectOperationalSelection, value: null },
                        { selector: selectClusterSummary, value: null }
                    ]
                })
            ]
        });
        store = TestBed.inject(MockStore);
    });
    afterEach(() => store.resetSelectors());

    it('uses the existing listing lifecycle only for the current readable connection', () => {
        const fixture = TestBed.createComponent(OperationalInspectorComponent);
        const selection = { id: 'queue', kind: 'connection' as const, readable: true, name: 'Test queue' };
        fixture.componentRef.setInput('selection', selection);
        store.overrideSelector(selectOperationalSelection, selection);
        store.refreshState();
        const dispatch = vi.spyOn(store, 'dispatch');
        fixture.componentInstance.listQueue();
        expect(dispatch).toHaveBeenCalledExactlyOnceWith(
            navigateToQueueListing({ request: { connectionId: 'queue' } })
        );
        dispatch.mockClear();
        store.overrideSelector(selectOperationalSelection, { ...selection, readable: false });
        store.refreshState();
        fixture.componentInstance.listQueue();
        expect(dispatch).not.toHaveBeenCalled();
        store.overrideSelector(selectOperationalSelection, { ...selection, id: 'different-queue' });
        store.refreshState();
        fixture.componentInstance.listQueue();
        expect(dispatch).not.toHaveBeenCalled();
    });

    it('opens real status history only for the current readable selection', () => {
        const fixture = TestBed.createComponent(OperationalInspectorComponent);
        const selection = { id: 'queue', kind: 'connection' as const, readable: true, name: 'Queue' };
        fixture.componentRef.setInput('selection', selection);
        const selector = store.overrideSelector(selectOperationalSelection, selection);
        store.refreshState();
        const dispatch = vi.spyOn(store, 'dispatch');
        fixture.componentInstance.history();
        expect(dispatch).toHaveBeenCalledExactlyOnceWith(
            navigateToViewStatusHistoryForComponent({ request: { id: 'queue', type: ComponentType.Connection } })
        );
        dispatch.mockClear();
        selector.setResult({ ...selection, id: 'other' });
        store.refreshState();
        fixture.componentInstance.history();
        expect(dispatch).not.toHaveBeenCalled();
        selector.setResult({ ...selection, readable: false });
        store.refreshState();
        fixture.componentInstance.history();
        expect(dispatch).not.toHaveBeenCalled();
    });

    it('redacts every selected field when the component is restricted', () => {
        const fixture = TestBed.createComponent(OperationalInspectorComponent);
        fixture.componentRef.setInput('selection', {
            id: 'secret',
            kind: 'connection',
            readable: false,
            name: 'Sensitive queue name',
            queuedFlowFiles: 9999,
            source: 'Private source'
        });
        fixture.detectChanges();
        expect(fixture.nativeElement.textContent).toContain('restricted');
        expect(fixture.nativeElement.textContent).not.toContain('Sensitive queue name');
        expect(fixture.nativeElement.textContent).not.toContain('9999');
        expect(fixture.nativeElement.textContent).not.toContain('Private source');
    });

    it('distinguishes an explicitly disabled object threshold from absent metrics', () => {
        const fixture = TestBed.createComponent(OperationalInspectorComponent);
        fixture.componentRef.setInput('selection', {
            id: 'queue',
            kind: 'connection',
            readable: true,
            name: 'Test queue',
            objectThreshold: 0
        });
        fixture.detectChanges();
        expect(fixture.nativeElement.textContent).toContain('Disabled (0)');
        expect(fixture.nativeElement.textContent).toContain('Unavailable');
        expect(fixture.nativeElement.querySelector('button[mat-stroked-button]').textContent).toContain('List queue');
    });
});
