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
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting, HttpTestingController } from '@angular/common/http/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { QueueNodeStatusComponent } from './queue-node-status.component';
import { selectClusterSummary } from '../../../../../state/cluster-summary/cluster-summary.selectors';
import { OperationalSelection } from '../../../state/operational-overview/operational-overview.selectors';

const selected: OperationalSelection = {
    id: 'queue',
    kind: 'connection',
    readable: true,
    name: 'Test queue',
    objectThreshold: 5,
    dataThreshold: '1 MB'
};
const payload = {
    canRead: true,
    connectionStatus: { id: 'queue', aggregateSnapshot: { flowFilesQueued: 5, bytesQueued: 235 } }
};

describe('queue node status lifecycle', () => {
    let http: HttpTestingController;
    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [QueueNodeStatusComponent],
            providers: [
                provideHttpClient(),
                provideHttpClientTesting(),
                provideMockStore({
                    selectors: [
                        {
                            selector: selectClusterSummary,
                            value: { clustered: false, connectedNodeCount: 0, totalNodeCount: 0 }
                        }
                    ]
                })
            ]
        });
        http = TestBed.inject(HttpTestingController);
    });
    afterEach(() => http.verify());
    function setup() {
        const fixture = TestBed.createComponent(QueueNodeStatusComponent);
        fixture.componentRef.setInput('selection', selected);
        fixture.detectChanges();
        return fixture;
    }
    function request() {
        return http.expectOne(
            (r) => r.url.endsWith('/connections/queue/status') && r.params.get('nodewise') === 'true'
        );
    }
    it('is on demand and retains a snapshot through ordinary flow refreshes', () => {
        const fixture = setup();
        http.expectNone(() => true);
        fixture.componentInstance.load();
        request().flush(payload);
        fixture.detectChanges();
        expect(fixture.nativeElement.textContent).toContain('At or above threshold');
        fixture.componentRef.setInput('selection', { ...selected, percentCount: 100 });
        fixture.detectChanges();
        expect(fixture.componentInstance.status()).toBe('ready');
        http.expectNone(() => true);
    });
    it('cancels an old request on selection/configuration change or destruction', () => {
        const fixture = setup();
        fixture.componentInstance.load();
        const old = request();
        fixture.componentRef.setInput('selection', { ...selected, id: 'new' });
        fixture.detectChanges();
        expect(old.cancelled).toBe(true);
        expect(fixture.componentInstance.status()).toBe('idle');
        fixture.componentInstance.load();
        const next = http.expectOne((r) => r.url.endsWith('/connections/new/status'));
        fixture.destroy();
        expect(next.cancelled).toBe(true);
    });
    it('invalidates a loaded comparison when thresholds or read permission change', () => {
        const fixture = setup();
        fixture.componentInstance.load();
        request().flush(payload);
        fixture.detectChanges();
        fixture.componentRef.setInput('selection', { ...selected, objectThreshold: 10 });
        fixture.detectChanges();
        expect(fixture.componentInstance.response()).toBeNull();
        fixture.componentRef.setInput('selection', { ...selected, readable: false });
        fixture.detectChanges();
        fixture.componentInstance.load();
        http.expectNone(() => true);
    });
    it('clears retained metrics before refresh and redacts a forbidden response', () => {
        const fixture = setup();
        fixture.componentInstance.load();
        request().flush(payload);
        fixture.detectChanges();
        fixture.componentInstance.load();
        expect(fixture.componentInstance.response()).toBeNull();
        request().flush('forbidden', { status: 403, statusText: 'Forbidden' });
        fixture.detectChanges();
        expect(fixture.componentInstance.status()).toBe('forbidden');
        expect(fixture.componentInstance.rows()).toEqual([]);
        expect(fixture.nativeElement.textContent).not.toContain('At or above threshold');
    });
    it('rejects wrong component responses and canRead false without showing values', () => {
        const fixture = setup();
        fixture.componentInstance.load();
        request().flush({ ...payload, connectionStatus: { ...payload.connectionStatus, id: 'different' } });
        expect(fixture.componentInstance.status()).toBe('error');
        expect(fixture.componentInstance.rows()).toEqual([]);
        fixture.componentInstance.load();
        request().flush({ ...payload, canRead: false });
        expect(fixture.componentInstance.status()).toBe('forbidden');
    });
    it('does not duplicate an in-flight request and supports explicit retry', () => {
        const fixture = setup();
        fixture.componentInstance.load();
        fixture.componentInstance.load();
        request().flush('unavailable', { status: 503, statusText: 'Unavailable' });
        expect(fixture.componentInstance.status()).toBe('error');
        fixture.componentInstance.load();
        request().flush(payload);
        expect(fixture.componentInstance.status()).toBe('ready');
    });
});
