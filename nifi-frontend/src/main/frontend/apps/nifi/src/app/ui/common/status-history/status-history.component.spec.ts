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

import { TestBed } from '@angular/core/testing';

import { StatusHistory } from './status-history.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { initialState as initialStatusHistoryState } from '../../../state/status-history/status-history.reducer';
import {
    StatusHistoryAggregateSnapshot,
    StatusHistoryEntity,
    statusHistoryFeatureKey
} from '../../../state/status-history';
import { initialState as initialErrorState } from '../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../state/error';
import { initialState as initialCurrentUserState } from '../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../state/current-user';
import { ComponentType } from '@nifi/shared';
import { NiFiCommon } from '@nifi/shared';

const now = new Date().getTime();
function minutes(x: number) {
    return x * NiFiCommon.MILLIS_PER_MINUTE;
}

const snapshots: StatusHistoryAggregateSnapshot[] = [
    { timestamp: now - minutes(61), statusMetrics: { freeHeap: 829569792, usedHeap: 244172032 } },
    { timestamp: now - minutes(30), statusMetrics: { freeHeap: 829569792, usedHeap: 244172032 } },
    { timestamp: now - minutes(10), statusMetrics: { freeHeap: 829569792, usedHeap: 244172032 } },
    { timestamp: now - minutes(5), statusMetrics: { freeHeap: 829569792, usedHeap: 244172032 } },
    { timestamp: now, statusMetrics: { freeHeap: 829569792, usedHeap: 244172032 } }
];

describe('StatusHistory', () => {
    const statusHistoryEntity: StatusHistoryEntity = {
        canRead: true,
        statusHistory: {
            generated: 'generated',
            componentDetails: {
                'Group Id': 'local',
                Name: 'local',
                Id: 'id',
                Type: ComponentType.Processor
            },
            fieldDescriptors: [
                {
                    field: 'freeHeap',
                    label: 'Free Heap',
                    description: 'The amount of free memory in the heap that can be used by the Java virtual machine.',
                    formatter: 'DATA_SIZE'
                },
                {
                    field: 'usedHeap',
                    label: 'Used Heap',
                    description: 'The amount of used memory in the heap that is used by the Java virtual machine.',
                    formatter: 'DATA_SIZE'
                }
            ],
            aggregateSnapshots: snapshots,
            nodeSnapshots: [
                { nodeId: '1', address: 'http://one.local/nifi', apiPort: '8001', statusSnapshots: snapshots },
                { nodeId: '2', address: 'http://one.local/nifi', apiPort: '8002', statusSnapshots: snapshots }
            ]
        }
    };

    async function setup({ statusHistoryState = initialStatusHistoryState } = {}) {
        await TestBed.configureTestingModule({
            imports: [HttpClientTestingModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: {} },
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [statusHistoryFeatureKey]: statusHistoryState
                    }
                }),
                { provide: MatDialogRef, useValue: null }
            ]
        }).compileComponents();
        const store: MockStore = TestBed.inject(MockStore);
        const fixture = TestBed.createComponent(StatusHistory);
        const component = fixture.componentInstance;
        fixture.detectChanges();
        return { component, fixture, store };
    }

    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    it('it should filter instances based on start time selected', async () => {
        const { component, fixture } = await setup({
            statusHistoryState: {
                statusHistory: statusHistoryEntity,
                status: 'success',
                loadedTimestamp: ''
            }
        });

        let filtered = component.filteredInstances();
        expect(component.instances.length).toBe(3);
        expect(component.instances[0].snapshots.length).toBe(5);
        expect(component.instances[1].snapshots.length).toBe(5);
        expect(component.instances[2].snapshots.length).toBe(5);
        expect(filtered[0].snapshots.length).toBe(5);
        expect(filtered[1].snapshots.length).toBe(5);
        expect(filtered[2].snapshots.length).toBe(5);

        expect(component.maxDate).toBe(now);

        // The 'All' option should be initially selected
        expect(component.statusHistoryForm.get('start')?.value).toBe(0);

        component.statusHistoryForm.get('start')?.setValue(minutes(5));
        component.startChanged(minutes(5));
        fixture.detectChanges();

        filtered = component.filteredInstances();
        expect(filtered[0].snapshots.length).toBe(2);
        expect(filtered[1].snapshots.length).toBe(2);
        expect(filtered[2].snapshots.length).toBe(2);

        component.statusHistoryForm.get('start')?.setValue(minutes(30));
        component.startChanged(minutes(30));
        fixture.detectChanges();

        filtered = component.filteredInstances();
        expect(filtered[0].snapshots.length).toBe(4);
        expect(filtered[1].snapshots.length).toBe(4);
        expect(filtered[2].snapshots.length).toBe(4);
    });
});
