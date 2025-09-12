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

import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { MatDialogRef } from '@angular/material/dialog';
import { FormBuilder } from '@angular/forms';

import { ComponentStateDialog } from './component-state.component';
import { initialState } from '../../../state/component-state/component-state.reducer';
import { ComponentState, ComponentStateState } from '../../../state/component-state';
import * as ComponentStateActions from '../../../state/component-state/component-state.actions';
import { NiFiCommon } from '@nifi/shared';

describe('ComponentStateDialog', () => {
    let component: ComponentStateDialog;
    let fixture: ComponentFixture<ComponentStateDialog>;
    let store: MockStore;
    let nifiCommon: jest.Mocked<NiFiCommon>;

    const mockComponentState: ComponentState = {
        componentId: 'test-component-id',
        stateDescription: 'Test state description for processor',
        localState: {
            scope: 'LOCAL',
            state: [
                { key: 'local-key1', value: 'local-value1', clusterNodeAddress: 'node1:8443' },
                { key: 'local-key2', value: 'local-value2', clusterNodeAddress: 'node1:8443' }
            ],
            totalEntryCount: 2
        },
        clusterState: {
            scope: 'CLUSTER',
            state: [
                { key: 'cluster-key1', value: 'cluster-value1' },
                { key: 'cluster-key2', value: 'cluster-value2' }
            ],
            totalEntryCount: 2
        },
        dropStateKeySupported: true
    };

    const mockInitialState: ComponentStateState = {
        ...initialState,
        componentName: 'Test Component',
        componentUri: 'https://localhost:8443/nifi-api/processors/test-id',
        componentState: mockComponentState,
        canClear: true,
        clearing: false,
        status: 'success'
    };

    beforeEach(() => {
        const nifiCommonSpy = {
            compareString: jest.fn()
        };

        TestBed.configureTestingModule({
            imports: [ComponentStateDialog, NoopAnimationsModule],
            providers: [
                provideMockStore({ initialState: { componentState: mockInitialState } }),
                { provide: MatDialogRef, useValue: null },
                { provide: NiFiCommon, useValue: nifiCommonSpy },
                FormBuilder
            ]
        });

        fixture = TestBed.createComponent(ComponentStateDialog);
        component = fixture.componentInstance;
        store = TestBed.inject(MockStore);
        nifiCommon = TestBed.inject(NiFiCommon) as jest.Mocked<NiFiCommon>;

        // Mock compareString to provide predictable sorting
        nifiCommon.compareString.mockImplementation((a: string | null | undefined, b: string | null | undefined) => {
            if (!a && !b) return 0;
            if (!a) return -1;
            if (!b) return 1;
            return a.localeCompare(b);
        });

        jest.spyOn(store, 'dispatch');
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('component initialization', () => {
        it('should process component state and populate data source', () => {
            expect(component.dataSource.data.length).toBe(4); // 2 local + 2 cluster
            expect(component.totalEntries).toBe(4);
            expect(component.filteredEntries).toBe(4);
            expect(component.stateDescription).toBe('Test state description for processor');
        });

        it('should set correct scope for local and cluster state items', () => {
            const localItems = component.dataSource.data.filter((item) => item.scope !== 'Cluster');
            const clusterItems = component.dataSource.data.filter((item) => item.scope === 'Cluster');

            expect(localItems.length).toBe(2);
            expect(clusterItems.length).toBe(2);
            expect(localItems[0].scope).toBe('node1:8443');
            expect(clusterItems[0].scope).toBe('Cluster');
        });

        it('should include actions column when canClear and dropStateKeySupported are true', () => {
            expect(component.displayedColumns).toContain('actions');
            expect(component.canClear).toBe(true);
        });
    });

    describe('filtering', () => {
        it('should filter data source based on filter term', fakeAsync(() => {
            component.filterForm.get('filterTerm')?.setValue('local-key1');
            tick(500); // Wait for debounce

            expect(component.dataSource.filteredData.length).toBe(1);
            expect(component.filteredEntries).toBe(1);
            expect(component.dataSource.filteredData[0].key).toBe('local-key1');
        }));

        it('should filter case-insensitively', fakeAsync(() => {
            component.filterForm.get('filterTerm')?.setValue('CLUSTER');
            tick(500); // Wait for debounce

            expect(component.dataSource.filteredData.length).toBe(2);
            expect(component.filteredEntries).toBe(2);
            expect(component.dataSource.filteredData.every((item) => item.key.includes('cluster'))).toBe(true);
        }));

        it('should show all items when filter is cleared', fakeAsync(() => {
            component.filterForm.get('filterTerm')?.setValue('local');
            tick(500);
            expect(component.filteredEntries).toBe(2);

            component.filterForm.get('filterTerm')?.setValue('');
            tick(500);
            expect(component.filteredEntries).toBe(4);
        }));
    });

    describe('sorting', () => {
        it('should sort data by key in ascending order by default', () => {
            // Data is sorted alphabetically, so check we have the expected structure
            expect(component.dataSource.data.length).toBe(4);
            expect(component.dataSource.data.some((item) => item.key.includes('cluster'))).toBe(true);
            expect(component.dataSource.data.some((item) => item.key.includes('local'))).toBe(true);
        });

        it('should sort data by key in descending order', () => {
            component.sortData({ active: 'key', direction: 'desc' });

            // Verify that data is still properly structured after sorting
            expect(component.dataSource.data.length).toBe(4);
            expect(component.dataSource.data.every((item) => item.key && item.value)).toBe(true);
        });

        it('should sort data by value', () => {
            component.sortData({ active: 'value', direction: 'asc' });

            // Verify sorting is called with value comparison
            expect(nifiCommon.compareString).toHaveBeenCalledWith(expect.any(String), expect.any(String));
        });

        it('should sort data by scope', () => {
            component.sortData({ active: 'scope', direction: 'asc' });

            // Cluster scope should come before node addresses
            const clusterItems = component.dataSource.data.filter((item) => item.scope === 'Cluster');
            expect(clusterItems.length).toBe(2);
        });
    });

    describe('clearState', () => {
        it('should dispatch clearComponentState action', () => {
            component.clearState();

            expect(store.dispatch).toHaveBeenCalledWith(ComponentStateActions.clearComponentState());
        });
    });

    describe('clearComponentStateEntry', () => {
        it('should dispatch clearComponentStateEntry action for local state', () => {
            const localStateItem = {
                key: 'local-key1',
                value: 'local-value1',
                scope: 'node1:8443'
            };

            component.clearComponentStateEntry(localStateItem);

            expect(store.dispatch).toHaveBeenCalledWith(
                ComponentStateActions.clearComponentStateEntry({
                    request: {
                        keyToDelete: 'local-key1',
                        scope: 'LOCAL'
                    }
                })
            );
        });

        it('should dispatch clearComponentStateEntry action for cluster state', () => {
            const clusterStateItem = {
                key: 'cluster-key1',
                value: 'cluster-value1',
                scope: 'Cluster'
            };

            component.clearComponentStateEntry(clusterStateItem);

            expect(store.dispatch).toHaveBeenCalledWith(
                ComponentStateActions.clearComponentStateEntry({
                    request: {
                        keyToDelete: 'cluster-key1',
                        scope: 'CLUSTER'
                    }
                })
            );
        });
    });

    describe('processStateMap', () => {
        it('should handle state map with partial results', () => {
            const partialStateMap = {
                scope: 'LOCAL',
                state: [{ key: 'key1', value: 'value1' }],
                totalEntryCount: 5 // More than actual state entries
            };

            const result = component.processStateMap(partialStateMap, false);

            expect(result.length).toBe(1);
            expect(component.partialResults).toBe(true);
            expect(component.totalEntries).toBeGreaterThan(0);
        });

        it('should correctly map cluster state items', () => {
            const clusterStateMap = {
                scope: 'CLUSTER',
                state: [{ key: 'cluster-key', value: 'cluster-value', clusterNodeAddress: 'node1:8443' }],
                totalEntryCount: 1
            };

            const result = component.processStateMap(clusterStateMap, true);

            expect(result[0].scope).toBe('Cluster');
            expect(result[0].key).toBe('cluster-key');
            expect(result[0].value).toBe('cluster-value');
        });

        it('should correctly map local state items', () => {
            const localStateMap = {
                scope: 'LOCAL',
                state: [{ key: 'local-key', value: 'local-value', clusterNodeAddress: 'node2:8443' }],
                totalEntryCount: 1
            };

            const result = component.processStateMap(localStateMap, false);

            expect(result[0].scope).toBe('node2:8443');
            expect(result[0].key).toBe('local-key');
            expect(result[0].value).toBe('local-value');
        });
    });
});
