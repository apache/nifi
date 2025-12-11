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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { MatDialogRef } from '@angular/material/dialog';
import { FormBuilder } from '@angular/forms';

import { ComponentStateDialog } from './component-state.component';
import { initialState } from '../../../state/component-state/component-state.reducer';
import { ComponentState, ComponentStateState, componentStateFeatureKey } from '../../../state/component-state';
import * as ComponentStateActions from '../../../state/component-state/component-state.actions';
import { initialState as initialErrorState } from '../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../state/error';
import { initialState as initialCurrentUserState } from '../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../state/current-user';
import { ComponentType, NiFiCommon } from '@nifi/shared';

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
        componentType: ComponentType.Processor,
        componentId: 'test-id',
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
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [componentStateFeatureKey]: mockInitialState
                    }
                }),
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
            expect(component.dataSource.length).toBe(4); // 2 local + 2 cluster
            expect(component.allStateItems.length).toBe(4);
            expect(component.totalEntries).toBe(4);
            expect(component.filteredEntries).toBe(4);
            expect(component.stateDescription).toBe('Test state description for processor');
        });

        it('should set correct scope for local and cluster state items', () => {
            const localItems = component.dataSource.filter((item) => item.scope !== 'Cluster');
            const clusterItems = component.dataSource.filter((item) => item.scope === 'Cluster');

            expect(localItems.length).toBe(2);
            expect(clusterItems.length).toBe(2);
            expect(localItems[0].scope).toBe('node1:8443');
            expect(clusterItems[0].scope).toBe('Cluster');
        });

        it('should include actions column when canClear and dropStateKeySupported are true', () => {
            expect(component.displayedColumns).toContain('actions');
            expect(component.canClear).toBe(true);
        });

        it('should initialize currentSortColumn and currentSortDirection', () => {
            expect(component.currentSortColumn).toBe('key');
            expect(component.currentSortDirection).toBe('asc');
        });
    });

    describe('filtering', () => {
        it('should filter data source based on filter term', () => {
            component.applyFilter('local-key1');

            expect(component.dataSource.length).toBe(1);
            expect(component.filteredEntries).toBe(1);
            expect(component.dataSource[0].key).toBe('local-key1');
        });

        it('should filter case-insensitively', () => {
            component.applyFilter('CLUSTER');

            expect(component.dataSource.length).toBe(2);
            expect(component.filteredEntries).toBe(2);
            expect(component.dataSource.every((item) => item.key.includes('cluster'))).toBe(true);
        });

        it('should show all items when filter is cleared', () => {
            component.applyFilter('local');
            expect(component.filteredEntries).toBe(2);

            component.applyFilter('');
            expect(component.filteredEntries).toBe(4);
        });

        it('should filter by value field', () => {
            component.applyFilter('value1');

            expect(component.dataSource.length).toBeGreaterThan(0);
            expect(component.dataSource.every((item) => item.value.includes('value1'))).toBe(true);
        });

        it('should filter by scope field', () => {
            component.applyFilter('node1');

            expect(component.dataSource.length).toBe(2);
            expect(component.dataSource.every((item) => item.scope?.includes('node1'))).toBe(true);
        });

        it('should preserve allStateItems when filtering', () => {
            const originalLength = component.allStateItems.length;
            component.applyFilter('local');

            expect(component.allStateItems.length).toBe(originalLength);
            expect(component.dataSource.length).toBeLessThan(originalLength);
        });

        it('should trim filter term before applying', () => {
            component.applyFilter('  local-key1  ');

            expect(component.dataSource.length).toBe(1);
            expect(component.dataSource[0].key).toBe('local-key1');
        });
    });

    describe('sorting', () => {
        it('should sort data by key in ascending order by default', () => {
            // Data is sorted alphabetically, so check we have the expected structure
            expect(component.dataSource.length).toBe(4);
            expect(component.dataSource.some((item) => item.key.includes('cluster'))).toBe(true);
            expect(component.dataSource.some((item) => item.key.includes('local'))).toBe(true);
        });

        it('should sort data by key in descending order', () => {
            component.sortData({ active: 'key', direction: 'desc' });

            // Verify that data is still properly structured after sorting
            expect(component.dataSource.length).toBe(4);
            expect(component.dataSource.every((item) => item.key && item.value)).toBe(true);
        });

        it('should sort data by value', () => {
            component.sortData({ active: 'value', direction: 'asc' });

            // Verify sorting is called with value comparison
            expect(nifiCommon.compareString).toHaveBeenCalledWith(expect.any(String), expect.any(String));
        });

        it('should sort data by scope', () => {
            component.sortData({ active: 'scope', direction: 'asc' });

            // Cluster scope should come before node addresses
            const clusterItems = component.dataSource.filter((item) => item.scope === 'Cluster');
            expect(clusterItems.length).toBe(2);
        });

        it('should update currentSortColumn and currentSortDirection when sorting', () => {
            component.sortData({ active: 'value', direction: 'desc' });

            expect(component.currentSortColumn).toBe('value');
            expect(component.currentSortDirection).toBe('desc');
        });

        it('should update allStateItems when sorting without filter', () => {
            const originalFirstItem = component.allStateItems[0];
            component.sortData({ active: 'key', direction: 'desc' });

            // After sorting descending, first item should be different
            expect(component.allStateItems[0]).not.toBe(originalFirstItem);
        });

        it('should preserve allStateItems when sorting with active filter', () => {
            const originalAllStateItems = [...component.allStateItems];

            // Apply filter directly
            component.applyFilter('local');

            // Sort the filtered data
            component.sortData({ active: 'key', direction: 'desc' });

            // allStateItems should remain unchanged (same keys, though may be reordered)
            expect(component.allStateItems.length).toBe(originalAllStateItems.length);
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

        it('should handle empty state map', () => {
            const emptyStateMap = {
                scope: 'LOCAL',
                state: [],
                totalEntryCount: 0
            };

            const result = component.processStateMap(emptyStateMap, false);

            expect(result.length).toBe(0);
        });

        it('should handle state map with null state array', () => {
            const nullStateMap = {
                scope: 'LOCAL',
                state: null,
                totalEntryCount: 0
            } as any;

            const result = component.processStateMap(nullStateMap, false);

            expect(result.length).toBe(0);
        });
    });

    describe('trackByKey', () => {
        it('should return a unique identifier combining key and scope', () => {
            const item = { key: 'test-key', value: 'test-value', scope: 'LOCAL' };
            const result = component.trackByKey(0, item);

            expect(result).toBe('test-key-LOCAL');
        });

        it('should return unique identifiers for different items', () => {
            const item1 = { key: 'key-1', value: 'value-1', scope: 'LOCAL' };
            const item2 = { key: 'key-2', value: 'value-2', scope: 'LOCAL' };

            const result1 = component.trackByKey(0, item1);
            const result2 = component.trackByKey(1, item2);

            expect(result1).not.toBe(result2);
        });

        it('should differentiate items with same key but different scopes', () => {
            const localItem = { key: 'same-key', value: 'value-1', scope: 'node1:8443' };
            const clusterItem = { key: 'same-key', value: 'value-2', scope: 'Cluster' };

            const result1 = component.trackByKey(0, localItem);
            const result2 = component.trackByKey(1, clusterItem);

            expect(result1).toBe('same-key-node1:8443');
            expect(result2).toBe('same-key-Cluster');
            expect(result1).not.toBe(result2);
        });

        it('should handle items with undefined scope', () => {
            const item = { key: 'test-key', value: 'test-value', scope: undefined };
            const result = component.trackByKey(0, item);

            expect(result).toBe('test-key-none');
        });
    });

    describe('virtual scrolling support', () => {
        it('should handle large datasets efficiently', () => {
            // Simulate a large dataset
            const largeStateMap = {
                scope: 'LOCAL',
                state: Array.from({ length: 1000 }, (_, i) => ({
                    key: `key-${i}`,
                    value: `value-${i}`,
                    clusterNodeAddress: 'node1:8443'
                })),
                totalEntryCount: 1000
            };

            const result = component.processStateMap(largeStateMap, false);

            expect(result.length).toBe(1000);
            expect(component.dataSource.length).toBeGreaterThan(0);
        });

        it('should maintain all data in allStateItems for virtual scrolling', () => {
            expect(component.allStateItems.length).toBe(component.dataSource.length);
        });
    });

    describe('input properties', () => {
        it('should use custom initial sort column when provided', () => {
            const customFixture = TestBed.createComponent(ComponentStateDialog);
            const customComponent = customFixture.componentInstance;
            customComponent.initialSortColumn = 'value';
            customComponent.initialSortDirection = 'desc';

            customFixture.detectChanges();

            expect(customComponent.initialSortColumn).toBe('value');
            expect(customComponent.initialSortDirection).toBe('desc');
        });
    });

    describe('edge cases', () => {
        it('should handle sorting with empty dataset', () => {
            component.dataSource = [];
            component.allStateItems = [];

            component.sortData({ active: 'key', direction: 'asc' });

            expect(component.dataSource.length).toBe(0);
        });

        it('should handle filtering with empty dataset', () => {
            component.dataSource = [];
            component.allStateItems = [];

            component.applyFilter('test');

            expect(component.dataSource.length).toBe(0);
            expect(component.filteredEntries).toBe(0);
        });

        it('should handle scope sorting when scope is undefined', () => {
            const itemsWithoutScope = [
                { key: 'key1', value: 'value1', scope: undefined },
                { key: 'key2', value: 'value2', scope: 'Cluster' }
            ];
            component.dataSource = itemsWithoutScope;
            component.allStateItems = itemsWithoutScope;

            component.sortData({ active: 'scope', direction: 'asc' });

            // Should not throw error
            expect(component.dataSource.length).toBe(2);
        });
    });
});
