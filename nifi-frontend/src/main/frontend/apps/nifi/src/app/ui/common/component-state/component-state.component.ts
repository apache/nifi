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

import { AfterViewInit, Component, DestroyRef, inject, Input, Signal } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatSortModule, Sort } from '@angular/material/sort';
import { AsyncPipe } from '@angular/common';
import { Observable } from 'rxjs';
import { ScrollingModule } from '@angular/cdk/scrolling';
import { isDefinedAndNotNull, CloseOnEscapeDialog, NiFiCommon, NifiTooltipDirective, TextTip } from '@nifi/shared';
import { ComponentStateState, StateEntry, StateItem, StateMap } from '../../../state/component-state';
import { Store } from '@ngrx/store';
import { clearComponentState, clearComponentStateEntry } from '../../../state/component-state/component-state.actions';
import {
    selectCanClear,
    selectClearing,
    selectComponentName,
    selectComponentState,
    selectDropStateKeySupported
} from '../../../state/component-state/component-state.selectors';
import { debounceTime } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { selectClusterSummary } from '../../../state/cluster-summary/cluster-summary.selectors';
import { ErrorContextKey } from '../../../state/error';
import { ContextErrorBanner } from '../context-error-banner/context-error-banner.component';
import { concatLatestFrom } from '@ngrx/operators';
import { NifiSpinnerDirective } from '../spinner/nifi-spinner.directive';
import { MatTableModule } from '@angular/material/table';

@Component({
    selector: 'component-state',
    standalone: true,
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatSortModule,
        AsyncPipe,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatInputModule,
        ContextErrorBanner,
        NifiTooltipDirective,
        NifiSpinnerDirective,
        ScrollingModule,
        MatTableModule
    ],
    templateUrl: './component-state.component.html',
    styleUrls: ['./component-state.component.scss']
})
export class ComponentStateDialog extends CloseOnEscapeDialog implements AfterViewInit {
    private store = inject<Store<ComponentStateState>>(Store);
    private formBuilder = inject(FormBuilder);
    private nifiCommon = inject(NiFiCommon);

    @Input() initialSortColumn: 'key' | 'value' = 'key';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';

    componentName$: Observable<string> = this.store.select(selectComponentName).pipe(isDefinedAndNotNull());
    dropStateKeySupported$: Observable<boolean> = this.store.select(selectDropStateKeySupported);
    clearing: Signal<boolean> = this.store.selectSignal(selectClearing);

    displayedColumns: string[] = ['key', 'value'];
    dataSource: StateItem[] = [];
    allStateItems: StateItem[] = []; // Full dataset for filtering

    // Virtual scroll configuration
    readonly ROW_HEIGHT = 36; // Height of each table row in pixels (density -4)

    // Sort state
    currentSortColumn: 'key' | 'value' | 'scope' = this.initialSortColumn;
    currentSortDirection: 'asc' | 'desc' = this.initialSortDirection;

    filterForm: FormGroup;

    stateDescription = '';
    totalEntries = 0;
    filteredEntries = 0;
    partialResults = false;
    canClear = false;
    private destroyRef: DestroyRef = inject(DestroyRef);

    constructor() {
        super();
        this.filterForm = this.formBuilder.group({ filterTerm: '' });

        this.store
            .select(selectComponentState)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((componentState) => {
                this.totalEntries = 0;
                this.stateDescription = componentState.stateDescription;

                const stateItems: StateItem[] = [];

                if (componentState.localState) {
                    const localStateItems: StateItem[] = this.processStateMap(componentState.localState, false);
                    stateItems.push(...localStateItems);
                }

                if (componentState.clusterState) {
                    const clusterStateItems: StateItem[] = this.processStateMap(componentState.clusterState, true);
                    stateItems.push(...clusterStateItems);
                }

                const sortedItems = this.sortStateItems(stateItems, {
                    active: this.initialSortColumn,
                    direction: this.initialSortDirection
                });

                // Store full dataset for virtual scrolling
                this.allStateItems = sortedItems;
                this.dataSource = sortedItems;

                this.filteredEntries = stateItems.length;

                // apply any filtering to the new data
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                if (filterTerm?.length > 0) {
                    this.applyFilter(filterTerm);
                }
            });

        this.store
            .select(selectClusterSummary)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((clusterSummary) => {
                if (clusterSummary.connectedToCluster) {
                    // if we're connected to the cluster add a scope column if it's not already present
                    if (!this.displayedColumns.includes('scope')) {
                        this.displayedColumns.splice(this.displayedColumns.length, 0, 'scope');
                    }
                } else {
                    // if we're not connected to the cluster remove the scope column if it is present
                    const nodeIndex = this.displayedColumns.indexOf('scope');
                    if (nodeIndex > -1) {
                        this.displayedColumns.splice(nodeIndex, 1);
                    }
                }
            });

        // Subscribe to dropStateKeySupported to conditionally show actions column
        this.dropStateKeySupported$
            .pipe(
                concatLatestFrom(() => this.store.select(selectCanClear).pipe(isDefinedAndNotNull())),
                takeUntilDestroyed()
            )
            .subscribe(([dropStateKeySupported, canClear]) => {
                this.canClear = canClear;

                if (canClear && dropStateKeySupported) {
                    // Add actions column if it's not already present
                    if (!this.displayedColumns.includes('actions')) {
                        this.displayedColumns.push('actions');
                    }
                } else {
                    // Remove actions column if it is present
                    const actionsIndex = this.displayedColumns.indexOf('actions');
                    if (actionsIndex > -1) {
                        this.displayedColumns.splice(actionsIndex, 1);
                    }
                }
            });
    }

    ngAfterViewInit(): void {
        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                this.applyFilter(filterTerm);
            });
    }

    processStateMap(stateMap: StateMap, clusterState: boolean): StateItem[] {
        const stateEntries: StateEntry[] = stateMap.state ? stateMap.state : [];

        if (stateEntries.length !== stateMap.totalEntryCount) {
            this.partialResults = true;
        }

        this.totalEntries += stateMap.totalEntryCount;

        return stateEntries.map((stateEntry) => {
            return {
                key: stateEntry.key,
                value: stateEntry.value,
                scope: clusterState ? 'Cluster' : stateEntry.clusterNodeAddress
            };
        });
    }

    applyFilter(filterTerm: string) {
        const term = filterTerm.trim().toLowerCase();

        if (!term) {
            // No filter - show all items
            this.dataSource = this.allStateItems;
            this.filteredEntries = this.allStateItems.length;
        } else {
            // Filter the full dataset
            const filtered = this.filterStateItems(term);
            this.dataSource = filtered;
            this.filteredEntries = filtered.length;
        }
    }

    sortData(sort: Sort) {
        this.currentSortColumn = sort.active as 'key' | 'value' | 'scope';
        this.currentSortDirection = sort.direction as 'asc' | 'desc';

        // Determine what data to sort (filtered or all)
        const filterTerm = this.filterForm.get('filterTerm')?.value?.trim().toLowerCase();
        let dataToSort = this.allStateItems;

        if (filterTerm) {
            dataToSort = this.filterStateItems(filterTerm);
        }

        const sortedData = this.sortStateItems(dataToSort, sort);

        // Update allStateItems with sorted data if not filtering
        if (!filterTerm) {
            this.allStateItems = sortedData;
        }

        this.dataSource = sortedData;
        this.filteredEntries = sortedData.length;
    }

    private filterStateItems(filterTerm: string): StateItem[] {
        return this.allStateItems.filter(
            (item) =>
                item.key.toLowerCase().includes(filterTerm) ||
                item.value.toLowerCase().includes(filterTerm) ||
                (item.scope && item.scope.toLowerCase().includes(filterTerm))
        );
    }

    private sortStateItems(data: StateItem[], sort: Sort): StateItem[] {
        if (!data) {
            return [];
        }

        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'key':
                    retVal = this.nifiCommon.compareString(a.key, b.key);
                    break;
                case 'value':
                    retVal = this.nifiCommon.compareString(a.value, b.value);
                    break;
                case 'scope':
                    if (a.scope && b.scope) {
                        retVal = this.nifiCommon.compareString(a.scope, b.scope);
                    }
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    clearState(): void {
        this.store.dispatch(clearComponentState());
    }

    clearComponentStateEntry(item: StateItem): void {
        // Determine the scope based on the item's scope property
        // If scope is 'Cluster', it's cluster state
        // Otherwise, it's local state (could be node address or empty)
        const scope = item.scope === 'Cluster' ? 'CLUSTER' : 'LOCAL';

        this.store.dispatch(
            clearComponentStateEntry({
                request: {
                    keyToDelete: item.key,
                    scope
                }
            })
        );
    }

    trackByKey(index: number, item: StateItem): string {
        return `${item.key}-${item.scope || 'none'}`;
    }

    protected readonly ErrorContextKey = ErrorContextKey;
    protected readonly TextTip = TextTip;
}
