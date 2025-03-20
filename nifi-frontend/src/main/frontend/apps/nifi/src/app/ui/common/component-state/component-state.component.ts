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

import { AfterViewInit, Component, DestroyRef, inject, Input } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { AsyncPipe } from '@angular/common';
import { isDefinedAndNotNull, CloseOnEscapeDialog, NiFiCommon } from '@nifi/shared';
import { ComponentStateState, StateEntry, StateItem, StateMap } from '../../../state/component-state';
import { Store } from '@ngrx/store';
import { clearComponentState } from '../../../state/component-state/component-state.actions';
import {
    selectCanClear,
    selectComponentName,
    selectComponentState
} from '../../../state/component-state/component-state.selectors';
import { debounceTime, Observable } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { selectClusterSummary } from '../../../state/cluster-summary/cluster-summary.selectors';
import { ErrorContextKey } from '../../../state/error';
import { ContextErrorBanner } from '../context-error-banner/context-error-banner.component';

@Component({
    selector: 'component-state',
    templateUrl: './component-state.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        MatSortModule,
        AsyncPipe,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatInputModule,
        ContextErrorBanner
    ],
    styleUrls: ['./component-state.component.scss']
})
export class ComponentStateDialog extends CloseOnEscapeDialog implements AfterViewInit {
    @Input() initialSortColumn: 'key' | 'value' = 'key';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';

    componentName$: Observable<string> = this.store.select(selectComponentName).pipe(isDefinedAndNotNull());
    canClear$: Observable<boolean> = this.store.select(selectCanClear).pipe(isDefinedAndNotNull());

    displayedColumns: string[] = ['key', 'value'];
    dataSource: MatTableDataSource<StateItem> = new MatTableDataSource<StateItem>();

    filterForm: FormGroup;

    stateDescription = '';
    totalEntries = 0;
    filteredEntries = 0;
    partialResults = false;
    private destroyRef: DestroyRef = inject(DestroyRef);

    constructor(
        private store: Store<ComponentStateState>,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        super();
        this.filterForm = this.formBuilder.group({ filterTerm: '' });

        this.store
            .select(selectComponentState)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((componentState) => {
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

                this.dataSource.data = this.sortStateItems(stateItems, {
                    active: this.initialSortColumn,
                    direction: this.initialSortDirection
                });
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
        this.dataSource.filter = filterTerm.trim().toLowerCase();
        this.filteredEntries = this.dataSource.filteredData.length;
    }

    sortData(sort: Sort) {
        this.dataSource.data = this.sortStateItems(this.dataSource.data, sort);
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

    protected readonly ErrorContextKey = ErrorContextKey;
}
