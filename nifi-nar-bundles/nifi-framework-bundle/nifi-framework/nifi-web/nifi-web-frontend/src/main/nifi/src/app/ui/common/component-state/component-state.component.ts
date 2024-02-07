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
import { NiFiCommon } from '../../../service/nifi-common.service';
import { MatSortModule, Sort } from '@angular/material/sort';
import { AsyncPipe, NgIf } from '@angular/common';
import { NifiTooltipDirective } from '../tooltips/nifi-tooltip.directive';
import { NifiSpinnerDirective } from '../spinner/nifi-spinner.directive';
import { ComponentStateState, StateEntry, StateMap } from '../../../state/component-state';
import { Store } from '@ngrx/store';
import { clearComponentState } from '../../../state/component-state/component-state.actions';
import {
    selectCanClear,
    selectComponentName,
    selectComponentState
} from '../../../state/component-state/component-state.selectors';
import { isDefinedAndNotNull } from '../../../state/shared';
import { debounceTime, Observable } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';

@Component({
    selector: 'component-state',
    standalone: true,
    templateUrl: './component-state.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        MatSortModule,
        NgIf,
        NifiTooltipDirective,
        NifiSpinnerDirective,
        AsyncPipe,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatInputModule
    ],
    styleUrls: ['./component-state.component.scss']
})
export class ComponentStateDialog implements AfterViewInit {
    @Input() initialSortColumn: 'key' | 'value' = 'key';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';

    componentName$: Observable<string> = this.store.select(selectComponentName).pipe(isDefinedAndNotNull());
    canClear$: Observable<boolean> = this.store.select(selectCanClear).pipe(isDefinedAndNotNull());

    // TODO - need to include scope column when clustered
    displayedColumns: string[] = ['key', 'value'];
    dataSource: MatTableDataSource<StateEntry> = new MatTableDataSource<StateEntry>();

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
        this.filterForm = this.formBuilder.group({ filterTerm: '' });

        this.store
            .select(selectComponentState)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((componentState) => {
                this.stateDescription = componentState.stateDescription;

                const stateItems: StateEntry[] = [];
                if (componentState.localState) {
                    const localStateItems: StateEntry[] = this.processStateMap(componentState.localState);
                    stateItems.push(...localStateItems);
                }
                if (componentState.clusterState) {
                    const clusterStateItems: StateEntry[] = this.processStateMap(componentState.clusterState);
                    stateItems.push(...clusterStateItems);
                }

                this.dataSource.data = this.sortStateEntries(stateItems, {
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
    }

    ngAfterViewInit(): void {
        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                this.applyFilter(filterTerm);
            });
    }

    processStateMap(stateMap: StateMap): StateEntry[] {
        const stateItems: StateEntry[] = stateMap.state ? stateMap.state : [];

        if (stateItems.length !== stateMap.totalEntryCount) {
            this.partialResults = true;
        }

        this.totalEntries += stateMap.totalEntryCount;

        return stateItems;
    }

    applyFilter(filterTerm: string) {
        this.dataSource.filter = filterTerm.trim().toLowerCase();
        this.filteredEntries = this.dataSource.filteredData.length;
    }

    sortData(sort: Sort) {
        this.dataSource.data = this.sortStateEntries(this.dataSource.data, sort);
    }

    private sortStateEntries(data: StateEntry[], sort: Sort): StateEntry[] {
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
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    clearState(): void {
        this.store.dispatch(clearComponentState());
    }
}
