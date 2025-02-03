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

import { Component, OnDestroy, OnInit } from '@angular/core';

import { Store } from '@ngrx/store';
import { BulletinBoardFilterArgs, BulletinBoardState, LoadBulletinBoardRequest } from '../../state/bulletin-board';
import {
    selectBulletinBoardFilter,
    selectBulletinBoardState
} from '../../state/bulletin-board/bulletin-board.selectors';
import {
    clearBulletinBoard,
    loadBulletinBoard,
    resetBulletinBoardState,
    setBulletinBoardAutoRefresh,
    setBulletinBoardFilter,
    startBulletinBoardPolling,
    stopBulletinBoardPolling
} from '../../state/bulletin-board/bulletin-board.actions';
import { initialBulletinBoardState } from '../../state/bulletin-board/bulletin-board.reducer';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { ReactiveFormsModule } from '@angular/forms';
import { BulletinBoardList } from './bulletin-board-list/bulletin-board-list.component';
import { MatSlideToggleChange, MatSlideToggleModule } from '@angular/material/slide-toggle';
import { CommonModule } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';

@Component({
    selector: 'bulletin-board',
    imports: [
        NgxSkeletonLoaderModule,
        MatFormFieldModule,
        MatInputModule,
        MatOptionModule,
        MatSelectModule,
        ReactiveFormsModule,
        BulletinBoardList,
        MatSlideToggleModule,
        CommonModule,
        MatButtonModule
    ],
    templateUrl: './bulletin-board.component.html',
    styleUrls: ['./bulletin-board.component.scss']
})
export class BulletinBoard implements OnInit, OnDestroy {
    bulletinBoardState$ = this.store.select(selectBulletinBoardState);
    private bulletinBoardFilter$ = this.store.select(selectBulletinBoardFilter);
    private currentFilter: BulletinBoardFilterArgs | null = null;

    autoRefresh = true;

    constructor(private store: Store<BulletinBoardState>) {}

    ngOnInit(): void {
        this.store.dispatch(
            loadBulletinBoard({
                request: {
                    limit: 10
                }
            })
        );

        this.bulletinBoardFilter$.subscribe((filter) => (this.currentFilter = filter));

        this.store.dispatch(startBulletinBoardPolling());
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetBulletinBoardState());
        this.store.dispatch(stopBulletinBoardPolling());
    }

    isInitialLoading(state: BulletinBoardState): boolean {
        return state.loadedTimestamp == initialBulletinBoardState.loadedTimestamp;
    }

    refreshBulletinBoard(lastBulletinId: number) {
        const request: LoadBulletinBoardRequest = {
            after: lastBulletinId
        };
        if (this.currentFilter) {
            if (this.currentFilter.filterTerm?.length > 0) {
                const filterTerm = this.currentFilter.filterTerm;
                switch (this.currentFilter.filterColumn) {
                    case 'message':
                        request.message = filterTerm;
                        break;
                    case 'id':
                        request.sourceId = filterTerm;
                        break;
                    case 'groupId':
                        request.groupId = filterTerm;
                        break;
                    case 'name':
                        request.sourceName = filterTerm;
                        break;
                }
            }
        }
        this.store.dispatch(loadBulletinBoard({ request }));
    }

    clear() {
        this.store.dispatch(clearBulletinBoard());
    }

    applyFilter(filter: BulletinBoardFilterArgs) {
        // only fire the filter changed event if there is something different
        if (
            filter.filterTerm.length > 0 ||
            (filter.filterTerm.length === 0 && filter.filterColumn === this.currentFilter?.filterColumn)
        ) {
            this.store.dispatch(setBulletinBoardFilter({ filter }));
        }
    }

    autoRefreshToggle(event: MatSlideToggleChange) {
        this.autoRefresh = event.checked;
        this.store.dispatch(setBulletinBoardAutoRefresh({ autoRefresh: this.autoRefresh }));
    }
}
