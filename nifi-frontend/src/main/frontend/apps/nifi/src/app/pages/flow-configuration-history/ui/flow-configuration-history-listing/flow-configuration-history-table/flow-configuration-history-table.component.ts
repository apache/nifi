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

import { Component, EventEmitter, Input, Output } from '@angular/core';

import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { ActionEntity } from '../../../state/flow-configuration-history-listing';
import { MatIconButton } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';

@Component({
    selector: 'flow-configuration-history-table',
    imports: [MatTableModule, MatSortModule, MatIconButton, MatMenu, MatMenuTrigger, MatMenuItem],
    templateUrl: './flow-configuration-history-table.component.html',
    styleUrls: ['./flow-configuration-history-table.component.scss']
})
export class FlowConfigurationHistoryTable {
    @Input() selectedHistoryActionId: number | null = null;
    @Input() initialSortColumn: 'timestamp' | 'sourceId' | 'sourceName' | 'sourceType' | 'operation' | 'userIdentity' =
        'timestamp';
    @Input() initialSortDirection: 'asc' | 'desc' = 'desc';

    @Input() set historyActions(historyActions: ActionEntity[]) {
        if (historyActions) {
            this.dataSource.data = historyActions;
        }
    }

    @Output() selectionChanged: EventEmitter<ActionEntity> = new EventEmitter<ActionEntity>();
    @Output() sortChanged: EventEmitter<Sort> = new EventEmitter<Sort>();
    @Output() moreDetailsClicked: EventEmitter<ActionEntity> = new EventEmitter<ActionEntity>();

    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };

    displayedColumns: string[] = [
        'timestamp',
        'sourceId',
        'sourceName',
        'sourceType',
        'operation',
        'userIdentity',
        'actions'
    ];
    dataSource: MatTableDataSource<ActionEntity> = new MatTableDataSource<ActionEntity>();

    constructor() {}

    sortData(sort: Sort) {
        this.sortChanged.next(sort);
    }

    canRead(item: ActionEntity): boolean {
        return item.canRead;
    }

    private format(item: ActionEntity, property: string): string {
        if (this.canRead(item) && item.action && Object.hasOwn(item.action, property)) {
            const value = (item.action as any)[property];
            if (!value) {
                return 'Empty String Set';
            }
            return value;
        }
        return 'Not Authorized';
    }

    formatTimestamp(item: ActionEntity): string {
        return this.format(item, 'timestamp');
    }

    formatID(item: ActionEntity): string {
        return this.format(item, 'sourceId');
    }

    formatName(item: ActionEntity): string {
        return this.format(item, 'sourceName');
    }

    formatType(item: ActionEntity): string {
        return this.format(item, 'sourceType');
    }

    formatOperation(item: ActionEntity): string {
        return this.format(item, 'operation');
    }

    formatUser(item: ActionEntity): string {
        return this.format(item, 'userIdentity');
    }

    select(item: ActionEntity) {
        this.selectionChanged.next(item);
    }

    isSelected(item: ActionEntity): boolean {
        if (this.selectedHistoryActionId) {
            return item.id === this.selectedHistoryActionId;
        }
        return false;
    }

    moreDetails(item: ActionEntity) {
        this.moreDetailsClicked.next(item);
    }
}
