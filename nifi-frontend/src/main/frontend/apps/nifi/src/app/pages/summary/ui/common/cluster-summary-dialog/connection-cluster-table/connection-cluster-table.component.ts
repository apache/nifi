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

import { Component } from '@angular/core';
import { ComponentClusterTable } from '../component-cluster-table/component-cluster-table.component';
import { NodeConnectionStatusSnapshot } from '../../../../state';
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatTableModule } from '@angular/material/table';
import { NgClass } from '@angular/common';

@Component({
    selector: 'connection-cluster-table',
    imports: [MatTableModule, MatSortModule, NgClass],
    templateUrl: './connection-cluster-table.component.html',
    styleUrl: './connection-cluster-table.component.scss'
})
export class ConnectionClusterTable extends ComponentClusterTable<NodeConnectionStatusSnapshot> {
    displayedColumns: string[] = ['node', 'queue', 'threshold', 'in', 'out'];

    constructor() {
        super();
    }

    formatNode(processor: NodeConnectionStatusSnapshot): string {
        return `${processor.address}:${processor.apiPort}`;
    }

    formatIn(connection: NodeConnectionStatusSnapshot): string {
        return connection.statusSnapshot.input;
    }

    formatOut(connection: NodeConnectionStatusSnapshot): string {
        return connection.statusSnapshot.output;
    }

    formatQueue(connection: NodeConnectionStatusSnapshot): string {
        return connection.statusSnapshot.queued;
    }

    formatThreshold(connection: NodeConnectionStatusSnapshot): string {
        return `${connection.statusSnapshot.percentUseCount}% | ${connection.statusSnapshot.percentUseBytes}%`;
    }

    override sortEntities(data: NodeConnectionStatusSnapshot[], sort: Sort): NodeConnectionStatusSnapshot[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc: boolean = sort.direction === 'asc';
            switch (sort.active) {
                case 'node':
                    return this.compare(a.address, b.address, isAsc);
                case 'in':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(a.statusSnapshot.flowFilesIn, b.statusSnapshot.flowFilesIn, isAsc);
                    } else {
                        return this.compare(a.statusSnapshot.bytesIn, b.statusSnapshot.bytesIn, isAsc);
                    }
                case 'out':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(a.statusSnapshot.flowFilesOut, b.statusSnapshot.flowFilesOut, isAsc);
                    } else {
                        return this.compare(a.statusSnapshot.bytesOut, b.statusSnapshot.bytesOut, isAsc);
                    }
                case 'queue':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(a.statusSnapshot.flowFilesQueued, b.statusSnapshot.flowFilesQueued, isAsc);
                    } else {
                        return this.compare(a.statusSnapshot.bytesQueued, b.statusSnapshot.bytesQueued, isAsc);
                    }
                case 'threshold':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(a.statusSnapshot.percentUseCount, b.statusSnapshot.percentUseCount, isAsc);
                    } else {
                        return this.compare(a.statusSnapshot.percentUseBytes, b.statusSnapshot.percentUseBytes, isAsc);
                    }
                default:
                    return 0;
            }
        });
    }

    override supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'in':
            case 'out':
            case 'threshold':
            case 'queue':
                return true;
            default:
                return false;
        }
    }
}
