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
import { NodeProcessGroupStatusSnapshot } from '../../../../state';
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatTableModule } from '@angular/material/table';
import { NgClass } from '@angular/common';

@Component({
    selector: 'process-group-cluster-table',
    imports: [MatTableModule, MatSortModule, NgClass],
    templateUrl: './process-group-cluster-table.component.html',
    styleUrl: './process-group-cluster-table.component.scss'
})
export class ProcessGroupClusterTable extends ComponentClusterTable<NodeProcessGroupStatusSnapshot> {
    displayedColumns: string[] = ['node', 'transferred', 'in', 'readWrite', 'out', 'sent', 'received'];

    constructor() {
        super();
    }

    formatNode(processor: NodeProcessGroupStatusSnapshot): string {
        return `${processor.address}:${processor.apiPort}`;
    }

    formatTransferred(pg: NodeProcessGroupStatusSnapshot): string {
        return pg.statusSnapshot.transferred;
    }

    formatIn(pg: NodeProcessGroupStatusSnapshot): string {
        return pg.statusSnapshot.input;
    }

    formatReadWrite(pg: NodeProcessGroupStatusSnapshot): string {
        return `${pg.statusSnapshot.read} | ${pg.statusSnapshot.written}`;
    }

    formatOut(pg: NodeProcessGroupStatusSnapshot): string {
        return pg.statusSnapshot.output;
    }

    formatSent(pg: NodeProcessGroupStatusSnapshot): string {
        return pg.statusSnapshot.sent;
    }

    formatReceived(pg: NodeProcessGroupStatusSnapshot): string {
        return pg.statusSnapshot.received;
    }

    override sortEntities(data: NodeProcessGroupStatusSnapshot[], sort: Sort): NodeProcessGroupStatusSnapshot[] {
        if (!data) {
            return [];
        }

        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            switch (sort.active) {
                case 'node':
                    return this.compare(a.address, b.address, isAsc);
                case 'transferred':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(
                            a.statusSnapshot.flowFilesTransferred,
                            b.statusSnapshot.flowFilesTransferred,
                            isAsc
                        );
                    } else {
                        return this.compare(
                            a.statusSnapshot.bytesTransferred,
                            b.statusSnapshot.bytesTransferred,
                            isAsc
                        );
                    }
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
                case 'readWrite':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(a.statusSnapshot.bytesRead, b.statusSnapshot.bytesRead, isAsc);
                    } else {
                        return this.compare(a.statusSnapshot.bytesWritten, b.statusSnapshot.bytesWritten, isAsc);
                    }
                case 'sent':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(a.statusSnapshot.flowFilesSent, b.statusSnapshot.flowFilesSent, isAsc);
                    } else {
                        return this.compare(a.statusSnapshot.bytesSent, b.statusSnapshot.bytesSent, isAsc);
                    }
                case 'received':
                    if (this.multiSort.sortValueIndex === 0) {
                        return this.compare(
                            a.statusSnapshot.flowFilesReceived,
                            b.statusSnapshot.flowFilesReceived,
                            isAsc
                        );
                    } else {
                        return this.compare(a.statusSnapshot.bytesReceived, b.statusSnapshot.bytesReceived, isAsc);
                    }
                default:
                    return 0;
            }
        });
    }

    override supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'transferred':
            case 'in':
            case 'out':
            case 'readWrite':
            case 'received':
            case 'sent':
                return true;
            default:
                return false;
        }
    }
}
