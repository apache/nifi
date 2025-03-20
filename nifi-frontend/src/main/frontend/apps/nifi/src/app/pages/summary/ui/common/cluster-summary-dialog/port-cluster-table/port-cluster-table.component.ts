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

import { Component, Input } from '@angular/core';
import { ComponentClusterTable } from '../component-cluster-table/component-cluster-table.component';
import { NodePortStatusSnapshot, NodeProcessorStatusSnapshot } from '../../../../state';
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatTableModule } from '@angular/material/table';
import { NgClass } from '@angular/common';

@Component({
    selector: 'port-cluster-table',
    imports: [MatSortModule, MatTableModule, NgClass],
    templateUrl: './port-cluster-table.component.html',
    styleUrl: './port-cluster-table.component.scss'
})
export class PortClusterTable extends ComponentClusterTable<NodePortStatusSnapshot> {
    private _portType!: 'input' | 'output';

    displayedColumns: string[] = [];

    @Input() set portType(type: 'input' | 'output') {
        if (type === 'input') {
            this.displayedColumns = ['node', 'runStatus', 'out'];
        } else {
            this.displayedColumns = ['node', 'runStatus', 'in'];
        }
        this._portType = type;
    }

    get portType() {
        return this._portType;
    }

    constructor() {
        super();
    }

    sortEntities(data: NodePortStatusSnapshot[], sort: Sort): NodePortStatusSnapshot[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc: boolean = sort.direction === 'asc';
            switch (sort.active) {
                case 'node':
                    return this.compare(a.address, b.address, isAsc);
                case 'runStatus':
                    return this.compare(this.formatRunStatus(a), this.formatRunStatus(b), isAsc);
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
                default:
                    return 0;
            }
        });
    }

    supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'in':
            case 'out':
                return true;
            default:
                return false;
        }
    }

    formatNode(processor: NodeProcessorStatusSnapshot): string {
        return `${processor.address}:${processor.apiPort}`;
    }

    formatRunStatus(port: NodePortStatusSnapshot): string {
        return port.statusSnapshot.runStatus;
    }

    formatIn(port: NodePortStatusSnapshot): string {
        return port.statusSnapshot.input;
    }

    formatOut(port: NodePortStatusSnapshot): string {
        return port.statusSnapshot.output;
    }

    getRunStatusIcon(port: NodePortStatusSnapshot): string {
        switch (port.statusSnapshot.runStatus.toLowerCase()) {
            case 'running':
                return 'running fa fa-play success-color-default';
            case 'stopped':
                return 'stopped fa fa-stop error-color-variant';
            case 'enabled':
                return 'enabled fa fa-flash success-color-variant';
            case 'disabled':
                return 'disabled icon icon-enable-false neutral-color';
            case 'validating':
                return 'validating fa fa-spin fa-circle-notch neutral-color';
            case 'invalid':
                return 'invalid fa fa-warning caution-color';
            default:
                return '';
        }
    }
}
