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
import { NodeRemoteProcessGroupStatusSnapshot } from '../../../../state';
import { MatSortModule, Sort } from '@angular/material/sort';
import { ComponentClusterTable } from '../component-cluster-table/component-cluster-table.component';
import { MatTableModule } from '@angular/material/table';
import { NgClass } from '@angular/common';

@Component({
    selector: 'remote-process-group-cluster-table',
    standalone: true,
    imports: [MatTableModule, MatSortModule, NgClass],
    templateUrl: './remote-process-group-cluster-table.component.html',
    styleUrl: './remote-process-group-cluster-table.component.scss'
})
export class RemoteProcessGroupClusterTable extends ComponentClusterTable<NodeRemoteProcessGroupStatusSnapshot> {
    displayedColumns: string[] = ['node', 'uri', 'transmitting', 'sent', 'received'];

    constructor() {
        super();
    }

    supportsMultiValuedSort(sort: Sort): boolean {
        switch (sort.active) {
            case 'sent':
            case 'received':
                return true;
            default:
                return false;
        }
    }

    sortEntities(data: NodeRemoteProcessGroupStatusSnapshot[], sort: Sort): NodeRemoteProcessGroupStatusSnapshot[] {
        if (!data) {
            return [];
        }

        return data.slice().sort((a, b) => {
            const isAsc: boolean = sort.direction === 'asc';
            switch (sort.active) {
                case 'node':
                    return this.compare(a.address, b.address, isAsc);
                case 'transmitting':
                    return this.compare(
                        a.statusSnapshot.transmissionStatus,
                        b.statusSnapshot.transmissionStatus,
                        isAsc
                    );
                case 'uri':
                    return this.compare(a.statusSnapshot.targetUri, b.statusSnapshot.targetUri, isAsc);
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

    formatNode(processor: NodeRemoteProcessGroupStatusSnapshot): string {
        return `${processor.address}:${processor.apiPort}`;
    }

    formatTransmitting(rpg: NodeRemoteProcessGroupStatusSnapshot): string {
        if (rpg.statusSnapshot.transmissionStatus === 'Transmitting') {
            return rpg.statusSnapshot.transmissionStatus;
        } else {
            return 'Not Transmitting';
        }
    }

    formatUri(rpg: NodeRemoteProcessGroupStatusSnapshot): string {
        return rpg.statusSnapshot.targetUri;
    }

    formatSent(rpg: NodeRemoteProcessGroupStatusSnapshot): string {
        return rpg.statusSnapshot.sent;
    }

    formatReceived(rpg: NodeRemoteProcessGroupStatusSnapshot): string {
        return rpg.statusSnapshot.received;
    }

    getTransmissionStatusIcon(rpg: NodeRemoteProcessGroupStatusSnapshot): string {
        if (rpg.statusSnapshot.transmissionStatus === 'Transmitting') {
            return 'transmitting nifi-success-default fa fa-bullseye';
        } else {
            return 'not-transmitting icon icon-transmit-false primary-color';
        }
    }
}
