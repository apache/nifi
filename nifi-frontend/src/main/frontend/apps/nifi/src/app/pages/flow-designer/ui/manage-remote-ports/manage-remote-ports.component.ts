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

import { Component, OnDestroy } from '@angular/core';
import { Store } from '@ngrx/store';
import { filter, switchMap, take, tap } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
    selectRemotePortsState,
    selectPort,
    selectPortIdFromRoute,
    selectPorts,
    selectRpg,
    selectRpgIdFromRoute,
    selectSingleEditedPort
} from '../../state/manage-remote-ports/manage-remote-ports.selectors';
import { RemotePortsState, PortSummary } from '../../state/manage-remote-ports';
import {
    loadRemotePorts,
    navigateToEditPort,
    openConfigureRemotePortDialog,
    resetRemotePortsState,
    selectRemotePort,
    startRemotePortTransmission,
    stopRemotePortTransmission
} from '../../state/manage-remote-ports/manage-remote-ports.actions';
import { initialState } from '../../state/manage-remote-ports/manage-remote-ports.reducer';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { NiFiState } from '../../../../state';
import { isDefinedAndNotNull, NiFiCommon, TextTip } from '@nifi/shared';
import { MatTableDataSource } from '@angular/material/table';
import { Sort } from '@angular/material/sort';
import { concatLatestFrom } from '@ngrx/operators';
import {
    selectFlowConfiguration,
    selectTimeOffset
} from '../../../../state/flow-configuration/flow-configuration.selectors';
import { selectAbout } from '../../../../state/about/about.selectors';

@Component({
    templateUrl: './manage-remote-ports.component.html',
    styleUrls: ['./manage-remote-ports.component.scss'],
    standalone: false
})
export class ManageRemotePorts implements OnDestroy {
    initialSortColumn: 'name' | 'type' | 'tasks' | 'count' | 'size' | 'duration' | 'compression' | 'actions' = 'name';
    initialSortDirection: 'asc' | 'desc' = 'asc';
    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };
    portsState$ = this.store.select(selectRemotePortsState);
    selectedRpgId$ = this.store.select(selectRpgIdFromRoute);
    selectedPortId!: string;
    currentUser$ = this.store.select(selectCurrentUser);
    flowConfiguration$ = this.store.select(selectFlowConfiguration).pipe(isDefinedAndNotNull());
    displayedColumns: string[] = [
        'moreDetails',
        'name',
        'type',
        'tasks',
        'count',
        'size',
        'duration',
        'compression',
        'actions'
    ];
    dataSource: MatTableDataSource<PortSummary> = new MatTableDataSource<PortSummary>([]);
    protected readonly TextTip = TextTip;

    private currentRpgId!: string;
    protected currentRpg: any | null = null;

    constructor(
        private store: Store<NiFiState>,
        private nifiCommon: NiFiCommon
    ) {
        // load the ports after the flow configuration `timeOffset` and about `timezone` are loaded into the store
        this.store
            .select(selectTimeOffset)
            .pipe(
                isDefinedAndNotNull(),
                switchMap(() => this.store.select(selectAbout)),
                isDefinedAndNotNull(),
                switchMap(() => this.store.select(selectRpgIdFromRoute)),
                tap((rpgId) => (this.currentRpgId = rpgId)),
                takeUntilDestroyed()
            )
            .subscribe((rpgId) => {
                this.store.dispatch(
                    loadRemotePorts({
                        request: {
                            rpgId
                        }
                    })
                );
            });

        // track selection using the port id from the route
        this.store
            .select(selectPortIdFromRoute)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((portId) => {
                this.selectedPortId = portId;
            });

        // data for table
        this.store
            .select(selectPorts)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((ports) => {
                this.dataSource = new MatTableDataSource<PortSummary>(this.sortEntities(ports, this.activeSort));
            });

        // the current RPG Entity
        this.store
            .select(selectRpg)
            .pipe(
                isDefinedAndNotNull(),
                tap((rpg) => (this.currentRpg = rpg)),
                takeUntilDestroyed()
            )
            .subscribe();

        // handle editing remote port deep link
        this.store
            .select(selectSingleEditedPort)
            .pipe(
                isDefinedAndNotNull(),
                switchMap((id: string) =>
                    this.store.select(selectPort(id)).pipe(
                        filter((entity) => entity != null),
                        take(1)
                    )
                ),
                concatLatestFrom(() => [this.store.select(selectRpg).pipe(isDefinedAndNotNull())]),
                takeUntilDestroyed()
            )
            .subscribe(([entity, rpg]) => {
                if (entity) {
                    this.store.dispatch(
                        openConfigureRemotePortDialog({
                            request: {
                                id: entity.id,
                                port: entity,
                                rpg
                            }
                        })
                    );
                }
            });
    }

    isInitialLoading(state: RemotePortsState): boolean {
        // using the current timestamp to detect the initial load event
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    refreshManageRemotePortsListing(): void {
        this.store.dispatch(
            loadRemotePorts({
                request: {
                    rpgId: this.currentRpgId
                }
            })
        );
    }

    formatName(entity: PortSummary): string {
        return entity.name;
    }

    formatTasks(entity: PortSummary): string {
        return entity.concurrentlySchedulableTaskCount ? `${entity.concurrentlySchedulableTaskCount}` : 'No value set';
    }

    formatCount(entity: PortSummary): string {
        if (!this.isCountBlank(entity)) {
            return `${entity.batchSettings.count}`;
        }
        return 'No value set';
    }

    isCountBlank(entity: PortSummary): boolean {
        return this.nifiCommon.isUndefined(entity.batchSettings.count);
    }

    formatSize(entity: PortSummary): string {
        if (!this.isSizeBlank(entity)) {
            return `${entity.batchSettings.size}`;
        }
        return 'No value set';
    }

    isSizeBlank(entity: PortSummary): boolean {
        return this.nifiCommon.isBlank(entity.batchSettings.size);
    }

    formatDuration(entity: PortSummary): string {
        if (!this.isDurationBlank(entity)) {
            return `${entity.batchSettings.duration}`;
        }
        return 'No value set';
    }

    isDurationBlank(entity: PortSummary): boolean {
        return this.nifiCommon.isBlank(entity.batchSettings.duration);
    }

    formatCompression(entity: PortSummary): string {
        return entity.useCompression ? 'Yes' : 'No';
    }

    formatType(entity: PortSummary): string {
        return entity.type || '';
    }

    configureClicked(port: PortSummary): void {
        this.store.dispatch(
            navigateToEditPort({
                id: port.id
            })
        );
    }

    hasComments(entity: PortSummary): boolean {
        return !this.nifiCommon.isBlank(entity.comments);
    }

    portExists(entity: PortSummary): boolean {
        return !entity.exists;
    }

    toggleTransmission(port: PortSummary): void {
        if (this.currentRpg) {
            if (port.transmitting) {
                this.store.dispatch(
                    stopRemotePortTransmission({
                        request: {
                            rpg: this.currentRpg,
                            port
                        }
                    })
                );
            } else {
                if (port.connected && port.exists) {
                    this.store.dispatch(
                        startRemotePortTransmission({
                            request: {
                                rpg: this.currentRpg,
                                port
                            }
                        })
                    );
                }
            }
        }
    }

    select(entity: PortSummary): void {
        this.store.dispatch(
            selectRemotePort({
                request: {
                    rpgId: this.currentRpgId,
                    id: entity.id
                }
            })
        );
    }

    isSelected(entity: any): boolean {
        if (this.selectedPortId) {
            return entity.id == this.selectedPortId;
        }
        return false;
    }

    sortData(sort: Sort) {
        this.activeSort = sort;
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private sortEntities(data: PortSummary[], sort: Sort): PortSummary[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;

            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(this.formatName(a), this.formatName(b));
                    break;
                case 'type':
                    retVal = this.nifiCommon.compareString(this.formatType(a), this.formatType(b));
                    break;
                case 'tasks':
                    retVal = this.nifiCommon.compareString(this.formatTasks(a), this.formatTasks(b));
                    break;
                case 'compression':
                    retVal = this.nifiCommon.compareString(this.formatCompression(a), this.formatCompression(b));
                    break;
                case 'count':
                    retVal = this.nifiCommon.compareString(this.formatCount(a), this.formatCount(b));
                    break;
                case 'size':
                    retVal = this.nifiCommon.compareString(this.formatSize(a), this.formatSize(b));
                    break;
                case 'duration':
                    retVal = this.nifiCommon.compareString(this.formatDuration(a), this.formatDuration(b));
                    break;
                default:
                    return 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetRemotePortsState());
    }
}
