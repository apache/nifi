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

import { Component, OnDestroy, OnInit, inject } from '@angular/core';
import { Store } from '@ngrx/store';
import {
    selectRegistryClient,
    selectRegistryClientIdFromRoute,
    selectRegistryClientsState,
    selectSingleEditedRegistryClient
} from '../../state/registry-clients/registry-clients.selectors';
import {
    loadRegistryClients,
    navigateToEditRegistryClient,
    openConfigureRegistryClientDialog,
    openNewRegistryClientDialog,
    promptRegistryClientDeletion,
    resetRegistryClientsState,
    selectClient,
    clearRegistryClientBulletins
} from '../../state/registry-clients/registry-clients.actions';
import { RegistryClientsState } from '../../state/registry-clients';
import { initialState } from '../../state/registry-clients/registry-clients.reducer';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { NiFiState } from '../../../../state';
import { filter, switchMap, take } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { RegistryClientEntity } from '../../../../state/shared';
import { AsyncPipe } from '@angular/common';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';
import { MatIconButton } from '@angular/material/button';
import { RegistryClientTable } from './registry-client-table/registry-client-table.component';
import { ComponentType, NiFiCommon } from '@nifi/shared';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';

@Component({
    selector: 'registry-clients',
    templateUrl: './registry-clients.component.html',
    imports: [AsyncPipe, NgxSkeletonLoaderComponent, MatIconButton, RegistryClientTable],
    styleUrls: ['./registry-clients.component.scss']
})
export class RegistryClients implements OnInit, OnDestroy {
    private store = inject<Store<NiFiState>>(Store);
    private nifiCommon = inject(NiFiCommon);

    registryClientsState$ = this.store.select(selectRegistryClientsState);
    selectedRegistryClientId$ = this.store.select(selectRegistryClientIdFromRoute);
    currentUser$ = this.store.select(selectCurrentUser);

    constructor() {
        this.store
            .select(selectSingleEditedRegistryClient)
            .pipe(
                filter((id: string) => id != null),
                switchMap((id: string) =>
                    this.store.select(selectRegistryClient(id)).pipe(
                        filter((entity) => entity != null),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((entity) => {
                if (entity) {
                    this.store.dispatch(
                        openConfigureRegistryClientDialog({
                            request: {
                                registryClient: entity
                            }
                        })
                    );
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadRegistryClients());
    }

    isInitialLoading(state: RegistryClientsState): boolean {
        // using the current timestamp to detect the initial load event
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    openNewRegistryClientDialog(): void {
        this.store.dispatch(openNewRegistryClientDialog());
    }

    refreshRegistryClientListing(): void {
        this.store.dispatch(loadRegistryClients());
    }

    selectRegistryClient(entity: RegistryClientEntity): void {
        this.store.dispatch(
            selectClient({
                request: {
                    id: entity.id
                }
            })
        );
    }

    configureRegistryClient(entity: RegistryClientEntity): void {
        this.store.dispatch(
            navigateToEditRegistryClient({
                id: entity.id
            })
        );
    }

    deleteRegistryClient(entity: RegistryClientEntity): void {
        this.store.dispatch(
            promptRegistryClientDeletion({
                request: {
                    registryClient: entity
                }
            })
        );
    }

    viewRegistryClientDocumentation(entity: RegistryClientEntity): void {
        this.store.dispatch(
            navigateToComponentDocumentation({
                request: {
                    backNavigation: {
                        route: ['/settings', 'registry-clients', entity.id],
                        routeBoundary: ['/documentation'],
                        context: 'Registry Client'
                    },
                    parameters: {
                        componentType: ComponentType.FlowRegistryClient,
                        type: entity.component.type,
                        group: entity.component.bundle.group,
                        artifact: entity.component.bundle.artifact,
                        version: entity.component.bundle.version
                    }
                }
            })
        );
    }

    clearBulletinsRegistryClient(entity: RegistryClientEntity): void {
        // Get the most recent bulletin timestamp from the entity's bulletins
        // This will be reconstructed from the time-only string to a full timestamp
        const fromTimestamp = this.nifiCommon.getMostRecentBulletinTimestamp(entity.bulletins || []);
        if (fromTimestamp === null) {
            return; // no bulletins to clear
        }

        this.store.dispatch(
            clearRegistryClientBulletins({
                request: {
                    uri: entity.uri,
                    fromTimestamp,
                    componentId: entity.id,
                    componentType: ComponentType.FlowRegistryClient
                }
            })
        );
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetRegistryClientsState());
    }
}
