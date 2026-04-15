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

import { Component, OnInit, inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { AsyncPipe } from '@angular/common';
import { MatIconButton } from '@angular/material/button';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';
import { ComponentType, ConnectorEntity } from '@nifi/shared';
import { ConnectorsListingState } from '../../state';
import {
    selectConnectorIdFromRoute,
    selectConnectorsListingState
} from '../../state/connectors-listing/connectors-listing.selectors';
import {
    cancelConnectorDrain,
    loadConnectorsListing,
    navigateToConfigureConnector,
    navigateToManageAccessPolicies,
    navigateToViewConnector,
    navigateToViewConnectorDetails,
    openNewConnectorDialog,
    openRenameConnectorDialog,
    promptConnectorDeletion,
    promptDiscardConnectorConfig,
    promptDrainConnector,
    selectConnector,
    startConnector,
    stopConnector
} from '../../state/connectors-listing/connectors-listing.actions';
import { promptPurgeConnector } from '../../state/purge-connector/purge-connector.actions';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { initialState } from '../../state/connectors-listing/connectors-listing.reducer';
import { NiFiState } from '../../../../state';
import { ConnectorTable } from '../connector-table/connector-table.component';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';
import { loadExtensionTypesForConnectors } from '../../../../state/extension-types/extension-types.actions';
import { ErrorContextKey } from '../../../../state/error';
import { ContextErrorBanner } from '../../../../ui/common/context-error-banner/context-error-banner.component';

@Component({
    selector: 'connectors-listing',
    standalone: true,
    templateUrl: './connectors-listing.component.html',
    imports: [AsyncPipe, NgxSkeletonLoaderComponent, MatIconButton, ConnectorTable, ContextErrorBanner]
})
export class ConnectorsListing implements OnInit {
    private store = inject<Store<NiFiState>>(Store);

    protected readonly ErrorContextKey = ErrorContextKey;

    connectorsListingState$ = this.store.select(selectConnectorsListingState);
    selectedConnectorId$ = this.store.select(selectConnectorIdFromRoute);
    currentUser$ = this.store.select(selectCurrentUser);
    flowConfiguration$ = this.store.select(selectFlowConfiguration);

    ngOnInit(): void {
        this.store.dispatch(loadExtensionTypesForConnectors());
        this.store.dispatch(loadConnectorsListing());
    }

    isInitialLoading(state: ConnectorsListingState): boolean {
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    openNewConnectorDialog(): void {
        this.store.dispatch(openNewConnectorDialog());
    }

    refreshConnectorListing(): void {
        this.store.dispatch(loadConnectorsListing());
    }

    selectConnector(entity: ConnectorEntity): void {
        this.store.dispatch(
            selectConnector({
                request: { id: entity.id }
            })
        );
    }

    viewConnector(entity: ConnectorEntity): void {
        this.store.dispatch(
            navigateToViewConnector({
                request: {
                    connectorId: entity.id,
                    processGroupId: entity.component.managedProcessGroupId
                }
            })
        );
    }

    viewDetails(connector: ConnectorEntity): void {
        this.store.dispatch(navigateToViewConnectorDetails({ id: connector.id }));
    }

    configureConnector(entity: ConnectorEntity): void {
        this.store.dispatch(
            navigateToConfigureConnector({
                request: {
                    id: entity.id,
                    connector: entity
                }
            })
        );
    }

    renameConnector(entity: ConnectorEntity): void {
        this.store.dispatch(openRenameConnectorDialog({ connector: entity }));
    }

    startConnector(entity: ConnectorEntity): void {
        this.store.dispatch(startConnector({ request: { connector: entity } }));
    }

    stopConnector(entity: ConnectorEntity): void {
        this.store.dispatch(stopConnector({ request: { connector: entity } }));
    }

    deleteConnector(entity: ConnectorEntity): void {
        this.store.dispatch(promptConnectorDeletion({ request: { connector: entity } }));
    }

    discardConnectorConfig(entity: ConnectorEntity): void {
        this.store.dispatch(promptDiscardConnectorConfig({ request: { connector: entity } }));
    }

    drainConnector(entity: ConnectorEntity): void {
        this.store.dispatch(promptDrainConnector({ request: { connector: entity } }));
    }

    cancelDrainConnector(entity: ConnectorEntity): void {
        this.store.dispatch(cancelConnectorDrain({ request: { connector: entity } }));
    }

    purgeConnector(entity: ConnectorEntity): void {
        this.store.dispatch(promptPurgeConnector({ request: { connector: entity } }));
    }

    navigateToManageAccessPolicies(entity: ConnectorEntity): void {
        this.store.dispatch(navigateToManageAccessPolicies({ id: entity.id }));
    }

    viewDocumentation(entity: ConnectorEntity): void {
        const bundle = entity.component.bundle;
        this.store.dispatch(
            navigateToComponentDocumentation({
                request: {
                    backNavigation: {
                        route: ['/connectors', entity.id],
                        routeBoundary: ['/documentation'],
                        context: 'connector'
                    },
                    parameters: {
                        componentType: ComponentType.Connector,
                        type: entity.component.type,
                        group: bundle.group,
                        artifact: bundle.artifact,
                        version: bundle.version
                    }
                }
            })
        );
    }
}
