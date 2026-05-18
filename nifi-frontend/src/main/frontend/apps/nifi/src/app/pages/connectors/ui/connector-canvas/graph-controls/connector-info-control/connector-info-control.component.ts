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

import { Component, inject, input } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatExpansionPanel, MatExpansionPanelHeader, MatExpansionPanelTitle } from '@angular/material/expansion';
import { MatTooltip } from '@angular/material/tooltip';
import { Store } from '@ngrx/store';
import {
    ConnectorDetailHeader,
    ConnectorEntity,
    Storage,
    canReadConnector,
    canModifyConnector,
    canOperateConnector,
    isConnectorActionAllowed,
    getConnectorActionDisabledReason
} from '@nifi/shared';
import * as ConnectorCanvasEntityActions from '../../../../state/connector-canvas-entity/connector-canvas-entity.actions';
import {
    navigateToViewConnectorDetails,
    navigateToConfigureConnector
} from '../../../../state/connectors-listing/connectors-listing.actions';

@Component({
    selector: 'connector-info-control',
    standalone: true,
    imports: [
        ConnectorDetailHeader,
        MatButtonModule,
        MatExpansionPanel,
        MatExpansionPanelHeader,
        MatExpansionPanelTitle,
        MatTooltip
    ],
    templateUrl: './connector-info-control.component.html',
    styleUrls: ['./connector-info-control.component.scss']
})
export class ConnectorInfoControl {
    private store = inject(Store);
    private storage = inject(Storage);

    private static readonly CONTROL_VISIBILITY_KEY = 'graph-control-visibility';
    private static readonly CONNECTOR_KEY = 'connector-info-control';

    connectorCollapsed = false;

    connectorEntity = input<ConnectorEntity | null>(null);
    entitySaving = input<boolean>(false);

    constructor() {
        try {
            const item: { [key: string]: boolean } | null = this.storage.getItem(
                ConnectorInfoControl.CONTROL_VISIBILITY_KEY
            );
            if (item) {
                this.connectorCollapsed = item[ConnectorInfoControl.CONNECTOR_KEY] === false;
            }
        } catch (_e) {
            // likely could not parse item... ignoring
        }
    }

    toggleCollapsed(collapsed: boolean): void {
        this.connectorCollapsed = collapsed;

        let item: { [key: string]: boolean } | null = this.storage.getItem(ConnectorInfoControl.CONTROL_VISIBILITY_KEY);
        if (item == null) {
            item = {};
        }

        item[ConnectorInfoControl.CONNECTOR_KEY] = !this.connectorCollapsed;
        this.storage.setItem(ConnectorInfoControl.CONTROL_VISIBILITY_KEY, item);
    }

    canRead(): boolean {
        const entity = this.connectorEntity();
        return !!entity && canReadConnector(entity);
    }

    canConfigure(): boolean {
        const entity = this.connectorEntity();
        if (!entity || this.entitySaving()) {
            return false;
        }
        return canReadConnector(entity) && canModifyConnector(entity) && isConnectorActionAllowed(entity, 'CONFIGURE');
    }

    getConfigureDisabledReason(): string {
        const entity = this.connectorEntity();
        if (!entity) {
            return '';
        }
        return getConnectorActionDisabledReason(entity, 'CONFIGURE');
    }

    showConfigureButton(): boolean {
        const entity = this.connectorEntity();
        if (!entity) {
            return false;
        }
        return canReadConnector(entity) && canModifyConnector(entity);
    }

    showDrain(): boolean {
        const entity = this.connectorEntity();
        if (!entity) {
            return false;
        }
        return canOperateConnector(entity) && isConnectorActionAllowed(entity, 'DRAIN_FLOWFILES');
    }

    showCancelDrain(): boolean {
        const entity = this.connectorEntity();
        if (!entity) {
            return false;
        }
        return canOperateConnector(entity) && isConnectorActionAllowed(entity, 'CANCEL_DRAIN_FLOWFILES');
    }

    canDrain(): boolean {
        const entity = this.connectorEntity();
        if (!entity || this.entitySaving()) {
            return false;
        }
        return canOperateConnector(entity) && isConnectorActionAllowed(entity, 'DRAIN_FLOWFILES');
    }

    canCancelDrain(): boolean {
        const entity = this.connectorEntity();
        if (!entity || this.entitySaving()) {
            return false;
        }
        return canOperateConnector(entity) && isConnectorActionAllowed(entity, 'CANCEL_DRAIN_FLOWFILES');
    }

    showOperateButtons(): boolean {
        const entity = this.connectorEntity();
        return !!entity && canOperateConnector(entity);
    }

    canStart(): boolean {
        const entity = this.connectorEntity();
        if (!entity || this.entitySaving()) {
            return false;
        }
        return canOperateConnector(entity) && isConnectorActionAllowed(entity, 'START');
    }

    getStartDisabledReason(): string {
        const entity = this.connectorEntity();
        if (!entity) {
            return '';
        }
        return getConnectorActionDisabledReason(entity, 'START');
    }

    canStop(): boolean {
        const entity = this.connectorEntity();
        if (!entity || this.entitySaving()) {
            return false;
        }
        return canOperateConnector(entity) && isConnectorActionAllowed(entity, 'STOP');
    }

    getStopDisabledReason(): string {
        const entity = this.connectorEntity();
        if (!entity) {
            return '';
        }
        return getConnectorActionDisabledReason(entity, 'STOP');
    }

    viewDetails(): void {
        const entity = this.connectorEntity();
        if (entity) {
            this.store.dispatch(navigateToViewConnectorDetails({ id: entity.id }));
        }
    }

    configureConnector(): void {
        const entity = this.connectorEntity();
        if (entity) {
            this.store.dispatch(navigateToConfigureConnector({ request: { id: entity.id, connector: entity } }));
        }
    }

    drainConnector(): void {
        const entity = this.connectorEntity();
        if (entity) {
            this.store.dispatch(ConnectorCanvasEntityActions.promptDrainConnector({ connector: entity }));
        }
    }

    cancelDrain(): void {
        const entity = this.connectorEntity();
        if (entity) {
            this.store.dispatch(ConnectorCanvasEntityActions.cancelConnectorDrain({ connector: entity }));
        }
    }

    startConnector(): void {
        const entity = this.connectorEntity();
        if (entity) {
            this.store.dispatch(ConnectorCanvasEntityActions.startConnector({ connector: entity }));
        }
    }

    stopConnector(): void {
        const entity = this.connectorEntity();
        if (entity) {
            this.store.dispatch(ConnectorCanvasEntityActions.stopConnector({ connector: entity }));
        }
    }
}
