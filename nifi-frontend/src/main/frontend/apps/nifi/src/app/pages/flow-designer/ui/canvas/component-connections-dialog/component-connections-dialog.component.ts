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

import { Component, inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatTableModule } from '@angular/material/table';
import { MatTooltipModule } from '@angular/material/tooltip';
import { Store } from '@ngrx/store';
import { CloseOnEscapeDialog, ComponentType } from '@nifi/shared';
import { CanvasState } from '../../../state';
import { ComponentConnectionsDialogRequest, ConnectionEntity } from '../../../state/flow';
import { CanvasUtils } from '../../../service/canvas-utils.service';
import { navigateToComponent } from '../../../state/flow/flow.actions';
import { selectProcessGroupIdToNameMap } from '../../../state/flow/flow.selectors';

/**
 * One end of a connection, with enough information to render a cell and navigate to it.
 * - {@code id}: the component's own id.
 * - {@code groupId}: the id of the process group that directly contains the component.
 * - {@code type}: the component type, used to tell {@code navigateToComponent} what it's looking at.
 * - {@code name}: the component name, or {@code null} when the current user cannot read the
 *   connection, in which case the cell renders an "Unauthorized" placeholder and is not clickable.
 */
export interface ConnectionEndpoint {
    id: string;
    groupId: string;
    type: ComponentType;
    name: string | null;
}

/**
 * Row in the connections table.
 * - {@code id}: the connection id.
 * - {@code name}: the connection name, or the relationships it carries when it has no name.
 *   {@code null} when it has neither, so the cell renders an "Unnamed" placeholder.
 *
 * Both ends are listed, along with each end's process group, rather than only the far end. When the
 * selected component is a Process Group or Remote Process Group the connection actually terminates at
 * a port inside it, and which group and port that is matters as much as the component on the other side.
 */
export interface ComponentConnectionRow {
    id: string;
    name: string | null;
    source: ConnectionEndpoint;
    destination: ConnectionEndpoint;
}

/**
 * Lists the connections attached to a component in one direction. For most components those
 * connections are already drawn on the canvas, so this is a way to reach one whose other end sits
 * somewhere else entirely. For an Input Port's upstream connections and an Output Port's downstream
 * connections it is the only way, since those are defined in the parent process group and are not drawn
 * alongside the port at all. Each of the 5 cells in a row is independently clickable and navigates
 * to the process group, component, or connection it represents.
 */
@Component({
    selector: 'component-connections-dialog',
    imports: [MatButtonModule, MatDialogModule, MatTableModule, MatTooltipModule],
    templateUrl: './component-connections-dialog.component.html',
    styleUrls: ['./component-connections-dialog.component.scss']
})
export class ComponentConnectionsDialog extends CloseOnEscapeDialog {
    private dialogRequest = inject<ComponentConnectionsDialogRequest>(MAT_DIALOG_DATA);
    private componentConnectionsDialogRef = inject<MatDialogRef<ComponentConnectionsDialog>>(MatDialogRef);
    private store = inject<Store<CanvasState>>(Store);
    private canvasUtils = inject(CanvasUtils);
    // Signal-based snapshot — reads current value synchronously, no manual subscribe/unsubscribe.
    private groupIdToName = this.store.selectSignal(selectProcessGroupIdToNameMap);

    // Maps the string type returned by the NiFi API to the ComponentType enum used for navigation.
    private static readonly TYPE_MAP: Record<string, ComponentType> = {
        PROCESSOR: ComponentType.Processor,
        INPUT_PORT: ComponentType.InputPort,
        OUTPUT_PORT: ComponentType.OutputPort,
        REMOTE_INPUT_PORT: ComponentType.RemoteProcessGroup,
        REMOTE_OUTPUT_PORT: ComponentType.RemoteProcessGroup,
        FUNNEL: ComponentType.Funnel
    };

    readonly displayedColumns: string[] = [
        'sourceProcessGroup',
        'sourceComponent',
        'connection',
        'destinationProcessGroup',
        'destinationComponent'
    ];
    readonly componentName: string;
    readonly componentType: ComponentType = this.dialogRequest.componentType;
    readonly title: string;
    readonly emptyMessage: string;
    readonly rows: ComponentConnectionRow[];
    readonly dialogRequestGroupId: string = this.dialogRequest.groupId;
    readonly processGroupType = ComponentType.ProcessGroup;
    readonly connectionType = ComponentType.Connection;

    constructor() {
        super();

        const upstream = this.dialogRequest.direction === 'upstream';
        this.componentName = this.dialogRequest.componentName;
        this.title = upstream ? 'Upstream Connections' : 'Downstream Connections';
        this.emptyMessage = upstream ? 'No upstream connections were found.' : 'No downstream connections were found.';
        this.rows = this.dialogRequest.connections.map((connection: ConnectionEntity) => this.buildRow(connection));
    }

    /**
     * Navigates to and selects the given component, then closes the dialog.
     *
     * @param id the id of the component to navigate to
     * @param processGroupId the id of the process group that should be entered to find the component
     * @param type the type of component being navigated to
     */
    navigateTo(id: string, processGroupId: string, type: ComponentType): void {
        this.store.dispatch(
            navigateToComponent({
                request: {
                    id,
                    processGroupId,
                    type
                }
            })
        );
        this.componentConnectionsDialogRef.close();
    }

    /**
     * Maps a Process Group ID value to its name.
     *
     * @param groupId the uuid of the process group
     * @returns string name of the process group
     */
    resolveGroupName(groupId: string): string {
        return this.groupIdToName().get(groupId) ?? groupId; // note the () — calling the signal
    }

    /**
     * Resolves the flowfont icon class that represents the given component type, matching the icons
     * used for the same components on the canvas.
     *
     * @param type the type of the component
     * @returns the icon class to render ahead of the component name
     */
    componentIcon(type: ComponentType): string {
        switch (type) {
            case ComponentType.Processor:
                return 'icon-processor';
            case ComponentType.InputPort:
                return 'icon-port-in';
            case ComponentType.OutputPort:
                return 'icon-port-out';
            case ComponentType.Funnel:
                return 'icon-funnel';
            case ComponentType.ProcessGroup:
                return 'icon-group';
            case ComponentType.RemoteProcessGroup:
                return 'icon-group-remote';
            case ComponentType.Connection:
                return 'icon-connect';
            default:
                return 'icon-drop';
        }
    }

    private buildRow(connection: ConnectionEntity): ComponentConnectionRow {
        const name = connection.component ? this.canvasUtils.formatConnectionName(connection.component) : '';

        return {
            id: connection.id,
            name: name === '' ? null : name,
            source: {
                id: connection.sourceId,
                groupId: connection.sourceGroupId,
                type: this.mapComponentType(connection.sourceType),
                name: connection.component?.source?.name ?? null
            },
            destination: {
                id: connection.destinationId,
                groupId: connection.destinationGroupId,
                type: this.mapComponentType(connection.destinationType),
                name: connection.component?.destination?.name ?? null
            }
        };
    }

    private mapComponentType(type: string): ComponentType {
        return ComponentConnectionsDialog.TYPE_MAP[type] ?? ComponentType.Connector;
    }
}
