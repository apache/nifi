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
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatTooltipModule } from '@angular/material/tooltip';
import { Store } from '@ngrx/store';
import { CloseOnEscapeDialog, ComponentContext, ComponentType, NiFiCommon } from '@nifi/shared';
import { CanvasState } from '../../../state';
import { ComponentConnectionsDialogRequest, ConnectionEntity } from '../../../state/flow';
import { CanvasUtils } from '../../../service/canvas-utils.service';
import { enterProcessGroup, navigateToComponent } from '../../../state/flow/flow.actions';

/**
 * One end of a connection, with enough information to render a cell and navigate to it.
 * - {@code id}: the component's own id.
 * - {@code groupId}: the id of the process group that directly contains the component.
 * - {@code type}: the component type, used to tell {@code navigateToComponent} what it's looking at.
 * - {@code name}: the component name, or {@code null} when the current user cannot read this end, in
 *   which case the cell renders an "Unauthorized" placeholder. The cell stays clickable either way,
 *   the same as an unreadable component the user can select on the canvas.
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
    imports: [ComponentContext, MatButtonModule, MatDialogModule, MatSortModule, MatTableModule, MatTooltipModule],
    templateUrl: './component-connections-dialog.component.html',
    styleUrls: ['./component-connections-dialog.component.scss']
})
export class ComponentConnectionsDialog extends CloseOnEscapeDialog {
    private dialogRequest = inject<ComponentConnectionsDialogRequest>(MAT_DIALOG_DATA);
    private componentConnectionsDialogRef = inject<MatDialogRef<ComponentConnectionsDialog>>(MatDialogRef);
    private store = inject<Store<CanvasState>>(Store);
    private canvasUtils = inject(CanvasUtils);
    private nifiCommon = inject(NiFiCommon);

    // Maps the string type returned by the NiFi API to the ComponentType enum used for navigation.
    private static readonly TYPE_MAP: Record<string, ComponentType> = {
        PROCESSOR: ComponentType.Processor,
        INPUT_PORT: ComponentType.InputPort,
        OUTPUT_PORT: ComponentType.OutputPort,
        REMOTE_INPUT_PORT: ComponentType.RemoteProcessGroup,
        REMOTE_OUTPUT_PORT: ComponentType.RemoteProcessGroup,
        FUNNEL: ComponentType.Funnel
    };

    // rendered in place of a name the current user cannot read, or that the component does not have
    private static readonly UNAUTHORIZED_LABEL = 'Unauthorized';
    private static readonly UNNAMED_CONNECTION_LABEL = 'Connection';

    readonly displayedColumns: string[] = [
        'sourceProcessGroup',
        'sourceComponent',
        'connection',
        'destinationProcessGroup',
        'destinationComponent'
    ];
    readonly componentId: string = this.dialogRequest.componentId;
    // the name the current user can read, or the component id when they cannot; component-context
    // renders whatever it is given
    readonly componentName: string;
    readonly componentType: ComponentType = this.dialogRequest.componentType;
    readonly title: string;
    readonly emptyMessage: string;
    readonly rows: ComponentConnectionRow[];
    readonly dialogRequestGroupId: string = this.dialogRequest.groupId;
    readonly processGroupType = ComponentType.ProcessGroup;
    readonly remoteProcessGroupType = ComponentType.RemoteProcessGroup;
    readonly connectionType = ComponentType.Connection;

    readonly initialSortColumn = 'connection';
    readonly initialSortDirection: 'asc' | 'desc' = 'asc';
    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };
    readonly dataSource: MatTableDataSource<ComponentConnectionRow> = new MatTableDataSource<ComponentConnectionRow>();

    constructor() {
        super();

        const upstream = this.dialogRequest.direction === 'upstream';
        this.componentName = this.dialogRequest.componentName;
        this.title = upstream ? 'Upstream Connections' : 'Downstream Connections';
        this.emptyMessage = upstream ? 'No upstream connections were found.' : 'No downstream connections were found.';
        this.rows = this.dialogRequest.connections.map((connection: ConnectionEntity) => this.buildRow(connection));
        this.dataSource.data = this.sortRows(this.rows, this.activeSort);
    }

    sortData(sort: Sort): void {
        this.activeSort = sort;
        this.dataSource.data = this.sortRows(this.dataSource.data, sort);
    }

    /**
     * Orders the rows by the text each column actually renders, so that a row whose name is unreadable
     * or absent sorts under the placeholder the user sees rather than under an empty key.
     *
     * @param data the rows to sort
     * @param sort the active column and direction
     * @returns the sorted rows
     */
    sortRows(data: ComponentConnectionRow[], sort: Sort): ComponentConnectionRow[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal: number;
            switch (sort.active) {
                case 'sourceProcessGroup':
                    retVal = this.nifiCommon.compareString(
                        this.resolveGroupName(a.source.groupId),
                        this.resolveGroupName(b.source.groupId)
                    );
                    break;
                case 'sourceComponent':
                    retVal = this.nifiCommon.compareString(
                        this.formatComponentName(a.source),
                        this.formatComponentName(b.source)
                    );
                    break;
                case 'connection':
                    retVal = this.nifiCommon.compareString(this.formatConnectionName(a), this.formatConnectionName(b));
                    break;
                case 'destinationProcessGroup':
                    retVal = this.nifiCommon.compareString(
                        this.resolveGroupName(a.destination.groupId),
                        this.resolveGroupName(b.destination.groupId)
                    );
                    break;
                case 'destinationComponent':
                    retVal = this.nifiCommon.compareString(
                        this.formatComponentName(a.destination),
                        this.formatComponentName(b.destination)
                    );
                    break;
                default:
                    return 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    /**
     * Returns the name rendered for an endpoint, which is a placeholder when the connection cannot be
     * read and the endpoint has no name to show.
     *
     * @param endpoint the source or destination endpoint
     * @returns the endpoint name to render and sort on
     */
    formatComponentName(endpoint: ConnectionEndpoint): string {
        return endpoint.name ?? ComponentConnectionsDialog.UNAUTHORIZED_LABEL;
    }

    /**
     * Returns the tooltip of an endpoint, which falls back to the id of a component the current user
     * cannot read, since the placeholder rendered in its place identifies nothing on its own.
     *
     * @param endpoint the source or destination endpoint
     * @returns the endpoint name, or its id when unreadable
     */
    componentTooltip(endpoint: ConnectionEndpoint): string {
        return endpoint.name ?? endpoint.id;
    }

    /**
     * Returns the name rendered for a connection, which is a placeholder when the connection has
     * neither a name nor relationships to name it by.
     *
     * @param row the row of the connection
     * @returns the connection name to render and sort on
     */
    formatConnectionName(row: ComponentConnectionRow): string {
        return row.name ?? ComponentConnectionsDialog.UNNAMED_CONNECTION_LABEL;
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
     * Determines whether there is somewhere to go for the given process group. The only group there is
     * not is the one that both defines these connections and is already open on the canvas: selecting
     * it within itself is not a place the canvas can go, and the user is looking at it already.
     *
     * The group that defines the connections is the parent group when a port's connections cross its
     * own group's boundary, and the parent is a group the user can still be taken to.
     *
     * @param groupId the process group id to check
     * @returns whether the group can be navigated to
     */
    isNavigableProcessGroup(groupId: string): boolean {
        return !(groupId === this.dialogRequestGroupId && groupId === this.canvasUtils.getProcessGroupId());
    }

    /**
     * Navigates to the process group at one end of a connection, then closes the dialog. The group that
     * defines the connections is entered, since it holds no component of its own to select, while any
     * other group is a component of it and is selected there - as a Remote Process Group when the end is
     * one of its remote ports.
     *
     * @param endpoint the source or destination endpoint whose group should be navigated to
     */
    navigateToProcessGroup(endpoint: ConnectionEndpoint): void {
        if (endpoint.groupId === this.dialogRequestGroupId) {
            this.store.dispatch(
                enterProcessGroup({
                    request: {
                        id: endpoint.groupId
                    }
                })
            );
            this.componentConnectionsDialogRef.close();
            return;
        }

        this.navigateTo(endpoint.groupId, this.dialogRequestGroupId, this.processGroupTypeOf(endpoint));
    }

    /**
     * Returns the type the process group at one end of a connection is navigated to as.
     *
     * @param endpoint the source or destination endpoint
     * @returns Remote Process Group when the end is a remote port, Process Group otherwise
     */
    processGroupTypeOf(endpoint: ConnectionEndpoint): ComponentType {
        return this.isRemoteProcessGroupPort(endpoint) ? this.remoteProcessGroupType : this.processGroupType;
    }

    /**
     * Maps a Process Group ID value to its name.
     *
     * @param groupId the uuid of the process group
     * @returns string name of the process group
     */
    resolveGroupName(groupId: string): string {
        return this.dialogRequest.groupIdToName.get(groupId) ?? groupId;
    }

    /**
     * Determines whether the endpoint is a port inside a Remote Process Group. Remote ports
     * are not rendered as separate selectable elements on the current graph, so they should
     * not be linked from the connections table.
     *
     * @param endpoint the source or destination endpoint to check
     * @returns whether the endpoint is a remote port in a Remote Process Group
     */
    isRemoteProcessGroupPort(endpoint: ConnectionEndpoint): boolean {
        return endpoint.type === ComponentType.RemoteProcessGroup;
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
            source: this.buildEndpoint(
                connection.sourceId,
                connection.sourceGroupId,
                connection.sourceType,
                connection.component?.source?.name
            ),
            destination: this.buildEndpoint(
                connection.destinationId,
                connection.destinationGroupId,
                connection.destinationType,
                connection.component?.destination?.name
            )
        };
    }

    private buildEndpoint(id: string, groupId: string, type: string, nameOnConnection?: string): ConnectionEndpoint {
        return {
            id,
            groupId,
            type: this.mapComponentType(type),
            name:
                this.mapComponentType(type) === ComponentType.Funnel
                    ? 'Funnel'
                    : this.resolveComponentName(id, nameOnConnection)
        };
    }

    /**
     * Resolves the name of one end of a connection from that component's own read permission, with no
     * regard for the other end or for the connection between them. A connection is readable only when
     * the current user can read both of its ends, so the names it carries disappear for both ends as
     * soon as either one is unreadable; they are only a fallback here.
     *
     * The components of every group these connections reach into were listed with their own
     * permissions, and each readable one is named there. An end that is not is either unreadable or a
     * port inside a Remote Process Group, whose group lists nothing of its own - the name the
     * connection carries covers the latter, and is only ever present when both ends are readable.
     *
     * @param id the id of the component at this end of the connection
     * @param nameOnConnection the name the connection reports for this end, when it can be read
     * @returns the name to render, or null when this end is unreadable
     */
    private resolveComponentName(id: string, nameOnConnection?: string): string | null {
        return this.dialogRequest.componentIdToName.get(id) ?? nameOnConnection ?? null;
    }

    private mapComponentType(type: string): ComponentType {
        return ComponentConnectionsDialog.TYPE_MAP[type] ?? ComponentType.Connector;
    }
}
