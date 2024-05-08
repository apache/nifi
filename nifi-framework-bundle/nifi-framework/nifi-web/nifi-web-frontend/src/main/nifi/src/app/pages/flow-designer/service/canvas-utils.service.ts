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

import { DestroyRef, inject, Injectable, Type } from '@angular/core';
import * as d3 from 'd3';
import { humanizer, Humanizer } from 'humanize-duration';
import { Store } from '@ngrx/store';
import { CanvasState } from '../state';
import {
    selectBreadcrumbs,
    selectCanvasPermissions,
    selectConnections,
    selectCopiedSnippet,
    selectCurrentParameterContext,
    selectCurrentProcessGroupId,
    selectParentProcessGroupId
} from '../state/flow/flow.selectors';
import { initialState as initialFlowState } from '../state/flow/flow.reducer';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { BulletinsTip } from '../../../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { BreadcrumbEntity, Position } from '../state/shared';
import { ComponentType, ParameterContextReferenceEntity, Permissions } from '../../../state/shared';
import { NiFiCommon } from '../../../service/nifi-common.service';
import { CurrentUser } from '../../../state/current-user';
import { initialState as initialUserState } from '../../../state/current-user/current-user.reducer';
import { selectCurrentUser } from '../../../state/current-user/current-user.selectors';
import { FlowConfiguration } from '../../../state/flow-configuration';
import { initialState as initialFlowConfigurationState } from '../../../state/flow-configuration/flow-configuration.reducer';
import { selectFlowConfiguration } from '../../../state/flow-configuration/flow-configuration.selectors';
import { CopiedSnippet, VersionControlInformation } from '../state/flow';
import { Overlay, OverlayRef } from '@angular/cdk/overlay';
import { ComponentPortal } from '@angular/cdk/portal';

@Injectable({
    providedIn: 'root'
})
export class CanvasUtils {
    private static readonly TWO_PI: number = 2 * Math.PI;

    private destroyRef = inject(DestroyRef);

    private trimLengthCaches: Map<string, Map<string, Map<number, number>>> = new Map();
    private currentProcessGroupId: string = initialFlowState.id;
    private parentProcessGroupId: string | null = initialFlowState.flow.processGroupFlow.parentGroupId;
    private canvasPermissions: Permissions = initialFlowState.flow.permissions;
    private currentUser: CurrentUser = initialUserState.user;
    private currentParameterContext: ParameterContextReferenceEntity | null =
        initialFlowState.flow.processGroupFlow.parameterContext;
    private flowConfiguration: FlowConfiguration | null = initialFlowConfigurationState.flowConfiguration;
    private connections: any[] = [];
    private breadcrumbs: BreadcrumbEntity | null = null;
    private copiedSnippet: CopiedSnippet | null = null;

    private readonly humanizeDuration: Humanizer;

    constructor(
        private store: Store<CanvasState>,
        private nifiCommon: NiFiCommon,
        private overlay: Overlay
    ) {
        this.humanizeDuration = humanizer();

        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((currentProcessGroupId) => {
                this.currentProcessGroupId = currentProcessGroupId;
                this.trimLengthCaches.clear();
            });

        this.store
            .select(selectParentProcessGroupId)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((parentProcessGroupId) => {
                this.parentProcessGroupId = parentProcessGroupId;
            });

        this.store
            .select(selectCanvasPermissions)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((canvasPermissions) => {
                this.canvasPermissions = canvasPermissions;
            });

        this.store
            .select(selectConnections)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((connections) => {
                this.connections = connections;
            });

        this.store
            .select(selectCurrentUser)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((user) => {
                this.currentUser = user;
            });

        this.store
            .select(selectCurrentParameterContext)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((currentParameterContext) => {
                this.currentParameterContext = currentParameterContext;
            });

        this.store
            .select(selectFlowConfiguration)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((flowConfiguration) => {
                this.flowConfiguration = flowConfiguration;
            });

        this.store
            .select(selectBreadcrumbs)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((breadcrumbs) => {
                this.breadcrumbs = breadcrumbs;
            });

        this.store
            .select(selectCopiedSnippet)
            .pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((copiedSnippet) => {
                this.copiedSnippet = copiedSnippet;
            });
    }

    public hasDownstream(selection: any): boolean {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }

        return (
            this.isFunnel(selection) ||
            this.isProcessor(selection) ||
            this.isProcessGroup(selection) ||
            this.isRemoteProcessGroup(selection) ||
            this.isInputPort(selection) ||
            (this.isOutputPort(selection) && this.parentProcessGroupId !== null)
        );
    }

    public hasUpstream(selection: any): boolean {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }

        return (
            this.isFunnel(selection) ||
            this.isProcessor(selection) ||
            this.isProcessGroup(selection) ||
            this.isRemoteProcessGroup(selection) ||
            this.isOutputPort(selection) ||
            (this.isInputPort(selection) && this.parentProcessGroupId !== null)
        );
    }

    /**
     * Removes the temporary if necessary.
     */
    public removeTempEdge(): void {
        d3.select('path.connector').remove();
    }

    /**
     * Determines whether the current user can access provenance.
     */
    public canAccessProvenance(): boolean {
        return this.currentUser.provenancePermissions.canRead;
    }

    /**
     * Determines whether the specified selection is empty.
     *
     * @param selection     The selection
     */
    public emptySelection(selection: any): boolean {
        return selection.empty();
    }

    /**
     * Returns whether the current group is not the root group.
     */
    public isNotRootGroup(): boolean {
        return this.parentProcessGroupId != null;
    }

    /**
     * Returns the current group id.
     */
    public getProcessGroupId(): string {
        return this.currentProcessGroupId;
    }

    /**
     * Returns the parent group id or null if current is root.
     */
    public getParentProcessGroupId(): string | null {
        return this.parentProcessGroupId;
    }

    /**
     * Returns the current parameter context id or null if there is no bound parameter context.
     */
    public getParameterContextId(): string | null {
        return this.currentParameterContext ? this.currentParameterContext.id : null;
    }

    /**
     * Returns whether the current group is not the root group and the current selection is empty.
     *
     * @param {selection} selection         The selection of currently selected components
     */
    public isNotRootGroupAndEmptySelection(selection: any) {
        return this.isNotRootGroup() && selection.empty();
    }

    /**
     * Determines if the specified selection is alignable (in a single action).
     *
     * @param {selection} selection     The selection
     * @returns {boolean}
     */
    public canAlign(selection: any) {
        let canAlign = true;

        // determine if the current selection is entirely connections
        const selectedConnections = selection.filter((d: any) => {
            return d.type == ComponentType.Connection;
        });

        // require multiple selections besides connections
        if (selection.size() - selectedConnections.size() < 2) {
            canAlign = false;
        }

        // require write permissions
        if (!this.canModify(selection)) {
            canAlign = false;
        }

        return canAlign;
    }

    /**
     * Determines whether the components in the specified selection are writable.
     *
     * @argument {selection} selection      The selection
     * @return {boolean}            Whether the selection is writable
     */
    public canModify(selection: any): boolean {
        const selectionSize = selection.size();
        const writableSize = selection
            .filter(function (d: any) {
                return d.permissions.canWrite;
            })
            .size();

        return selectionSize === writableSize;
    }

    /**
     * Determines whether the components in the specified selection are readable.
     *
     * @argument {selection} selection      The selection
     * @return {boolean}            Whether the selection is readable
     */
    public canRead(selection: any): boolean {
        const selectionSize = selection.size();
        const readableSize = selection
            .filter(function (d: any) {
                return d.permissions.canRead;
            })
            .size();

        return selectionSize === readableSize;
    }

    /**
     * Determines whether the specified Processor, Input Port, or Output Port currently supports modification.
     *
     * @param entity
     */
    public runnableSupportsModification(entity: any): boolean {
        return !(
            entity.status.aggregateSnapshot.runStatus === 'Running' ||
            entity.status.aggregateSnapshot.activeThreadCount > 0
        );
    }

    /**
     * Determines whether the specified Remote Process Group currently supports modification.
     *
     * @param entity
     */
    public remoteProcessGroupSupportsModification(entity: any): boolean {
        return !(
            entity.status.transmissionStatus === 'Transmitting' || entity.status.aggregateSnapshot.activeThreadCount > 0
        );
    }

    /**
     * Determines whether the specified selection is in a state to support modification.
     *
     * @argument {selection} selection      The selection
     */
    private supportsModification(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        // get the selection data
        const selectionData: any = selection.datum();

        let supportsModification = false;
        if (this.isProcessor(selection) || this.isInputPort(selection) || this.isOutputPort(selection)) {
            supportsModification = this.runnableSupportsModification(selectionData);
        } else if (this.isRemoteProcessGroup(selection)) {
            supportsModification = this.remoteProcessGroupSupportsModification(selectionData);
        } else if (this.isProcessGroup(selection)) {
            supportsModification = true;
        } else if (this.isFunnel(selection)) {
            supportsModification = true;
        } else if (this.isLabel(selection)) {
            supportsModification = true;
        } else if (this.isConnection(selection)) {
            let isSourceConfigurable = false;
            let isDestinationConfigurable = false;

            const sourceComponentId: string = this.getConnectionSourceComponentId(selectionData);
            const source: any = d3.select('#id-' + sourceComponentId);
            if (!source.empty()) {
                if (this.isRemoteProcessGroup(source) || this.isProcessGroup(source)) {
                    isSourceConfigurable = true;
                } else {
                    isSourceConfigurable = this.supportsModification(source);
                }
            }

            const destinationComponentId: string = this.getConnectionDestinationComponentId(selectionData);
            const destination: any = d3.select('#id-' + destinationComponentId);
            if (!destination.empty()) {
                if (this.isRemoteProcessGroup(destination) || this.isProcessGroup(destination)) {
                    isDestinationConfigurable = true;
                } else {
                    isDestinationConfigurable = this.supportsModification(destination);
                }
            }

            supportsModification = isSourceConfigurable && isDestinationConfigurable;
        }
        return supportsModification;
    }

    /**
     * Determines whether the components in the specified selection are deletable.
     *
     * @argument {selection} selection      The selection
     * @return {boolean}            Whether the selection is deletable
     */
    public areDeletable(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.empty()) {
            return false;
        }

        let isDeletable = true;
        selection.each((data, index, nodes) => {
            if (!this.isDeletable(d3.select(nodes[index]))) {
                isDeletable = false;
            }
        });

        return isDeletable;
    }

    /**
     * Determines whether the component in the specified selection is deletable.
     *
     * @argument {selection} selection      The selection
     * @return {boolean}            Whether the selection is deletable
     */
    public isDeletable(selection: any): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        // ensure the user has write permissions to the current process group
        if (!this.canvasPermissions.canWrite) {
            return false;
        }

        if (!this.canModify(selection)) {
            return false;
        }

        return this.supportsModification(selection);
    }

    /**
     * Determines whether the specified selection is configurable.
     *
     * @param selection
     */
    public isConfigurable(selection: any): boolean {
        if (selection.empty()) {
            return this.canvasPermissions.canRead && this.canvasPermissions.canWrite;
        }

        // disable configuration if there are multiple components selected
        if (selection.size() > 1) {
            return false;
        }

        if (!this.canRead(selection) || !this.canModify(selection)) {
            return false;
        }
        if (this.isFunnel(selection)) {
            return false;
        }

        return this.supportsModification(selection);
    }

    /**
     * Determines whether the specified selection has details.
     *
     * @param selection
     */
    public hasDetails(selection: any): boolean {
        if (selection.empty()) {
            return this.canvasPermissions.canRead && !this.canvasPermissions.canWrite;
        }

        // ensure the correct number of components are selected
        if (selection.size() > 1) {
            return false;
        }

        if (!this.canRead(selection)) {
            return false;
        }

        if (this.canModify(selection)) {
            if (
                this.isProcessor(selection) ||
                this.isInputPort(selection) ||
                this.isOutputPort(selection) ||
                this.isRemoteProcessGroup(selection) ||
                this.isProcessGroup(selection) ||
                this.isConnection(selection)
            ) {
                return !this.isConfigurable(selection);
            }
        } else {
            return (
                this.isProcessor(selection) ||
                this.isInputPort(selection) ||
                this.isOutputPort(selection) ||
                this.isRemoteProcessGroup(selection) ||
                this.isProcessGroup(selection) ||
                this.isConnection(selection)
            );
        }

        return false;
    }

    /**
     * Determines if the specified selection is a connection.
     *
     * @argument {selection} selection      The selection
     */
    public isConnection(selection: any): boolean {
        return selection.size() === 1 && selection.classed('connection');
    }

    /**
     * Determines if the specified selection is a remote process group.
     *
     * @argument {selection} selection      The selection
     */
    public isRemoteProcessGroup(selection: any): boolean {
        return selection.size() === 1 && selection.classed('remote-process-group');
    }

    /**
     * Determines if the specified selection is a processor.
     *
     * @argument {selection} selection      The selection
     */
    public isProcessor(selection: any): boolean {
        return selection.size() === 1 && selection.classed('processor');
    }

    /**
     * Determines if the specified selection is a label.
     *
     * @argument {selection} selection      The selection
     */
    public isLabel(selection: any): boolean {
        return selection.size() === 1 && selection.classed('label');
    }

    /**
     * Determines if the specified selection is an input port.
     *
     * @argument {selection} selection      The selection
     */
    public isInputPort(selection: any): boolean {
        return selection.size() === 1 && selection.classed('input-port');
    }

    /**
     * Determines if the specified selection is an output port.
     *
     * @argument {selection} selection      The selection
     */
    public isOutputPort(selection: any): boolean {
        return selection.size() === 1 && selection.classed('output-port');
    }

    /**
     * Determines if the specified selection is a process group.
     *
     * @argument {selection} selection      The selection
     */
    public isProcessGroup(selection: any): boolean {
        return selection.size() === 1 && selection.classed('process-group');
    }

    /**
     * Determines if the specified selection is a funnel.
     *
     * @argument {selection} selection      The selection
     */
    public isFunnel(selection: any): boolean {
        return selection.size() === 1 && selection.classed('funnel');
    }

    // determine if the source of this connection is part of the selection
    public isSourceSelected(connection: any, selection: any): boolean {
        return (
            selection
                .filter((d: any) => {
                    return this.getConnectionSourceComponentId(connection) === d.id;
                })
                .size() > 0
        );
    }

    /**
     * Determines whether the current selection is a stateful processor.
     *
     * @param {selection} selection
     */
    public isStatefulProcessor(selection: any): boolean {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }
        if (this.canRead(selection) === false || this.canModify(selection) === false) {
            return false;
        }

        if (this.isProcessor(selection)) {
            const processorData: any = selection.datum();
            return processorData.component.persistsState === true;
        } else {
            return false;
        }
    }

    /**
     * Determines whether the user can configure or open the policy management page.
     */
    public canManagePolicies(selection: any): boolean {
        // ensure 0 or 1 components selected
        if (selection.size() <= 1) {
            // if something is selected, ensure it's not a connection
            if (!selection.empty() && this.isConnection(selection)) {
                return false;
            }

            // ensure access to read tenants
            return this.canAccessTenants();
        }

        return false;
    }

    public supportsManagedAuthorizer(): boolean {
        if (this.flowConfiguration) {
            return this.flowConfiguration.supportsManagedAuthorizer;
        }
        return false;
    }

    /**
     * Determines whether the current user can access tenants.
     *
     * @returns {boolean}
     */
    public canAccessTenants(): boolean {
        return this.currentUser.tenantsPermissions.canRead === true;
    }

    /**
     * Determines whether the current user can access provenance for the specified component.
     *
     * @argument {selection} selection      The selection
     */
    public canAccessComponentProvenance(selection: any): boolean {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }

        return (
            !this.isLabel(selection) &&
            !this.isConnection(selection) &&
            !this.isProcessGroup(selection) &&
            !this.isRemoteProcessGroup(selection) &&
            this.canAccessProvenance()
        );
    }

    /**
     * Determines whether the current selection should provide ability to replay latest provenance event.
     *
     * @param {selection} selection
     */
    public canReplayComponentProvenance(selection: any): boolean {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }

        return this.isProcessor(selection) && this.canAccessProvenance();
    }

    /**
     * Determines if there is a Parameter Context bound.
     *
     * @param selection
     */
    public hasParameterContext(selection: d3.Selection<any, any, any, any>): boolean {
        let parameterContext;

        if (selection.empty()) {
            parameterContext = this.currentParameterContext;
        } else if (this.isProcessGroup(selection)) {
            const pg = selection.datum();
            parameterContext = pg.parameterContext;
        }

        if (parameterContext) {
            return parameterContext.permissions.canRead;
        }

        return false;
    }

    /**
     * Determines whether the current user can view the status history for the selected component.
     *
     * @param {selection} selection
     */
    public canViewStatusHistory(selection: any): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        return (
            this.isProcessor(selection) ||
            this.isConnection(selection) ||
            this.isRemoteProcessGroup(selection) ||
            this.isProcessGroup(selection)
        );
    }

    /**
     * Gets the currently selected components and connections.
     *
     * @returns {selection}     The currently selected components and connections
     */
    public getSelection(): any {
        return d3.selectAll('g.component.selected, g.connection.selected');
    }

    public getComponentConnections(id: string): any[] {
        return this.connections.filter((connection) => {
            return (
                this.getConnectionSourceComponentId(connection) === id ||
                this.getConnectionDestinationComponentId(connection) === id
            );
        });
    }

    /**
     * Returns the position for centering a connection based on the presence of bends.
     *
     * @param d connection data
     */
    public getPositionForCenteringConnection(d: any): Position {
        let x, y;
        if (d.bends.length > 0) {
            const i: number = Math.min(Math.max(0, d.labelIndex), d.bends.length - 1);
            x = d.bends[i].x;
            y = d.bends[i].y;
        } else {
            x = (d.start.x + d.end.x) / 2;
            y = (d.start.y + d.end.y) / 2;
        }

        return {
            x,
            y
        };
    }

    /**
     * Returns the component id of the source of this processor. If the connection is attached
     * to a port in a [sub|remote] group, the component id will be that of the group. Otherwise
     * it is the component itself.
     *
     * @param {object} connection   The connection in question
     */
    public getConnectionSourceComponentId(connection: any): string {
        let sourceId: string = connection.sourceId;
        if (connection.sourceGroupId !== this.currentProcessGroupId) {
            sourceId = connection.sourceGroupId;
        }
        return sourceId;
    }

    /**
     * Returns the component id of the source of this processor. If the connection is attached
     * to a port in a [sub|remote] group, the component id will be that of the group. Otherwise
     * it is the component itself.
     *
     * @param {object} connection   The connection in question
     */
    public getConnectionDestinationComponentId(connection: any): string {
        let destinationId: string = connection.destinationId;
        if (connection.destinationGroupId !== this.currentProcessGroupId) {
            destinationId = connection.destinationGroupId;
        }
        return destinationId;
    }

    /**
     * Determines the connectable type for the specified source.
     *
     * @argument {type} ComponentType      The component type
     */
    getConnectableTypeForSource(type: ComponentType): string {
        switch (type) {
            case ComponentType.Processor:
                return 'PROCESSOR';
            case ComponentType.RemoteProcessGroup:
                return 'REMOTE_OUTPUT_PORT';
            case ComponentType.ProcessGroup:
                return 'OUTPUT_PORT';
            case ComponentType.InputPort:
                return 'INPUT_PORT';
            case ComponentType.Funnel:
                return 'FUNNEL';
            default:
                return '';
        }
    }

    /**
     * Determines the component type for the specified source.
     *
     * @argument {connectableType} string      The connectable type
     */
    getComponentTypeForSource(connectableType: string): ComponentType | null {
        switch (connectableType) {
            case 'PROCESSOR':
                return ComponentType.Processor;
            case 'REMOTE_OUTPUT_PORT':
                return ComponentType.RemoteProcessGroup;
            case 'OUTPUT_PORT':
                return ComponentType.ProcessGroup;
            case 'INPUT_PORT':
                return ComponentType.InputPort;
            case 'FUNNEL':
                return ComponentType.Funnel;
            default:
                return null;
        }
    }

    /**
     * Determines the connectable type for the specified destination.
     *
     * @argument {type} ComponentType      The component type
     */
    getConnectableTypeForDestination(type: ComponentType): string {
        switch (type) {
            case ComponentType.Processor:
                return 'PROCESSOR';
            case ComponentType.RemoteProcessGroup:
                return 'REMOTE_INPUT_PORT';
            case ComponentType.ProcessGroup:
                return 'INPUT_PORT';
            case ComponentType.OutputPort:
                return 'OUTPUT_PORT';
            case ComponentType.Funnel:
                return 'FUNNEL';
            default:
                return '';
        }
    }

    /**
     * Determines the component type for the specified destination.
     *
     * @argument {type} ComponentType      The component type
     */
    getComponentTypeForDestination(connectableType: string): ComponentType | null {
        switch (connectableType) {
            case 'PROCESSOR':
                return ComponentType.Processor;
            case 'REMOTE_INPUT_PORT':
                return ComponentType.RemoteProcessGroup;
            case 'INPUT_PORT':
                return ComponentType.ProcessGroup;
            case 'OUTPUT_PORT':
                return ComponentType.OutputPort;
            case 'FUNNEL':
                return ComponentType.Funnel;
            default:
                return null;
        }
    }

    /**
     * Determines if the specified selection is disconnected from other nodes.
     *
     * @argument {selection} selection          The selection
     */
    public isDisconnected(selection: any): boolean {
        // if nothing is selected return
        if (selection.empty()) {
            return false;
        }

        const connections: Map<string, any> = new Map<string, any>();
        const components: Map<string, any> = new Map<string, any>();

        let isDisconnected = true;

        // include connections
        selection
            .filter((d: any) => {
                return d.type === 'Connection';
            })
            .each((d: any) => {
                connections.set(d.id, d);
            });

        // include components and ensure their connections are included
        selection
            .filter((d: any) => {
                return d.type !== 'Connection';
            })
            .each((d: any) => {
                components.set(d.id, d.component);

                // check all connections of this component
                this.getComponentConnections(d.id).forEach((connection) => {
                    if (!connections.has(connection.id)) {
                        isDisconnected = false;
                    }
                });
            });

        if (isDisconnected) {
            // go through each connection to ensure its source and destination are included
            connections.forEach((connection) => {
                if (isDisconnected) {
                    // determine whether this connection and its components are included within the selection
                    isDisconnected =
                        components.has(this.getConnectionSourceComponentId(connection)) &&
                        components.has(this.getConnectionDestinationComponentId(connection));
                }
            });
        }

        return isDisconnected;
    }

    /**
     * Gets the origin of the bounding box for the specified selection. Returns
     * (0, 0) if the selection is empty or only contains connections.
     *
     * @argument {selection} selection      The selection
     */
    public getOrigin(selection: d3.Selection<any, any, any, any>): Position {
        let x: number | undefined;
        let y: number | undefined;

        selection.each((d, i, nodes) => {
            const selected: any = d3.select(nodes[i]);
            if (!this.isConnection(selected)) {
                if (x == null || d.position.x < x) {
                    x = d.position.x;
                }
                if (y == null || d.position.y < y) {
                    y = d.position.y;
                }
            }
        });

        if (x == null || y == null) {
            return { x: 0, y: 0 };
        }

        return { x, y };
    }

    public isCopyable(selection: d3.Selection<any, any, any, any>): boolean {
        // if nothing is selected return
        if (selection.empty()) {
            return false;
        }

        if (!this.canRead(selection)) {
            return false;
        }

        // determine how many copyable components are selected
        const copyable = selection.filter((d, i, nodes) => {
            const selected = d3.select(nodes[i]);
            if (this.isConnection(selected)) {
                const sourceIncluded = !selection
                    .filter((source) => {
                        const sourceComponentId = this.getConnectionSourceComponentId(d);
                        return sourceComponentId === source.id;
                    })
                    .empty();
                const destinationIncluded = !selection
                    .filter((destination) => {
                        const destinationComponentId = this.getConnectionDestinationComponentId(d);
                        return destinationComponentId === destination.id;
                    })
                    .empty();
                return sourceIncluded && destinationIncluded;
            } else {
                return (
                    this.isProcessor(selected) ||
                    this.isFunnel(selected) ||
                    this.isLabel(selected) ||
                    this.isProcessGroup(selected) ||
                    this.isRemoteProcessGroup(selected) ||
                    this.isInputPort(selected) ||
                    this.isOutputPort(selected)
                );
            }
        });

        // ensure everything selected is copyable
        return selection.size() === copyable.size();
    }

    public isPastable(): boolean {
        return this.canvasPermissions.canWrite && this.copiedSnippet != null;
    }

    /**
     * Gets the name for this connection.
     *
     * @param {object} connection
     */
    public formatConnectionName(connection: any): string {
        if (!this.nifiCommon.isBlank(connection.name)) {
            return connection.name;
        } else if (connection.selectedRelationships) {
            return connection.selectedRelationships.join(', ');
        }
        return '';
    }

    /**
     * Formats a number (in milliseconds) to a human-readable textual description.
     *
     * @param duration number of milliseconds representing the duration
     * @return {string|*} a human-readable string
     */
    public formatPredictedDuration(duration: number): string {
        if (duration === 0) {
            return 'now';
        }

        return this.humanizeDuration(duration, {
            round: true
        });
    }

    /**
     * Calculates the point on the specified bounding box that is closest to the
     * specified point.
     *
     * @param {object} p            The point
     * @param {object} bBox         The bounding box
     */
    public getPerimeterPoint(p: Position, bBox: any): Position {
        // calculate theta
        const theta: number = Math.atan2(bBox.height, bBox.width);

        // get the rectangle radius
        const xRadius: number = bBox.width / 2;
        const yRadius: number = bBox.height / 2;

        // get the center point
        const cx: number = bBox.x + xRadius;
        const cy: number = bBox.y + yRadius;

        // calculate alpha
        const dx: number = p.x - cx;
        const dy: number = p.y - cy;
        let alpha: number = Math.atan2(dy, dx);

        // normalize aphla into 0 <= alpha < 2 PI
        alpha = alpha % CanvasUtils.TWO_PI;
        if (alpha < 0) {
            alpha += CanvasUtils.TWO_PI;
        }

        // calculate beta
        const beta: number = Math.PI / 2 - alpha;

        // detect the appropriate quadrant and return the point on the perimeter
        if ((alpha >= 0 && alpha < theta) || (alpha >= CanvasUtils.TWO_PI - theta && alpha < CanvasUtils.TWO_PI)) {
            // right quadrant
            return {
                x: bBox.x + bBox.width,
                y: cy + Math.tan(alpha) * xRadius
            };
        } else if (alpha >= theta && alpha < Math.PI - theta) {
            // bottom quadrant
            return {
                x: cx + Math.tan(beta) * yRadius,
                y: bBox.y + bBox.height
            };
        } else if (alpha >= Math.PI - theta && alpha < Math.PI + theta) {
            // left quadrant
            return {
                x: bBox.x,
                y: cy - Math.tan(alpha) * xRadius
            };
        } else {
            // top quadrant
            return {
                x: cx - Math.tan(beta) * yRadius,
                y: bBox.y
            };
        }
    }

    /**
     * Determines if the component in the specified selection is a valid connection source.
     *
     * @param {selection} selection         The selection
     * @return {boolean} Whether the selection is a valid connection source
     */
    public isValidConnectionSource(selection: any): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        // always allow connections from process groups
        if (this.isProcessGroup(selection)) {
            return true;
        }

        // require read and write for a connection source since we'll need to read the source to obtain valid relationships, etc
        if (!this.canRead(selection) || !this.canModify(selection)) {
            return false;
        }

        return (
            this.isProcessor(selection) ||
            this.isRemoteProcessGroup(selection) ||
            this.isInputPort(selection) ||
            this.isFunnel(selection)
        );
    }

    /**
     * Determines if the component in the specified selection is a valid connection destination.
     *
     * @param {selection} selection         The selection
     * @return {boolean} Whether the selection is a valid connection destination
     */
    public isValidConnectionDestination(selection: any): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        if (this.isProcessGroup(selection)) {
            return true;
        }

        // require write for a connection destination
        if (!this.canModify(selection)) {
            return false;
        }

        if (this.isRemoteProcessGroup(selection) || this.isOutputPort(selection) || this.isFunnel(selection)) {
            return true;
        }

        // if processor, ensure it supports input
        if (this.isProcessor(selection)) {
            const destinationData: any = selection.datum();
            return destinationData.inputRequirement !== 'INPUT_FORBIDDEN';
        }

        return false;
    }

    private binarySearch(length: number, comparator: (data: number) => number): number {
        let low = 0;
        let high = length - 1;
        let mid = 0;

        let result = 0;
        while (low <= high) {
            mid = ~~((low + high) / 2);
            result = comparator(mid);
            if (result < 0) {
                high = mid - 1;
            } else if (result > 0) {
                low = mid + 1;
            } else {
                break;
            }
        }

        return mid;
    }

    /**
     * Creates tooltip for the specified selection.
     *
     * @param type
     * @param selection
     * @param tooltipData
     */
    public canvasTooltip<C>(type: Type<C>, selection: any, tooltipData: any): void {
        let closeTimer = -1;
        const overlay = this.overlay;
        let overlayRef: OverlayRef | null = null;

        selection
            .on('mouseenter', function (this: any) {
                if (overlayRef?.hasAttached()) {
                    return;
                }

                if (!overlayRef) {
                    const positionStrategy = overlay
                        .position()
                        .flexibleConnectedTo(d3.select(this).node())
                        .withPositions([
                            {
                                originX: 'end',
                                originY: 'bottom',
                                overlayX: 'start',
                                overlayY: 'top',
                                offsetX: 8,
                                offsetY: 8
                            }
                        ])
                        .withPush(true);

                    overlayRef = overlay.create({ positionStrategy });
                }

                const tooltipReference = overlayRef.attach(new ComponentPortal(type));
                tooltipReference.setInput('data', tooltipData);

                // register mouse events
                tooltipReference.location.nativeElement.addEventListener('mouseenter', () => {
                    if (closeTimer > 0) {
                        window.clearTimeout(closeTimer);
                        closeTimer = -1;
                    }
                });
                tooltipReference.location.nativeElement.addEventListener('mouseleave', () => {
                    overlayRef?.detach();
                    overlayRef?.dispose();
                    overlayRef = null;
                });
            })
            .on('mouseleave', function () {
                closeTimer = window.setTimeout(() => {
                    overlayRef?.detach();
                    overlayRef?.dispose();
                    overlayRef = null;
                }, 400);
            });
    }

    /**
     * Sets the bulletin visibility and applies a tooltip if necessary.
     *
     * @param selection
     * @param bulletins
     */
    public bulletins(selection: any, bulletins: string[]): void {
        if (this.nifiCommon.isEmpty(bulletins)) {
            // reset the bulletin icon/background
            selection.select('text.bulletin-icon').style('visibility', 'hidden');
            selection.select('rect.bulletin-background').style('visibility', 'hidden');
        } else {
            // show the bulletin icon/background
            const bulletinIcon: any = selection.select('text.bulletin-icon').style('visibility', 'visible');
            selection.select('rect.bulletin-background').style('visibility', 'visible');

            // add the tooltip
            this.canvasTooltip(BulletinsTip, bulletinIcon, {
                bulletins: bulletins
            });
        }
    }

    /**
     * Applies single line ellipsis to the component in the specified selection if necessary.
     *
     * @param {selection} selection
     * @param {string} text
     * @param {string} cacheName
     */
    public ellipsis(selection: any, text: string, cacheName: string) {
        text = text.trim();
        let width = parseInt(selection.attr('width'), 10);
        const node = selection.node();

        // set the element text
        selection.text(text);

        // Never apply ellipses to text less than 5 characters and don't keep it in the cache
        // because it could take up a lot of space unnecessarily.
        const textLength: number = text.length;
        if (textLength < 5) {
            return;
        }

        // Check our cache of text lengths to see if we already know how much to trim it to
        let trimLengths = this.trimLengthCaches.get(cacheName);
        if (!trimLengths) {
            trimLengths = new Map();
            this.trimLengthCaches.set(cacheName, trimLengths);
        }

        const cacheForText = trimLengths.get(text);
        let trimLength = cacheForText === undefined ? undefined : cacheForText.get(width);
        if (!trimLength) {
            // We haven't cached the length for this text yet. Determine whether we need
            // to trim & add ellipses or not
            if (node.getSubStringLength(0, text.length - 1) > width) {
                // make some room for the ellipsis
                width -= 5;

                // determine the appropriate index
                trimLength = this.binarySearch(text.length, function (x: number) {
                    const length = node.getSubStringLength(0, x);
                    if (length > width) {
                        // length is too long, try the lower half
                        return -1;
                    } else if (length < width) {
                        // length is too short, try the upper half
                        return 1;
                    }
                    return 0;
                });
            } else {
                // trimLength of -1 indicates we do not need ellipses
                trimLength = -1;
            }

            // Store the trim length in our cache
            let trimLengthsForText = trimLengths.get(text);
            if (trimLengthsForText === undefined) {
                trimLengthsForText = new Map();
                trimLengths.set(text, trimLengthsForText);
            }
            trimLengthsForText.set(width, trimLength);
        }

        if (trimLength === -1) {
            return;
        }

        // trim at the appropriate length and add ellipsis
        selection.text(text.substring(0, trimLength) + String.fromCharCode(8230));
    }

    /**
     * Applies multiline ellipsis to the component in the specified seleciton. Text will
     * wrap for the specified number of lines. The last line will be ellipsis if necessary.
     *
     * @param {selection} selection
     * @param {number} lineCount
     * @param {string} text
     * @param {string} cacheName
     */
    public multilineEllipsis(selection: any, lineCount: number, text: string, cacheName: string) {
        let i = 1;
        const words: string[] = text.split(/\s+/).reverse();

        // get the appropriate position
        const x = parseInt(selection.attr('x'), 10);
        const y = parseInt(selection.attr('y'), 10);
        const width = parseInt(selection.attr('width'), 10);

        let line: string[] = [];
        let tspan = selection.append('tspan').attr('x', x).attr('y', y).attr('width', width);

        // go through each word
        let word = words.pop();
        while (word) {
            // add the current word
            line.push(word);

            // update the label text
            tspan.text(line.join(' '));

            // if this word caused us to go too far
            if (tspan.node().getComputedTextLength() > width) {
                // remove the current word
                line.pop();

                // update the label text
                tspan.text(line.join(' '));

                // create the tspan for the next line
                tspan = selection.append('tspan').attr('x', x).attr('dy', '1.2em').attr('width', width);

                // if we've reached the last line, use single line ellipsis
                if (++i >= lineCount) {
                    // get the remainder using the current word and
                    // reversing whats left
                    const remainder = [word].concat(words.reverse());

                    // apply ellipsis to the last line
                    this.ellipsis(tspan, remainder.join(' '), cacheName);

                    // we've reached the line count
                    break;
                } else {
                    tspan.text(word);

                    // prep the line for the next iteration
                    line = [word];
                }
            }

            // get the next word
            word = words.pop();
        }
    }

    /**
     * Updates the active thread count on the specified selection.
     *
     * @param {selection} selection         The selection
     * @param {object} d                    The data
     * @return
     */
    public activeThreadCount(selection: any, d: any) {
        const activeThreads = d.status.aggregateSnapshot.activeThreadCount;
        const terminatedThreads = d.status.aggregateSnapshot.terminatedThreadCount;

        // if there is active threads show the count, otherwise hide
        if (activeThreads > 0 || terminatedThreads > 0) {
            const generateThreadsTip = function () {
                let tip = activeThreads + ' active threads';
                if (terminatedThreads > 0) {
                    tip += ' (' + terminatedThreads + ' terminated)';
                }

                return tip;
            };

            // update the active thread count
            const activeThreadCount = selection
                .select('text.active-thread-count')
                .text(function () {
                    if (terminatedThreads > 0) {
                        return activeThreads + ' (' + terminatedThreads + ')';
                    } else {
                        return activeThreads;
                    }
                })
                .style('display', 'block')
                .each(function (this: any) {
                    const activeThreadCountText = d3.select(this);

                    const bBox = this.getBBox();
                    activeThreadCountText.attr('x', function () {
                        return d.dimensions.width - bBox.width - 15;
                    });

                    // reset the active thread count tooltip
                    activeThreadCountText.selectAll('title').remove();
                });

            // append the tooltip
            activeThreadCount.append('title').text(generateThreadsTip);

            // update the background width
            selection
                .select('text.active-thread-count-icon')
                .attr('x', function () {
                    const bBox = activeThreadCount.node().getBBox();
                    return d.dimensions.width - bBox.width - 20;
                })
                .attr('class', function () {
                    if (terminatedThreads > 0) {
                        return `active-thread-count-icon warn-color-darker`;
                    } else {
                        return `active-thread-count-icon primary-color`;
                    }
                })
                .style('display', 'block')
                .each(function (this: any) {
                    const activeThreadCountIcon = d3.select(this);

                    // reset the active thread count tooltip
                    activeThreadCountIcon.selectAll('title').remove();
                })
                .append('title')
                .text(generateThreadsTip);
        } else {
            selection
                .selectAll('text.active-thread-count, text.active-thread-count-icon')
                .style('display', 'none')
                .each(function (this: any) {
                    d3.select(this).selectAll('title').remove();
                });
        }
    }

    /**
     * Determines the contrast color of a given hex color.
     *
     * @param {string} hex  The hex color to test.
     * @returns {string} The contrasting color string.
     */
    public determineContrastColor(hex: string): string {
        if (parseInt(hex, 16) > 0xffffff / 1.5) {
            return '#000000';
        }
        return '#ffffff';
    }

    /**
     * Filters the specified selection for any components that supports enable.
     *
     * @argument {selection} selection      The selection
     */
    public filterEnable(selection: d3.Selection<any, any, any, any>): d3.Selection<any, any, any, any> {
        return selection.filter((d, i, nodes) => {
            const selected = d3.select(nodes[i]);

            // enable always allowed for PGs since they will invoke the /flow endpoint for enabling all applicable components (based on permissions)
            if (this.isProcessGroup(selected)) {
                return true;
            }

            // not a PG, verify permissions to modify
            if (!this.canOperate(selected)) {
                return false;
            }

            // ensure it's a processor, input port, or output port and supports modification and is disabled (can enable)
            return (
                (this.isProcessor(selected) || this.isInputPort(selected) || this.isOutputPort(selected)) &&
                this.supportsModification(selected) &&
                d.status.aggregateSnapshot.runStatus === 'Disabled'
            );
        });
    }

    /**
     * Determines if the specified selection contains any components that supports enable.
     *
     * @argument {selection} selection      The selection
     */
    public canEnable(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.empty()) {
            return true;
        }

        return this.filterEnable(selection).size() === selection.size();
    }

    /**
     * Filters the specified selection for any components that supports disable.
     *
     * @argument {selection} selection      The selection
     */
    public filterDisable(selection: d3.Selection<any, any, any, any>): d3.Selection<any, any, any, any> {
        return selection.filter((d, i, nodes) => {
            const selected = d3.select(nodes[i]);

            // disable always allowed for PGs since they will invoke the /flow endpoint for disabling all applicable components (based on permissions)
            if (this.isProcessGroup(selected)) {
                return true;
            }

            // not a PG, verify permissions to modify
            if (!this.canOperate(selected)) {
                return false;
            }

            // ensure it's a processor, input port, or output port and supports modification and is stopped (can disable)
            return (
                (this.isProcessor(selected) || this.isInputPort(selected) || this.isOutputPort(selected)) &&
                this.supportsModification(selected) &&
                (d.status.aggregateSnapshot.runStatus === 'Stopped' ||
                    d.status.aggregateSnapshot.runStatus === 'Invalid')
            );
        });
    }

    /**
     * Determines if the specified selection contains any components that supports disable.
     *
     * @argument {selection} selection      The selection
     */
    public canDisable(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.empty()) {
            return true;
        }

        return this.filterDisable(selection).size() === selection.size();
    }

    /**
     * Determines if the components in the specified selection are runnable.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                    Whether the selection is runnable
     */
    public areRunnable(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.empty()) {
            return true;
        }

        let runnable = true;
        selection.each((data, index, nodes) => {
            if (!this.isRunnable(d3.select(nodes[index]))) {
                runnable = false;
            }
        });
        return runnable;
    }

    /**
     * Determines if any of the components in the specified selection are runnable or can start transmitting.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                    Whether the selection is runnable
     */
    public areAnyRunnable(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.empty()) {
            return true;
        }

        let runnable = false;
        selection.each((data, index, nodes) => {
            const d = d3.select(nodes[index]);
            if (this.isRunnable(d) || this.canStartTransmitting(d)) {
                runnable = true;
            }
        });
        return runnable;
    }

    /**
     * Determines if the component in the specified selection is runnable.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                    Whether the selection is runnable
     */
    public isRunnable(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.size() !== 1) {
            return false;
        }
        if (this.isProcessGroup(selection)) {
            return true;
        }
        if (!this.canOperate(selection)) {
            return false;
        }

        let runnable = false;
        const selectionData = selection.datum();
        if (this.isProcessor(selection) || this.isInputPort(selection) || this.isOutputPort(selection)) {
            runnable =
                this.supportsModification(selection) && selectionData.status.aggregateSnapshot.runStatus === 'Stopped';
        }
        return runnable;
    }

    /**
     * Determines if the components in the specified selection are stoppable.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                    Whether the selection is stoppable
     */
    public areStoppable(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.empty()) {
            return true;
        }

        let stoppable = true;
        selection.each((data, index, nodes) => {
            if (!this.isStoppable(d3.select(nodes[index]))) {
                stoppable = false;
            }
        });

        return stoppable;
    }

    /**
     * Determines if any of the components in the specified selection are stoppable.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                    Whether the selection is stoppable
     */
    public areAnyStoppable(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.empty()) {
            return true;
        }

        let stoppable = false;
        selection.each((data, index, nodes) => {
            const d = d3.select(nodes[index]);
            if (this.isStoppable(d) || this.canStopTransmitting(d)) {
                stoppable = true;
            }
        });

        return stoppable;
    }

    /**
     * Determines if the component in the specified selection is runnable.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                    Whether the selection is runnable
     */
    public isStoppable(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.size() !== 1) {
            return false;
        }
        if (this.isProcessGroup(selection)) {
            return true;
        }
        if (!this.canOperate(selection)) {
            return false;
        }

        let stoppable = false;
        const selectionData = selection.datum();
        if (this.isProcessor(selection) || this.isInputPort(selection) || this.isOutputPort(selection)) {
            stoppable = selectionData.status.aggregateSnapshot.runStatus === 'Running';
        }
        return stoppable;
    }

    public getStartable(selection: d3.Selection<any, any, any, any>) {
        return selection.filter((d, index, nodes) => {
            const context = nodes[index];
            return this.isRunnable(d3.select(context)) || this.canStartTransmitting(d3.select(context));
        });
    }

    public getStoppable(selection: d3.Selection<any, any, any, any>) {
        return selection.filter((d, index, nodes) => {
            const context = nodes[index];
            return this.isStoppable(d3.select(context)) || this.canStopTransmitting(d3.select(context));
        });
    }

    public canAllStartTransmitting(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.empty()) {
            return false;
        }

        let canStartTransmitting = true;
        selection.each((data, index, nodes) => {
            if (!this.canStartTransmitting(d3.select(nodes[index]))) {
                canStartTransmitting = false;
            }
        });
        return canStartTransmitting;
    }

    /**
     * Determines if the components in the specified selection support starting transmission.
     *
     * @param {d3.Selection} selection
     */
    public canStartTransmitting(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        if ((!this.canModify(selection) || !this.canRead(selection)) && !this.canOperate(selection)) {
            return false;
        }

        return this.isRemoteProcessGroup(selection);
    }

    /**
     * Determines if the components in the specified selection support stopping transmission.
     *
     * @param {d3.Selection} selection
     */
    public canStopTransmitting(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        if ((!this.canModify(selection) || this.canRead(selection)) && !this.canOperate(selection)) {
            return false;
        }

        return this.isRemoteProcessGroup(selection);
    }

    /**
     * Determines if the specified selection can all stop transmitting.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                    Whether the selection can stop transmitting
     */
    public canAllStopTransmitting(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.empty()) {
            return false;
        }

        let canStopTransmitting = true;
        selection.each((data, index, nodes) => {
            if (!this.canStopTransmitting(d3.select(nodes[index]))) {
                canStopTransmitting = false;
            }
        });
        return canStopTransmitting;
    }

    /**
     * Determines whether the components in the specified selection can be operated.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}            Whether the selection can be operated
     */
    public canOperate(selection: d3.Selection<any, any, any, any>): boolean {
        const selectionSize = selection.size();
        const writableSize = selection
            .filter((d) => {
                return d.permissions.canWrite || d.operatePermissions?.canWrite;
            })
            .size();

        return selectionSize === writableSize;
    }

    /**
     * Determines whether the selection represents a Processor that is no longer running but
     * still has active threads.
     *
     * @param selection
     */
    public canTerminate(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        if (!this.canOperate(selection)) {
            return false;
        }

        if (this.isProcessor(selection)) {
            const selectionData = selection.datum();
            const aggregateSnapshot = selectionData.status.aggregateSnapshot;
            return aggregateSnapshot.runStatus !== 'Running' && aggregateSnapshot.activeThreadCount > 0;
        }

        return false;
    }

    /**
     * Determines whether the current selection supports starting flow versioning.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                       Whether the selection supports starting flow versioning
     */
    public supportsStartFlowVersioning(selection: d3.Selection<any, any, any, any>): boolean {
        if (!this.supportsFlowVersioning(selection)) {
            return false;
        }

        if (selection.empty()) {
            // check bread crumbs for version control information in the current group
            if (this.breadcrumbs) {
                if (this.breadcrumbs.permissions.canRead) {
                    return !this.breadcrumbs.breadcrumb.versionControlInformation;
                }
                return false;
            }
        }

        // check the selection for version control information
        const pgData = selection.datum();
        return !pgData.component.versionControlInformation;
    }

    /**
     * Determines whether the current selection supports flow versioning.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                       Whether the selection supports flow versioning
     */
    public supportsFlowVersioning(selection: d3.Selection<any, any, any, any>): boolean {
        if (!this.canVersionFlows()) {
            return false;
        }

        if (selection.empty()) {
            // prevent versioning of the root group
            if (!this.getParentProcessGroupId()) {
                return false;
            }

            // if not root group, ensure adequate permissions
            return this.canvasPermissions.canRead && this.canvasPermissions.canWrite;
        }

        if (this.isProcessGroup(selection)) {
            return this.canRead(selection) && this.canModify(selection);
        }

        return false;
    }

    /**
     * Returns whether the process group support supports commit.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                       Whether the selection supports commit.
     */
    public supportsCommitFlowVersion(selection: d3.Selection<any, any, any, any>): boolean {
        const versionControlInformation = this.getFlowVersionControlInformation(selection);

        // check the selection for version control information
        return versionControlInformation !== null && versionControlInformation.state === 'LOCALLY_MODIFIED';
    }

    /**
     * Returns whether the process group support supports force commit.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                       Whether the selection supports force commit.
     */
    public supportsForceCommitFlowVersion(selection: d3.Selection<any, any, any, any>): boolean {
        const versionControlInformation = this.getFlowVersionControlInformation(selection);

        // check the selection for version control information
        return versionControlInformation !== null && versionControlInformation.state === 'LOCALLY_MODIFIED_AND_STALE';
    }

    /**
     * Returns whether the process group supports revert local changes.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                       Whether the selection has local changes.
     */
    public hasLocalChanges(selection: d3.Selection<any, any, any, any>): boolean {
        const versionControlInformation = this.getFlowVersionControlInformation(selection);

        // check the selection for version control information
        return (
            versionControlInformation !== null &&
            (versionControlInformation.state === 'LOCALLY_MODIFIED' ||
                versionControlInformation.state === 'LOCALLY_MODIFIED_AND_STALE')
        );
    }

    /**
     * Returns whether the process group supports changing the flow version.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                       Whether the selection supports change flow version.
     */
    public supportsChangeFlowVersion(selection: d3.Selection<any, any, any, any>): boolean {
        const versionControlInformation = this.getFlowVersionControlInformation(selection);

        return (
            versionControlInformation !== null &&
            versionControlInformation.state !== 'LOCALLY_MODIFIED' &&
            versionControlInformation.state !== 'LOCALLY_MODIFIED_AND_STALE' &&
            versionControlInformation.state !== 'SYNC_FAILURE'
        );
    }

    /**
     * Determines whether the current selection supports stopping flow versioning.
     *
     * @argument {d3.Selection} selection      The selection
     * @return {boolean}                       Whether the selection supports stopping flow versioning.
     */
    public supportsStopFlowVersioning(selection: d3.Selection<any, any, any, any>): boolean {
        const versionControlInformation = this.getFlowVersionControlInformation(selection);

        return versionControlInformation !== null;
    }

    /**
     * Determines whether the current user can version flows.
     */
    public canVersionFlows(): boolean {
        return this.currentUser.canVersionFlows;
    }

    /**
     * Convenience function to perform all flow versioning pre-checks and retrieve
     * valid version information.
     *
     * @argument {d3.Selection} selection      The selection
     */
    public getFlowVersionControlInformation(
        selection: d3.Selection<any, any, any, any>
    ): VersionControlInformation | null {
        if (!this.supportsFlowVersioning(selection)) {
            return null;
        }

        if (selection.empty()) {
            // check bread crumbs for version control information in the current group
            if (this.breadcrumbs) {
                if (this.breadcrumbs.permissions.canRead) {
                    return this.breadcrumbs.breadcrumb.versionControlInformation || null;
                }
            }
            return null;
        } else {
            // check the selection for version control information
            const pgData = selection.datum();
            return pgData.component.versionControlInformation || null;
        }
    }

    /**
     * Returns whether the process group supports downloading the current flow.
     *
     * @argument {d3.Selection} selection      The selection
     * @returns {boolean}
     */
    public supportsDownloadFlow(selection: d3.Selection<any, any, any, any>): boolean {
        // download is allowed when either nothing is selected or a single readable process group is selected
        if (selection.empty()) {
            return this.canvasPermissions.canRead;
        } else if (selection.size() === 1) {
            return this.isProcessGroup(selection) && this.canRead(selection);
        }

        return false;
    }

    /**
     * Determines whether the current selection is a processor with more than one version.
     *
     * @argument {d3.Selection} selection      The selection
     * @returns {boolean}
     */
    public canChangeProcessorVersion(selection: d3.Selection<any, any, any, any>): boolean {
        if (selection.size() !== 1) {
            return false;
        }

        if (!this.canRead(selection) || !this.canModify(selection)) {
            return false;
        }

        if (this.isProcessor(selection)) {
            const data = selection.datum();
            const supportsModification = this.runnableSupportsModification(data);
            return supportsModification && data.component.multipleVersionsAvailable;
        }
        return false;
    }

    public canMoveToFront(selection: d3.Selection<any, any, any, any>): boolean {
        // ensure the correct number of components are selected
        if (selection.size() !== 1) {
            return false;
        }
        if (this.canModify(selection) === false) {
            return false;
        }

        return this.isConnection(selection) || this.isLabel(selection);
    }
}
