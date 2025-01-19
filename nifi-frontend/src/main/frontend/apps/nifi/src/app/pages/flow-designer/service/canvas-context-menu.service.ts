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

import { Injectable } from '@angular/core';
import { CanvasUtils } from './canvas-utils.service';
import { Store } from '@ngrx/store';
import { CanvasState } from '../state';
import * as FlowActions from '../state/flow/flow.actions';
import {
    centerSelectedComponents,
    downloadFlow,
    enterProcessGroup,
    goToRemoteProcessGroup,
    moveComponents,
    moveToFront,
    navigateToAdvancedProcessorUi,
    navigateToComponent,
    navigateToControllerServicesForProcessGroup,
    navigateToEditComponent,
    navigateToEditCurrentProcessGroup,
    navigateToManageRemotePorts,
    navigateToParameterContext,
    navigateToProvenanceForComponent,
    navigateToQueueListing,
    navigateToViewStatusHistoryForComponent,
    openChangeProcessorVersionDialog,
    openChangeVersionDialogRequest,
    openCommitLocalChangesDialogRequest,
    openForceCommitLocalChangesDialogRequest,
    openRevertLocalChangesDialogRequest,
    openSaveVersionDialogRequest,
    openShowLocalChangesDialogRequest,
    replayLastProvenanceEvent,
    requestRefreshRemoteProcessGroup,
    runOnce,
    stopVersionControlRequest,
    terminateThreads,
    updatePositions
} from '../state/flow/flow.actions';
import { ComponentType } from '@nifi/shared';
import {
    ConfirmStopVersionControlRequest,
    MoveComponentRequest,
    OpenChangeVersionDialogRequest,
    OpenLocalChangesDialogRequest,
    UpdateComponentRequest
} from '../state/flow';
import {
    ContextMenuDefinition,
    ContextMenuDefinitionProvider,
    ContextMenuItemDefinition
} from '../../../ui/common/context-menu/context-menu.component';
import { promptEmptyQueueRequest, promptEmptyQueuesRequest } from '../state/queue/queue.actions';
import { getComponentStateAndOpenDialog } from '../../../state/component-state/component-state.actions';
import { navigateToComponentDocumentation } from '../../../state/documentation/documentation.actions';
import * as d3 from 'd3';
import { Client } from '../../../service/client.service';
import { CanvasView } from './canvas-view.service';
import { CanvasActionsService } from './canvas-actions.service';
import { DraggableBehavior } from './behavior/draggable-behavior.service';
import { BackNavigation } from '../../../state/navigation';

@Injectable({ providedIn: 'root' })
export class CanvasContextMenu implements ContextMenuDefinitionProvider {
    private updatePositionRequestId = 0;

    readonly VERSION_MENU = {
        id: 'version',
        menuItems: [
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.supportsStartFlowVersioning(selection);
                },
                clazz: 'fa fa-upload',
                text: 'Start Version Control',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    let pgId;
                    if (selection.empty()) {
                        pgId = this.canvasUtils.getProcessGroupId();
                    } else {
                        pgId = selection.datum().id;
                    }
                    this.store.dispatch(
                        openSaveVersionDialogRequest({
                            request: {
                                processGroupId: pgId
                            }
                        })
                    );
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.supportsCommitFlowVersion(selection);
                },
                clazz: 'fa fa-upload',
                text: 'Commit Local Changes',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    let pgId;
                    if (selection.empty()) {
                        pgId = this.canvasUtils.getProcessGroupId();
                    } else {
                        pgId = selection.datum().id;
                    }
                    this.store.dispatch(
                        openCommitLocalChangesDialogRequest({
                            request: {
                                processGroupId: pgId
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.supportsForceCommitFlowVersion(selection);
                },
                clazz: 'fa fa-upload',
                text: 'Commit Local Changes',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    let pgId;
                    if (selection.empty()) {
                        pgId = this.canvasUtils.getProcessGroupId();
                    } else {
                        pgId = selection.datum().id;
                    }
                    this.store.dispatch(
                        openForceCommitLocalChangesDialogRequest({
                            request: {
                                processGroupId: pgId,
                                forceCommit: true
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.hasLocalChanges(selection);
                },
                clazz: 'fa',
                text: 'Show Local Changes',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    let pgId;
                    if (selection.empty()) {
                        pgId = this.canvasUtils.getProcessGroupId();
                    } else {
                        pgId = selection.datum().id;
                    }
                    const request: OpenLocalChangesDialogRequest = {
                        processGroupId: pgId
                    };
                    this.store.dispatch(openShowLocalChangesDialogRequest({ request }));
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.hasLocalChanges(selection);
                },
                clazz: 'fa fa-undo',
                text: 'Revert Local Changes',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    let pgId;
                    if (selection.empty()) {
                        pgId = this.canvasUtils.getProcessGroupId();
                    } else {
                        pgId = selection.datum().id;
                    }
                    const request: OpenLocalChangesDialogRequest = {
                        processGroupId: pgId
                    };
                    this.store.dispatch(openRevertLocalChangesDialogRequest({ request }));
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.supportsChangeFlowVersion(selection);
                },
                clazz: 'fa',
                text: 'Change Version',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    let pgId;
                    if (selection.empty()) {
                        pgId = this.canvasUtils.getProcessGroupId();
                    } else {
                        pgId = selection.datum().id;
                    }
                    const request: OpenChangeVersionDialogRequest = {
                        processGroupId: pgId
                    };
                    this.store.dispatch(openChangeVersionDialogRequest({ request }));
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.supportsStopFlowVersioning(selection);
                },
                clazz: 'fa',
                text: 'Stop Version Control',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    let pgId;
                    if (selection.empty()) {
                        pgId = this.canvasUtils.getProcessGroupId();
                    } else {
                        pgId = selection.datum().id;
                    }
                    const request: ConfirmStopVersionControlRequest = {
                        processGroupId: pgId
                    };
                    this.store.dispatch(stopVersionControlRequest({ request }));
                }
            }
        ]
    };

    readonly PROVENANCE_REPLAY = {
        id: 'provenance-replay',
        menuItems: [
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canReplayComponentProvenance(selection);
                },
                clazz: 'fa',
                text: 'All Nodes',
                action: (selection: any) => {
                    const selectionData = selection.datum();
                    this.store.dispatch(
                        replayLastProvenanceEvent({
                            request: {
                                componentId: selectionData.id,
                                nodes: 'ALL'
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canReplayComponentProvenance(selection);
                },
                clazz: 'fa',
                text: 'Primary Node',
                action: (selection: any) => {
                    const selectionData = selection.datum();
                    this.store.dispatch(
                        replayLastProvenanceEvent({
                            request: {
                                componentId: selectionData.id,
                                nodes: 'PRIMARY'
                            }
                        })
                    );
                }
            }
        ]
    };

    readonly UPSTREAM_DOWNSTREAM = {
        id: 'upstream-downstream',
        menuItems: [
            {
                condition: (selection: any) => {
                    // TODO - hasUpstream
                    return false;
                },
                clazz: 'icon',
                text: 'Upstream',
                action: () => {
                    // TODO - showUpstream
                }
            },
            {
                condition: (selection: any) => {
                    // TODO - hasDownstream
                    return false;
                },
                clazz: 'icon',
                text: 'Downstream',
                action: () => {
                    // TODO - showDownstream
                }
            }
        ]
    };

    readonly ALIGN = {
        id: 'align',
        menuItems: [
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canAlign(selection);
                },
                clazz: 'fa fa-align-center fa-rotate-90',
                text: 'Horizontally',
                action: (selection: any) => {
                    const componentUpdates: Map<string, UpdateComponentRequest> = new Map();
                    const connectionUpdates: Map<string, UpdateComponentRequest> = new Map();

                    // determine the extent
                    let minY: number = 0,
                        maxY: number = 0;
                    selection.each((d: any) => {
                        if (d.type !== ComponentType.Connection) {
                            if (minY === 0 || d.position.y < minY) {
                                minY = d.position.y;
                            }
                            const componentMaxY = d.position.y + d.dimensions.height;
                            if (maxY === 0 || componentMaxY > maxY) {
                                maxY = componentMaxY;
                            }
                        }
                    });

                    const center = (minY + maxY) / 2;

                    // align all components with top most component
                    selection.each((d: any) => {
                        if (d.type !== ComponentType.Connection) {
                            const delta = {
                                x: 0,
                                y: center - (d.position.y + d.dimensions.height / 2)
                            };

                            // if this component is already centered, no need to updated it
                            if (delta.y !== 0) {
                                // consider any connections
                                const connections = this.canvasUtils.getComponentConnections(d.id);

                                connections.forEach((connection: any) => {
                                    const connectionSelection = d3.select('#id-' + connection.id);

                                    if (
                                        !connectionUpdates.has(connection.id) &&
                                        this.canvasUtils.getConnectionSourceComponentId(connection) ===
                                            this.canvasUtils.getConnectionDestinationComponentId(connection)
                                    ) {
                                        // this connection is self looping and hasn't been updated by the delta yet
                                        const connectionUpdate = this.draggableBehavior.updateConnectionPosition(
                                            connectionSelection.datum(),
                                            delta
                                        );
                                        if (connectionUpdate !== null) {
                                            connectionUpdates.set(connection.id, connectionUpdate);
                                        }
                                    } else if (
                                        !connectionUpdates.has(connection.id) &&
                                        connectionSelection.classed('selected') &&
                                        this.canvasUtils.canModify(connectionSelection)
                                    ) {
                                        // this is a selected connection that hasn't been updated by the delta yet
                                        if (
                                            this.canvasUtils.getConnectionSourceComponentId(connection) === d.id ||
                                            !this.canvasUtils.isSourceSelected(connection, selection)
                                        ) {
                                            // the connection is either outgoing or incoming when the source of the connection is not part of the selection
                                            const connectionUpdate = this.draggableBehavior.updateConnectionPosition(
                                                connectionSelection.datum(),
                                                delta
                                            );
                                            if (connectionUpdate !== null) {
                                                connectionUpdates.set(connection.id, connectionUpdate);
                                            }
                                        }
                                    }
                                });

                                componentUpdates.set(d.id, this.draggableBehavior.updateComponentPosition(d, delta));
                            }
                        }
                    });

                    if (connectionUpdates.size > 0 || componentUpdates.size > 0) {
                        // dispatch the position updates
                        this.store.dispatch(
                            updatePositions({
                                request: {
                                    requestId: this.updatePositionRequestId++,
                                    componentUpdates: Array.from(componentUpdates.values()),
                                    connectionUpdates: Array.from(connectionUpdates.values())
                                }
                            })
                        );
                    }
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canAlign(selection);
                },
                clazz: 'fa fa-align-center',
                text: 'Vertically',
                action: (selection: any) => {
                    const componentUpdates: Map<string, UpdateComponentRequest> = new Map();
                    const connectionUpdates: Map<string, UpdateComponentRequest> = new Map();

                    // determine the extent
                    let minX = 0;
                    let maxX = 0;
                    selection.each((d: any) => {
                        if (d.type !== ComponentType.Connection) {
                            if (minX === 0 || d.position.x < minX) {
                                minX = d.position.x;
                            }
                            const componentMaxX = d.position.x + d.dimensions.width;
                            if (maxX === 0 || componentMaxX > maxX) {
                                maxX = componentMaxX;
                            }
                        }
                    });

                    const center = (minX + maxX) / 2;

                    // align all components with top most component
                    selection.each((d: any) => {
                        if (d.type !== ComponentType.Connection) {
                            const delta = {
                                x: center - (d.position.x + d.dimensions.width / 2),
                                y: 0
                            };

                            // if this component is already centered, no need to updated it
                            if (delta.x !== 0) {
                                // consider any connections
                                const connections = this.canvasUtils.getComponentConnections(d.id);
                                connections.forEach((connection: any) => {
                                    const connectionSelection = d3.select('#id-' + connection.id);

                                    if (
                                        !connectionUpdates.has(connection.id) &&
                                        this.canvasUtils.getConnectionSourceComponentId(connection) ===
                                            this.canvasUtils.getConnectionDestinationComponentId(connection)
                                    ) {
                                        // this connection is self looping and hasn't been updated by the delta yet
                                        const connectionUpdate = this.draggableBehavior.updateConnectionPosition(
                                            connectionSelection.datum(),
                                            delta
                                        );
                                        if (connectionUpdate !== null) {
                                            connectionUpdates.set(connection.id, connectionUpdate);
                                        }
                                    } else if (
                                        !connectionUpdates.has(connection.id) &&
                                        connectionSelection.classed('selected') &&
                                        this.canvasUtils.canModify(connectionSelection)
                                    ) {
                                        // this is a selected connection that hasn't been updated by the delta yet
                                        if (
                                            this.canvasUtils.getConnectionSourceComponentId(connection) === d.id ||
                                            !this.canvasUtils.isSourceSelected(connection, selection)
                                        ) {
                                            // the connection is either outgoing or incoming when the source of the connection is not part of the selection
                                            const connectionUpdate = this.draggableBehavior.updateConnectionPosition(
                                                connectionSelection.datum(),
                                                delta
                                            );
                                            if (connectionUpdate !== null) {
                                                connectionUpdates.set(connection.id, connectionUpdate);
                                            }
                                        }
                                    }
                                });

                                componentUpdates.set(d.id, this.draggableBehavior.updateComponentPosition(d, delta));
                            }
                        }
                    });

                    if (connectionUpdates.size > 0 || componentUpdates.size > 0) {
                        // dispatch the position updates
                        this.store.dispatch(
                            updatePositions({
                                request: {
                                    requestId: this.updatePositionRequestId++,
                                    componentUpdates: Array.from(componentUpdates.values()),
                                    connectionUpdates: Array.from(connectionUpdates.values())
                                }
                            })
                        );
                    }
                }
            }
        ]
    };

    readonly DOWNLOAD = {
        id: 'download',
        menuItems: [
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.supportsDownloadFlow(selection);
                },
                clazz: 'fa',
                text: 'Without External Services',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    let pgId;
                    if (selection.empty()) {
                        pgId = this.canvasUtils.getProcessGroupId();
                    } else {
                        pgId = selection.datum().id;
                    }
                    this.store.dispatch(
                        downloadFlow({
                            request: {
                                processGroupId: pgId,
                                includeReferencedServices: false
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.supportsDownloadFlow(selection);
                },
                clazz: 'fa',
                text: 'With External Services',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    let pgId;
                    if (selection.empty()) {
                        pgId = this.canvasUtils.getProcessGroupId();
                    } else {
                        pgId = selection.datum().id;
                    }
                    this.store.dispatch(
                        downloadFlow({
                            request: {
                                processGroupId: pgId,
                                includeReferencedServices: true
                            }
                        })
                    );
                }
            }
        ]
    };

    readonly ROOT_MENU: ContextMenuDefinition = {
        id: 'root',
        menuItems: [
            {
                condition: this.canvasActionsService.getConditionFunction('refresh'),
                clazz: 'fa fa-refresh',
                text: 'Refresh',
                action: this.canvasActionsService.getActionFunction('refresh'),
                shortcut: {
                    control: true,
                    code: 'R'
                }
            },
            {
                condition: this.canvasActionsService.getConditionFunction('leaveGroup'),
                clazz: 'fa fa-level-up',
                text: 'Leave Group',
                action: this.canvasActionsService.getActionFunction('leaveGroup'),
                shortcut: {
                    code: 'ESC'
                }
            },
            {
                isSeparator: true
            },
            {
                condition: this.canvasActionsService.getConditionFunction('configure'),
                clazz: 'fa fa-gear',
                text: 'Configure',
                action: this.canvasActionsService.getActionFunction('configure')
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.hasDetails(selection);
                },
                clazz: 'fa fa-gear',
                text: 'View Configuration',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    if (selection.empty()) {
                        this.store.dispatch(navigateToEditCurrentProcessGroup());
                    } else {
                        const selectionData = selection.datum();
                        this.store.dispatch(
                            navigateToEditComponent({
                                request: {
                                    type: selectionData.type,
                                    id: selectionData.id
                                }
                            })
                        );
                    }
                }
            },
            {
                condition: (selection: any) => {
                    if (this.canvasUtils.canRead(selection) && this.canvasUtils.isProcessor(selection)) {
                        const selectionData = selection.datum();
                        return !!selectionData.component.config.customUiUrl;
                    }
                    return false;
                },
                clazz: 'fa fa-cogs',
                text: 'Advanced',
                action: (selection: any) => {
                    const selectionData = selection.datum();
                    this.store.dispatch(
                        navigateToAdvancedProcessorUi({
                            id: selectionData.id
                        })
                    );
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isProcessGroup(selection) || selection.empty();
                },
                clazz: 'fa fa-list',
                text: 'Controller Services',
                action: (selection: any) => {
                    if (selection.empty()) {
                        this.store.dispatch(
                            navigateToControllerServicesForProcessGroup({
                                request: {
                                    id: this.canvasUtils.getProcessGroupId()
                                }
                            })
                        );
                    } else {
                        const selectionData = selection.datum();
                        this.store.dispatch(
                            navigateToControllerServicesForProcessGroup({
                                request: {
                                    id: selectionData.id
                                }
                            })
                        );
                    }
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.hasParameterContext(selection);
                },
                clazz: 'fa fa-list-alt primary-color',
                text: 'Parameters',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    let id;
                    let backNavigation;
                    if (selection.empty()) {
                        id = this.canvasUtils.getParameterContextId();

                        backNavigation = {
                            route: ['/process-groups', this.canvasUtils.getProcessGroupId()],
                            routeBoundary: ['/parameter-contexts'],
                            context: 'Process Group'
                        } as BackNavigation;
                    } else {
                        const selectionData = selection.datum();
                        id = selectionData.parameterContext.id;

                        backNavigation = {
                            route: [
                                '/process-groups',
                                this.canvasUtils.getProcessGroupId(),
                                ComponentType.ProcessGroup,
                                selectionData.id
                            ],
                            routeBoundary: ['/parameter-contexts'],
                            context: 'Process Group'
                        } as BackNavigation;
                    }

                    if (id) {
                        this.store.dispatch(
                            navigateToParameterContext({
                                request: {
                                    id,
                                    backNavigation
                                }
                            })
                        );
                    }
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.supportsFlowVersioning(selection);
                },
                text: 'Version',
                subMenuId: this.VERSION_MENU.id
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isProcessGroup(selection);
                },
                clazz: 'fa fa-sign-in',
                text: 'Enter Group',
                action: (selection: any) => {
                    const d: any = selection.datum();

                    // enter the selected group
                    this.store.dispatch(
                        enterProcessGroup({
                            request: {
                                id: d.id
                            }
                        })
                    );
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    const startable = this.canvasUtils.getStartable(selection);

                    // To mimic the operation palette behavior, offer the start context menu option if any of the selected items
                    // are runnable or can start transmitting. However, if all the startable components are RGPs, we will defer
                    // to the Enable Transmission menu option and not show the start option.
                    const allRpgs =
                        !startable.empty() &&
                        startable.filter((d: any) => d.type === ComponentType.RemoteProcessGroup).size() ===
                            startable.size();

                    return this.canvasActionsService.getConditionFunction('start')(selection) && !allRpgs;
                },
                clazz: 'fa fa-play',
                text: 'Start',
                action: this.canvasActionsService.getActionFunction('start')
            },
            {
                condition: (selection: any) => {
                    const stoppable = this.canvasUtils.getStoppable(selection);

                    // To mimic the operation palette behavior, offer the stop context menu option if any of the selected items
                    // are runnable or can stop transmitting. However, if all the stoppable components are RGPs, we will defer
                    // to the Disable Transmission menu option and not show the start option.
                    const allRpgs =
                        !stoppable.empty() &&
                        stoppable.filter((d: any) => d.type === ComponentType.RemoteProcessGroup).size() ===
                            stoppable.size();

                    return this.canvasActionsService.getConditionFunction('stop')(selection) && !allRpgs;
                },
                clazz: 'fa fa-stop',
                text: 'Stop',
                action: this.canvasActionsService.getActionFunction('stop')
            },
            {
                condition: (selection: any) => {
                    if (selection.size() !== 1) {
                        return false;
                    }
                    return this.canvasUtils.areRunnable(selection) && this.canvasUtils.isProcessor(selection);
                },
                clazz: 'fa fa-caret-right',
                text: 'Run Once',
                action: (selection: any) => {
                    const d: any = selection.datum();
                    this.store.dispatch(
                        runOnce({
                            request: {
                                uri: d.uri,
                                revision: this.client.getRevision(d)
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.canTerminate(selection);
                },
                clazz: 'fa fa-hourglass-end',
                text: 'Terminate',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    const d: any = selection.datum();
                    this.store.dispatch(
                        terminateThreads({
                            request: {
                                id: d.id,
                                uri: d.uri
                            }
                        })
                    );
                }
            },
            {
                condition: this.canvasActionsService.getConditionFunction('enable'),
                clazz: 'fa fa-flash',
                text: 'Enable',
                action: this.canvasActionsService.getActionFunction('enable')
            },
            {
                condition: this.canvasActionsService.getConditionFunction('disable'),
                clazz: 'icon icon-enable-false',
                text: 'Disable',
                action: this.canvasActionsService.getActionFunction('disable')
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canAllStartTransmitting(selection);
                },
                clazz: 'fa fa-bullseye',
                text: 'Enable Transmission',
                action: this.canvasActionsService.getActionFunction('start')
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canAllStopTransmitting(selection);
                },
                clazz: 'icon icon-transmit-false',
                text: 'Disable Transmission',
                action: this.canvasActionsService.getActionFunction('stop')
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.isProcessGroup(selection) || this.canvasUtils.emptySelection(selection);
                },
                clazz: 'fa fa-flash',
                text: 'Enable All Controller Services',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    if (selection.empty()) {
                        this.store.dispatch(FlowActions.enableControllerServicesInCurrentProcessGroup());
                    } else {
                        const d: any = selection.datum();
                        this.store.dispatch(FlowActions.enableControllerServicesInProcessGroup({ id: d.id }));
                    }
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.isProcessGroup(selection) || this.canvasUtils.emptySelection(selection);
                },
                clazz: 'icon icon-enable-false',
                text: 'Disable All Controller Services',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    if (selection.empty()) {
                        this.store.dispatch(FlowActions.disableControllerServicesInCurrentProcessGroup());
                    } else {
                        const d: any = selection.datum();
                        this.store.dispatch(FlowActions.disableControllerServicesInProcessGroup({ id: d.id }));
                    }
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canAccessComponentProvenance(selection);
                },
                clazz: 'icon icon-provenance',
                // imgStyle: 'context-menu-provenance',
                text: 'View Data Provenance',
                action: (selection: any) => {
                    const selectionData = selection.datum();
                    this.store.dispatch(
                        navigateToProvenanceForComponent({
                            id: selectionData.id
                        })
                    );
                }
            },
            {
                clazz: 'fa fa-repeat',
                text: 'Replay Last Event',
                subMenuId: this.PROVENANCE_REPLAY.id
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canViewStatusHistory(selection);
                },
                clazz: 'fa fa-area-chart',
                text: 'View Status History',
                action: (selection: any) => {
                    if (this.canvasUtils.emptySelection(selection)) {
                        this.store.dispatch(FlowActions.navigateToViewStatusHistoryForCurrentProcessGroup());
                    } else {
                        const selectionData = selection.datum();
                        this.store.dispatch(
                            navigateToViewStatusHistoryForComponent({
                                request: {
                                    type: selectionData.type,
                                    id: selectionData.id
                                }
                            })
                        );
                    }
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isStatefulProcessor(selection);
                },
                clazz: 'fa fa-tasks',
                text: 'View State',
                action: (selection: any) => {
                    const selectionData = selection.datum();
                    this.store.dispatch(
                        getComponentStateAndOpenDialog({
                            request: {
                                componentName: selectionData.component.name,
                                componentUri: selectionData.uri,
                                canClear: this.canvasUtils.isConfigurable(selection)
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isConnection(selection);
                },
                clazz: 'fa fa-list',
                text: 'List Queue',
                action: (selection: any) => {
                    const selectionData = selection.datum();
                    this.store.dispatch(
                        navigateToQueueListing({
                            request: {
                                connectionId: selectionData.id
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: any) => {
                    return (
                        this.canvasUtils.canRead(selection) &&
                        selection.size() === 1 &&
                        this.canvasUtils.isProcessor(selection)
                    );
                },
                clazz: 'fa fa-book',
                text: 'View Documentation',
                action: (selection: any) => {
                    const processGroupId = this.canvasUtils.getProcessGroupId();
                    const selectionData = selection.datum();
                    this.store.dispatch(
                        navigateToComponentDocumentation({
                            request: {
                                backNavigation: {
                                    route: [
                                        '/process-groups',
                                        processGroupId,
                                        ComponentType.Processor,
                                        selectionData.id
                                    ],
                                    routeBoundary: ['/documentation'],
                                    context: 'Processor'
                                } as BackNavigation,
                                parameters: {
                                    componentType: ComponentType.Processor,
                                    type: selectionData.component.type,
                                    group: selectionData.component.bundle.group,
                                    artifact: selectionData.component.bundle.artifact,
                                    version: selectionData.component.bundle.version
                                }
                            }
                        })
                    );
                }
            },
            {
                clazz: 'icon icon-connect',
                text: 'View Connections',
                subMenuId: this.UPSTREAM_DOWNSTREAM.id
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canRead(selection) && this.canvasUtils.isRemoteProcessGroup(selection);
                },
                clazz: 'fa fa-refresh',
                text: 'Refresh Remote',
                action: (selection: any) => {
                    const d = selection.datum();
                    const id = d.id;
                    const refreshTimestamp = d.component.flowRefreshed;
                    const request = {
                        id,
                        refreshTimestamp
                    };
                    this.store.dispatch(requestRefreshRemoteProcessGroup({ request }));
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canRead(selection) && this.canvasUtils.isRemoteProcessGroup(selection);
                },
                clazz: 'fa fa-cloud',
                text: 'Manage Remote Ports',
                action: (selection: any) => {
                    const selectionData = selection.datum();

                    this.store.dispatch(
                        navigateToManageRemotePorts({
                            request: {
                                id: selectionData.id
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: any) => {
                    return (
                        this.canvasUtils.supportsManagedAuthorizer() && this.canvasUtils.canManagePolicies(selection)
                    );
                },
                clazz: 'fa fa-key',
                text: 'Manage Access Policies',
                action: (selection: any) => {
                    this.canvasActionsService.getActionFunction('manageAccess')(selection, {
                        processGroupId: this.canvasUtils.getProcessGroupId()
                    });
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.canChangeProcessorVersion(selection);
                },
                clazz: 'fa fa-exchange',
                text: 'Change Version',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    const data = selection.datum();
                    this.store.dispatch(
                        openChangeProcessorVersionDialog({
                            request: {
                                id: data.component.id,
                                uri: data.uri,
                                revision: data.revision,
                                type: data.component.type,
                                bundle: data.component.bundle
                            }
                        })
                    );
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.isConnection(selection);
                },
                clazz: 'fa fa-long-arrow-left',
                text: 'Go To Source',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    const selectionData = selection.datum();
                    const remoteConnectableType: string = this.canvasUtils.getConnectableTypeForSource(
                        ComponentType.RemoteProcessGroup
                    );

                    // if the source is remote
                    if (selectionData.sourceType == remoteConnectableType) {
                        this.store.dispatch(
                            navigateToComponent({
                                request: {
                                    id: selectionData.sourceGroupId,
                                    type: ComponentType.RemoteProcessGroup
                                }
                            })
                        );
                    } else {
                        const type: ComponentType | null = this.canvasUtils.getComponentTypeForSource(
                            selectionData.sourceType
                        );

                        if (type) {
                            this.store.dispatch(
                                navigateToComponent({
                                    request: {
                                        id: selectionData.sourceId,
                                        processGroupId: selectionData.sourceGroupId,
                                        type
                                    }
                                })
                            );
                        }
                    }
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.isConnection(selection);
                },
                clazz: 'fa fa-long-arrow-right',
                text: 'Go To Destination',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    const selectionData = selection.datum();
                    const remoteConnectableType: string = this.canvasUtils.getConnectableTypeForDestination(
                        ComponentType.RemoteProcessGroup
                    );

                    // if the source is remote
                    if (selectionData.destinationType == remoteConnectableType) {
                        this.store.dispatch(
                            navigateToComponent({
                                request: {
                                    id: selectionData.destinationGroupId,
                                    type: ComponentType.RemoteProcessGroup
                                }
                            })
                        );
                    } else {
                        const type: ComponentType | null = this.canvasUtils.getComponentTypeForDestination(
                            selectionData.destinationType
                        );

                        if (type) {
                            this.store.dispatch(
                                navigateToComponent({
                                    request: {
                                        id: selectionData.destinationId,
                                        processGroupId: selectionData.destinationGroupId,
                                        type
                                    }
                                })
                            );
                        }
                    }
                }
            },
            {
                isSeparator: true
            },
            {
                clazz: 'fa',
                text: 'Align',
                subMenuId: this.ALIGN.id
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.canMoveToFront(selection);
                },
                clazz: 'fa fa-clone',
                text: 'Bring To Front',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    const selectionData = selection.datum();

                    this.store.dispatch(
                        moveToFront({
                            request: {
                                componentType: selectionData.type,
                                id: selectionData.id,
                                uri: selectionData.uri,
                                revision: this.client.getRevision(selectionData),
                                zIndex: selectionData.zIndex
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return !selection.empty();
                },
                clazz: 'fa fa-crosshairs',
                text: 'Center In View',
                action: () => {
                    this.store.dispatch(centerSelectedComponents({ request: { allowTransition: true } }));
                }
            },
            {
                condition: this.canvasActionsService.getConditionFunction('changeColor'),
                clazz: 'fa fa-paint-brush',
                text: 'Change Color',
                action: this.canvasActionsService.getActionFunction('changeColor')
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.canRead(selection) && this.canvasUtils.isRemoteProcessGroup(selection);
                },
                clazz: 'fa fa-external-link',
                text: 'Go To',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    const selectionData = selection.datum();
                    const uri = selectionData.component.targetUri;

                    this.store.dispatch(goToRemoteProcessGroup({ request: { uri } }));
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    if (selection.empty()) {
                        return false;
                    }

                    if (!this.canvasUtils.canModify(selection)) {
                        return false;
                    }

                    return (
                        this.canvasUtils.isNotRootGroup() &&
                        this.canvasUtils.canModifyParentGroup() &&
                        this.canvasUtils.isDisconnected(selection)
                    );
                },
                clazz: 'fa fa-arrows',
                text: 'Move To Parent Group',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    const components: MoveComponentRequest[] = [];
                    selection.each(function (d: any) {
                        components.push({
                            id: d.id,
                            type: d.type,
                            uri: d.uri,
                            entity: d
                        });
                    });

                    // move the selection into the group
                    this.store.dispatch(
                        moveComponents({
                            request: {
                                components,
                                // @ts-ignore
                                groupId: this.canvasUtils.getParentProcessGroupId()
                            }
                        })
                    );
                }
            },
            {
                condition: this.canvasActionsService.getConditionFunction('group'),
                clazz: 'fa icon-group',
                text: 'Group',
                action: this.canvasActionsService.getActionFunction('group')
            },
            {
                isSeparator: true
            },
            {
                clazz: 'fa',
                text: 'Download Flow Definition',
                subMenuId: this.DOWNLOAD.id
            },
            {
                isSeparator: true
            },
            {
                condition: this.canvasActionsService.getConditionFunction('copy'),
                clazz: 'fa fa-copy',
                text: 'Copy',
                action: this.canvasActionsService.getActionFunction('copy'),
                shortcut: {
                    control: true,
                    code: 'C'
                }
            },
            {
                condition: () => {
                    return this.canvasActionsService.getConditionFunction('paste')(d3.select(null));
                },
                clazz: 'fa fa-paste',
                text: 'Paste',
                action: (selection: d3.Selection<any, any, any, any>, event) => {
                    if (event) {
                        const pasteLocation = this.canvasView.getCanvasPosition({ x: event.pageX, y: event.pageY });
                        if (pasteLocation) {
                            this.canvasActionsService.getActionFunction('paste')(selection, { pasteLocation });
                        }
                    }
                },
                shortcut: {
                    control: true,
                    code: 'V'
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isConnection(selection);
                },
                clazz: 'fa fa-minus-circle',
                text: 'Empty Queue',
                action: (selection: any) => {
                    const selectionData = selection.datum();

                    this.store.dispatch(
                        promptEmptyQueueRequest({
                            request: {
                                connectionId: selectionData.id
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: any) => {
                    return selection.empty() || this.canvasUtils.isProcessGroup(selection);
                },
                clazz: 'fa fa-minus-circle',
                text: 'Empty All Queues',
                action: (selection: any) => {
                    let processGroupId: string;
                    if (selection.empty()) {
                        processGroupId = this.canvasUtils.getProcessGroupId();
                    } else {
                        const selectionData = selection.datum();
                        processGroupId = selectionData.id;
                    }

                    this.store.dispatch(
                        promptEmptyQueuesRequest({
                            request: {
                                processGroupId
                            }
                        })
                    );
                }
            },
            {
                condition: this.canvasActionsService.getConditionFunction('delete'),
                clazz: 'fa fa-trash',
                text: 'Delete',
                action: this.canvasActionsService.getActionFunction('delete'),
                shortcut: {
                    code: '⌫'
                }
            }
        ]
    };

    private allMenus: Map<string, ContextMenuDefinition>;

    constructor(
        private store: Store<CanvasState>,
        private canvasUtils: CanvasUtils,
        private client: Client,
        private canvasView: CanvasView,
        private canvasActionsService: CanvasActionsService,
        private draggableBehavior: DraggableBehavior
    ) {
        this.allMenus = new Map<string, ContextMenuDefinition>();
        this.allMenus.set(this.ROOT_MENU.id, this.ROOT_MENU);
        this.allMenus.set(this.PROVENANCE_REPLAY.id, this.PROVENANCE_REPLAY);
        this.allMenus.set(this.VERSION_MENU.id, this.VERSION_MENU);
        this.allMenus.set(this.UPSTREAM_DOWNSTREAM.id, this.UPSTREAM_DOWNSTREAM);
        this.allMenus.set(this.ALIGN.id, this.ALIGN);
        this.allMenus.set(this.DOWNLOAD.id, this.DOWNLOAD);
    }

    getMenu(menuId: string): ContextMenuDefinition | undefined {
        return this.allMenus.get(menuId);
    }

    filterMenuItem(menuItem: ContextMenuItemDefinition): boolean {
        const selection = this.canvasUtils.getSelection();

        // include if the condition matches
        if (menuItem.condition) {
            return menuItem.condition(selection);
        }

        // include if there is no condition (non conditional item, separator, sub menu, etc)
        return true;
    }

    menuItemClicked(menuItem: ContextMenuItemDefinition, event: MouseEvent): void {
        if (menuItem.action) {
            const selection = this.canvasUtils.getSelection();
            menuItem.action(selection, event);
        }
    }
}
