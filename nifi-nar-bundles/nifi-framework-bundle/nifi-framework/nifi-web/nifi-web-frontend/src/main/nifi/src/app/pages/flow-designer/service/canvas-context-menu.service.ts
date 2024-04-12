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
import {
    centerSelectedComponents,
    deleteComponents,
    enterProcessGroup,
    getParameterContextsAndOpenGroupComponentsDialog,
    goToRemoteProcessGroup,
    leaveProcessGroup,
    moveComponents,
    navigateToComponent,
    navigateToControllerServicesForProcessGroup,
    navigateToAdvancedProcessorUi,
    navigateToEditComponent,
    navigateToEditCurrentProcessGroup,
    navigateToManageComponentPolicies,
    navigateToManageRemotePorts,
    navigateToProvenanceForComponent,
    navigateToQueueListing,
    navigateToViewStatusHistoryForComponent,
    openChangeVersionDialogRequest,
    openCommitLocalChangesDialogRequest,
    openForceCommitLocalChangesDialogRequest,
    openRevertLocalChangesDialogRequest,
    openSaveVersionDialogRequest,
    openShowLocalChangesDialogRequest,
    reloadFlow,
    replayLastProvenanceEvent,
    requestRefreshRemoteProcessGroup,
    runOnce,
    startComponents,
    startCurrentProcessGroup,
    stopComponents,
    stopCurrentProcessGroup,
    stopVersionControlRequest,
    downloadFlow
} from '../state/flow/flow.actions';
import { ComponentType } from '../../../state/shared';
import {
    ConfirmStopVersionControlRequest,
    DeleteComponentRequest,
    MoveComponentRequest,
    OpenChangeVersionDialogRequest,
    OpenLocalChangesDialogRequest,
    StartComponentRequest,
    StopComponentRequest
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

@Injectable({ providedIn: 'root' })
export class CanvasContextMenu implements ContextMenuDefinitionProvider {
    readonly VERSION_MENU = {
        id: 'version',
        menuItems: [
            {
                condition: (selection: d3.Selection<any, any, any, any>) => {
                    return this.canvasUtils.supportsStartFlowVersioning(selection);
                },
                clazz: 'fa fa-upload',
                text: 'Start version control',
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
                text: 'Commit local changes',
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
                text: 'Commit local changes',
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
                text: 'Show local changes',
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
                text: 'Revert local changes',
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
                text: 'Change version',
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
                text: 'Stop version control',
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
                text: 'All nodes',
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
                text: 'Primary node',
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
                    return this.canvasUtils.hasUpstream(selection);
                },
                clazz: 'icon',
                text: 'Upstream',
                action: () => {
                    // TODO - showUpstream
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.hasDownstream(selection);
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
                    // TODO - canAlign
                    return false;
                },
                clazz: 'fa fa-align-center fa-rotate-90',
                text: 'Horizontally',
                action: () => {
                    // TODO - alignHorizontal
                }
            },
            {
                condition: (selection: any) => {
                    // TODO - canAlign
                    return false;
                },
                clazz: 'fa fa-align-center',
                text: 'Vertically',
                action: () => {
                    // TODO - alignVertical
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
                text: 'Without external services',
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
                text: 'With external services',
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
                condition: (selection: any) => {
                    return this.canvasUtils.emptySelection(selection);
                },
                clazz: 'fa fa-refresh',
                text: 'Refresh',
                action: () => {
                    this.store.dispatch(reloadFlow());
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isNotRootGroupAndEmptySelection(selection);
                },
                clazz: 'fa fa-level-up',
                text: 'Leave group',
                action: () => {
                    this.store.dispatch(leaveProcessGroup());
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isConfigurable(selection);
                },
                clazz: 'fa fa-gear',
                text: 'Configure',
                action: (selection: any) => {
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
                condition: (selection: any) => {
                    // TODO - hasDetails
                    return false;
                },
                clazz: 'fa fa-gear',
                text: 'View configuration',
                action: (selection: any) => {
                    // TODO - showDetails... Can we support read only and configurable in the same dialog/form?
                }
            },
            {
                condition: (selection: any) => {
                    // TODO - hasParameterContext
                    return false;
                },
                clazz: 'fa',
                text: 'Parameters',
                action: () => {
                    // TODO - open parameter context
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
                text: 'Enter group',
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
                        startable.filter((d: any) => d.type === ComponentType.RemoteProcessGroup).size() ===
                        startable.size();

                    return this.canvasUtils.areAnyRunnable(selection) && !allRpgs;
                },
                clazz: 'fa fa-play',
                text: 'Start',
                action: (selection: any) => {
                    if (selection.empty()) {
                        // attempting to start the current process group
                        this.store.dispatch(startCurrentProcessGroup());
                    } else {
                        const components: StartComponentRequest[] = [];
                        const startable = this.canvasUtils.getStartable(selection);
                        startable.each((d: any) => {
                            components.push({
                                id: d.id,
                                uri: d.uri,
                                type: d.type,
                                revision: d.revision
                            });
                        });
                        this.store.dispatch(
                            startComponents({
                                request: {
                                    components
                                }
                            })
                        );
                    }
                }
            },
            {
                condition: (selection: any) => {
                    const stoppable = this.canvasUtils.getStoppable(selection);

                    // To mimic the operation palette behavior, offer the stop context menu option if any of the selected items
                    // are runnable or can stop transmitting. However, if all the stoppable components are RGPs, we will defer
                    // to the Disable Transmission menu option and not show the start option.
                    const allRpgs =
                        stoppable.filter((d: any) => d.type === ComponentType.RemoteProcessGroup).size() ===
                        stoppable.size();

                    return this.canvasUtils.areAnyStoppable(selection) && !allRpgs;
                },
                clazz: 'fa fa-stop',
                text: 'Stop',
                action: (selection: any) => {
                    if (selection.empty()) {
                        // attempting to start the current process group
                        this.store.dispatch(stopCurrentProcessGroup());
                    } else {
                        const components: StopComponentRequest[] = [];
                        const stoppable = this.canvasUtils.getStoppable(selection);
                        stoppable.each((d: any) => {
                            components.push({
                                id: d.id,
                                uri: d.uri,
                                type: d.type,
                                revision: d.revision
                            });
                        });
                        this.store.dispatch(
                            stopComponents({
                                request: {
                                    components
                                }
                            })
                        );
                    }
                }
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
                                revision: d.revision
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: any) => {
                    // TODO - canTerminate
                    return false;
                },
                clazz: 'fa fa-hourglass-end',
                text: 'Terminate',
                action: () => {
                    // TODO - terminate
                }
            },
            {
                condition: (selection: any) => {
                    // TODO - canEnable
                    return false;
                },
                clazz: 'fa fa-flash',
                text: 'Enable',
                action: () => {
                    // TODO - enable
                }
            },
            {
                condition: (selection: any) => {
                    // TODO - canDisable
                    return false;
                },
                clazz: 'icon icon-enable-false',
                text: 'Disable',
                action: () => {
                    // TODO - disable
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canAllStartTransmitting(selection);
                },
                clazz: 'fa fa-bullseye',
                text: 'Enable transmission',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    const components: StartComponentRequest[] = [];
                    const startable = this.canvasUtils.getStartable(selection);
                    startable.each((d: any) => {
                        components.push({
                            id: d.id,
                            uri: d.uri,
                            type: d.type,
                            revision: d.revision
                        });
                    });

                    this.store.dispatch(
                        startComponents({
                            request: {
                                components
                            }
                        })
                    );
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canAllStopTransmitting(selection);
                },
                clazz: 'icon icon-transmit-false',
                text: 'Disable transmission',
                action: (selection: d3.Selection<any, any, any, any>) => {
                    const components: StopComponentRequest[] = [];

                    const stoppable = this.canvasUtils.getStoppable(selection);
                    stoppable.each((d: any) => {
                        components.push({
                            id: d.id,
                            uri: d.uri,
                            type: d.type,
                            revision: d.revision
                        });
                    });
                    this.store.dispatch(
                        stopComponents({
                            request: {
                                components
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
                    return this.canvasUtils.isProcessGroup(selection);
                },
                clazz: 'fa fa-flash',
                text: 'Enable all controller services',
                action: () => {
                    // TODO - enableAllControllerServices
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.emptySelection(selection);
                },
                clazz: 'fa fa-flash',
                text: 'Enable all controller services',
                action: () => {
                    // TODO - enableAllControllerServices
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isProcessGroup(selection);
                },
                clazz: 'icon icon-enable-false',
                text: 'Disable all controller services',
                action: () => {
                    // TODO - disableAllControllerServices
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.emptySelection(selection);
                },
                clazz: 'icon icon-enable-false',
                text: 'Disable all controller services',
                action: () => {
                    // TODO - disableAllControllerServices
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
                text: 'View data provenance',
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
                text: 'Replay last event',
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
                text: 'View status history',
                action: (selection: any) => {
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
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isStatefulProcessor(selection);
                },
                clazz: 'fa fa-tasks',
                text: 'View state',
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
                text: 'List queue',
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
                text: 'View documentation',
                action: (selection: any) => {
                    const selectionData = selection.datum();
                    this.store.dispatch(
                        navigateToComponentDocumentation({
                            params: {
                                select: selectionData.component.type,
                                group: selectionData.component.bundle.group,
                                artifact: selectionData.component.bundle.artifact,
                                version: selectionData.component.bundle.version
                            }
                        })
                    );
                }
            },
            {
                clazz: 'icon icon-connect',
                text: 'View connections',
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
                text: 'Refresh remote',
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
                text: 'Manage remote ports',
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
                text: 'Manage access policies',
                action: (selection: any) => {
                    if (selection.empty()) {
                        this.store.dispatch(
                            navigateToManageComponentPolicies({
                                request: {
                                    resource: 'process-groups',
                                    id: this.canvasUtils.getProcessGroupId()
                                }
                            })
                        );
                    } else {
                        const selectionData = selection.datum();
                        const componentType: ComponentType = selectionData.type;

                        let resource = 'process-groups';
                        switch (componentType) {
                            case ComponentType.Processor:
                                resource = 'processors';
                                break;
                            case ComponentType.InputPort:
                                resource = 'input-ports';
                                break;
                            case ComponentType.OutputPort:
                                resource = 'output-ports';
                                break;
                            case ComponentType.Funnel:
                                resource = 'funnels';
                                break;
                            case ComponentType.Label:
                                resource = 'labels';
                                break;
                            case ComponentType.RemoteProcessGroup:
                                resource = 'remote-process-groups';
                                break;
                        }

                        this.store.dispatch(
                            navigateToManageComponentPolicies({
                                request: {
                                    resource,
                                    id: selectionData.id
                                }
                            })
                        );
                    }
                }
            },
            {
                condition: (selection: any) => {
                    // TODO - canChangeProcessorVersion
                    return false;
                },
                clazz: 'fa fa-exchange',
                text: 'Change version',
                action: () => {
                    // TODO - changeVersion
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isConnection(selection);
                },
                clazz: 'fa fa-long-arrow-left',
                text: 'Go to source',
                action: (selection: any) => {
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
                condition: (selection: any) => {
                    return this.canvasUtils.isConnection(selection);
                },
                clazz: 'fa fa-long-arrow-right',
                text: 'Go to destination',
                action: (selection: any) => {
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
                condition: (selection: any) => {
                    // TODO - canMoveToFront
                    return false;
                },
                clazz: 'fa fa-clone',
                text: 'Bring to front',
                action: () => {
                    // TODO - toFront
                }
            },
            {
                condition: (selection: any) => {
                    return !selection.empty();
                },
                clazz: 'fa fa-crosshairs',
                text: 'Center in view',
                action: () => {
                    this.store.dispatch(centerSelectedComponents({ request: { allowTransition: true } }));
                }
            },
            {
                condition: (selection: any) => {
                    // TODO - isColorable
                    return false;
                },
                clazz: 'fa fa-paint-brush',
                text: 'Change color',
                action: () => {
                    // TODO - fillColor
                }
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.canRead(selection) && this.canvasUtils.isRemoteProcessGroup(selection);
                },
                clazz: 'fa fa-external-link',
                text: 'Go to',
                action: (selection: any) => {
                    const selectionData = selection.datum();
                    const uri = selectionData.component.targetUri;

                    this.store.dispatch(goToRemoteProcessGroup({ request: { uri } }));
                }
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    return this.canvasUtils.isNotRootGroup();
                },
                clazz: 'fa fa-arrows',
                text: 'Move to parent group',
                action: (selection: any) => {
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
                condition: (selection: any) => {
                    return this.canvasUtils.isDisconnected(selection);
                },
                clazz: 'fa icon-group',
                text: 'Group',
                action: (selection: any) => {
                    const moveComponents: MoveComponentRequest[] = [];
                    selection.each(function (d: any) {
                        moveComponents.push({
                            id: d.id,
                            type: d.type,
                            uri: d.uri,
                            entity: d
                        });
                    });

                    // move the selection into the group
                    this.store.dispatch(
                        getParameterContextsAndOpenGroupComponentsDialog({
                            request: {
                                moveComponents,
                                position: this.canvasUtils.getOrigin(selection)
                            }
                        })
                    );
                }
            },
            {
                isSeparator: true
            },
            {
                clazz: 'fa',
                text: 'Download flow definition',
                subMenuId: this.DOWNLOAD.id
            },
            {
                isSeparator: true
            },
            {
                condition: (selection: any) => {
                    // TODO - isCopyable
                    return false;
                },
                clazz: 'fa fa-copy',
                text: 'Copy',
                action: () => {
                    // TODO - copy
                }
            },
            {
                condition: (selection: any) => {
                    // TODO - isPastable
                    return false;
                },
                clazz: 'fa fa-paste',
                text: 'Paste',
                action: () => {
                    // TODO - paste
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
                text: 'Empty queue',
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
                text: 'Empty all queues',
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
                condition: (selection: any) => {
                    return this.canvasUtils.areDeletable(selection);
                },
                clazz: 'fa fa-trash',
                text: 'Delete',
                action: (selection: any) => {
                    if (selection.size() === 1) {
                        const selectionData = selection.datum();
                        this.store.dispatch(
                            deleteComponents({
                                request: [
                                    {
                                        id: selectionData.id,
                                        type: selectionData.type,
                                        uri: selectionData.uri,
                                        entity: selectionData
                                    }
                                ]
                            })
                        );
                    } else {
                        const requests: DeleteComponentRequest[] = [];
                        selection.each(function (d: any) {
                            requests.push({
                                id: d.id,
                                type: d.type,
                                uri: d.uri,
                                entity: d
                            });
                        });
                        this.store.dispatch(
                            deleteComponents({
                                request: requests
                            })
                        );
                    }
                }
            }
        ]
    };

    private allMenus: Map<string, ContextMenuDefinition>;

    constructor(
        private store: Store<CanvasState>,
        private canvasUtils: CanvasUtils
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
