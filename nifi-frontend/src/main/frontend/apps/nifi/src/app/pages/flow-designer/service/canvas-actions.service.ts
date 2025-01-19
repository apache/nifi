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

import { Injectable } from '@angular/core';
import { CanvasUtils } from './canvas-utils.service';
import {
    copySuccess,
    deleteComponents,
    disableComponents,
    disableCurrentProcessGroup,
    enableComponents,
    enableCurrentProcessGroup,
    getParameterContextsAndOpenGroupComponentsDialog,
    leaveProcessGroup,
    navigateToEditComponent,
    navigateToEditCurrentProcessGroup,
    navigateToManageComponentPolicies,
    openChangeColorDialog,
    reloadFlow,
    selectComponents,
    startComponents,
    startCurrentProcessGroup,
    stopComponents,
    stopCurrentProcessGroup
} from '../state/flow/flow.actions';
import {
    ChangeColorRequest,
    DeleteComponentRequest,
    DisableComponentRequest,
    EnableComponentRequest,
    MoveComponentRequest,
    SelectedComponent,
    StartComponentRequest,
    StopComponentRequest
} from '../state/flow';
import { Store } from '@ngrx/store';
import { CanvasState } from '../state';
import * as d3 from 'd3';
import { MatDialog } from '@angular/material/dialog';
import { CanvasView } from './canvas-view.service';
import { ComponentType } from '@nifi/shared';
import { Client } from '../../../service/client.service';
import { CopyRequestContext, CopyRequestEntity, CopyResponseEntity } from '../../../state/copy';
import { CopyPasteService } from './copy-paste.service';
import { firstValueFrom } from 'rxjs';
import { selectCurrentProcessGroupId } from '../state/flow/flow.selectors';
import { snackBarError } from '../../../state/error/error.actions';

export type CanvasConditionFunction = (selection: d3.Selection<any, any, any, any>) => boolean;
export type CanvasActionFunction = (selection: d3.Selection<any, any, any, any>, extraArgs?: any) => void;

export interface CanvasAction {
    id: string;
    condition: CanvasConditionFunction;
    action: CanvasActionFunction;
}

export interface CanvasActions {
    [key: string]: CanvasAction;
}

@Injectable({
    providedIn: 'root'
})
export class CanvasActionsService {
    private _actions: CanvasActions = {
        delete: {
            id: 'delete',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                return this.canvasUtils.areDeletable(selection);
            },
            action: (selection: d3.Selection<any, any, any, any>) => {
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
                    selection.each((d: any) => {
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
        },
        refresh: {
            id: 'refresh',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                return this.canvasUtils.emptySelection(selection);
            },
            action: () => {
                this.store.dispatch(reloadFlow());
            }
        },
        leaveGroup: {
            id: 'leaveGroup',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                const dialogsAreOpen = this.dialog.openDialogs.length > 0;
                return this.canvasUtils.isNotRootGroupAndEmptySelection(selection) && !dialogsAreOpen;
            },
            action: () => {
                this.store.dispatch(leaveProcessGroup());
            }
        },
        copy: {
            id: 'copy',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                return this.canvasUtils.isCopyable(selection);
            },
            action: (selection: d3.Selection<any, any, any, any>) => {
                const copyRequestEntity: CopyRequestEntity = {};
                selection.each((d) => {
                    switch (d.type) {
                        case ComponentType.Processor:
                            if (!copyRequestEntity.processors) {
                                copyRequestEntity.processors = [];
                            }
                            copyRequestEntity.processors.push(d.id);
                            break;
                        case ComponentType.ProcessGroup:
                            if (!copyRequestEntity.processGroups) {
                                copyRequestEntity.processGroups = [];
                            }
                            copyRequestEntity.processGroups.push(d.id);
                            break;
                        case ComponentType.Connection:
                            if (!copyRequestEntity.connections) {
                                copyRequestEntity.connections = [];
                            }
                            copyRequestEntity.connections.push(d.id);
                            break;
                        case ComponentType.RemoteProcessGroup:
                            if (!copyRequestEntity.remoteProcessGroups) {
                                copyRequestEntity.remoteProcessGroups = [];
                            }
                            copyRequestEntity.remoteProcessGroups.push(d.id);
                            break;
                        case ComponentType.InputPort:
                            if (!copyRequestEntity.inputPorts) {
                                copyRequestEntity.inputPorts = [];
                            }
                            copyRequestEntity.inputPorts.push(d.id);
                            break;
                        case ComponentType.OutputPort:
                            if (!copyRequestEntity.outputPorts) {
                                copyRequestEntity.outputPorts = [];
                            }
                            copyRequestEntity.outputPorts.push(d.id);
                            break;
                        case ComponentType.Label:
                            if (!copyRequestEntity.labels) {
                                copyRequestEntity.labels = [];
                            }
                            copyRequestEntity.labels.push(d.id);
                            break;
                        case ComponentType.Funnel:
                            if (!copyRequestEntity.funnels) {
                                copyRequestEntity.funnels = [];
                            }
                            copyRequestEntity.funnels.push(d.id);
                            break;
                    }
                });

                const copyRequestContext: CopyRequestContext = {
                    copyRequestEntity,
                    processGroupId: this.currentProcessGroupId()
                };
                let copyResponse: CopyResponseEntity | null = null;

                // Safari in particular is strict in enforcing that any writing to the clipboard needs to be triggered directly by a user action.
                // As such, firing a simple async rxjs action to initiate the copy sequence fails this check.
                // However, below is the workaround to construct a ClipboardItem from an async call.
                const clipboardItem = new ClipboardItem({
                    'text/plain': firstValueFrom(this.copyService.copy(copyRequestContext)).then((response) => {
                        copyResponse = response;
                        return new Blob([JSON.stringify(response, null, 2)], { type: 'text/plain' });
                    })
                });
                navigator.clipboard
                    .write([clipboardItem])
                    .then(() => {
                        if (copyResponse) {
                            this.store.dispatch(
                                copySuccess({
                                    response: {
                                        copyResponse,
                                        processGroupId: copyRequestContext.processGroupId,
                                        pasteCount: 0
                                    }
                                })
                            );
                        } else {
                            this.store.dispatch(snackBarError({ error: 'Copy failed' }));
                        }
                    })
                    .catch(() => {
                        this.store.dispatch(snackBarError({ error: 'Copy failed' }));
                    });
            }
        },
        selectAll: {
            id: 'selectAll',
            condition: () => {
                return true;
            },
            action: () => {
                const selectedComponents = this.select(d3.selectAll('g.component, g.connection'));
                this.store.dispatch(
                    selectComponents({
                        request: {
                            components: selectedComponents
                        }
                    })
                );
            }
        },
        configure: {
            id: 'configure',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                return this.canvasUtils.isConfigurable(selection);
            },
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
        manageAccess: {
            id: 'manageAccess',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                return this.canvasUtils.canManagePolicies(selection);
            },
            action: (selection: d3.Selection<any, any, any, any>, extraArgs?) => {
                const routeBoundary: string[] = ['/access-policies'];

                if (extraArgs?.processGroupId) {
                    if (selection.empty()) {
                        this.store.dispatch(
                            navigateToManageComponentPolicies({
                                request: {
                                    resource: 'process-groups',
                                    id: extraArgs.processGroupId,
                                    backNavigation: {
                                        route: ['/process-groups', extraArgs.processGroupId],
                                        routeBoundary,
                                        context: 'Process Group'
                                    }
                                }
                            })
                        );
                    } else {
                        const selectionData = selection.datum();
                        const componentType: ComponentType = selectionData.type;

                        let resource = 'process-groups';
                        let context = 'Process Group';
                        switch (componentType) {
                            case ComponentType.Processor:
                                resource = 'processors';
                                context = 'Processor';
                                break;
                            case ComponentType.InputPort:
                                resource = 'input-ports';
                                context = 'Input Port';
                                break;
                            case ComponentType.OutputPort:
                                resource = 'output-ports';
                                context = 'Output Port';
                                break;
                            case ComponentType.Funnel:
                                resource = 'funnels';
                                context = 'Funnel';
                                break;
                            case ComponentType.Label:
                                resource = 'labels';
                                context = 'Label';
                                break;
                            case ComponentType.RemoteProcessGroup:
                                resource = 'remote-process-groups';
                                context = 'Remote Process Group';
                                break;
                        }

                        this.store.dispatch(
                            navigateToManageComponentPolicies({
                                request: {
                                    resource,
                                    id: selectionData.id,
                                    backNavigation: {
                                        route: [
                                            '/process-groups',
                                            extraArgs.processGroupId,
                                            componentType,
                                            selectionData.id
                                        ],
                                        routeBoundary,
                                        context
                                    }
                                }
                            })
                        );
                    }
                }
            }
        },
        start: {
            id: 'start',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                if (!selection) {
                    return false;
                }
                return this.canvasUtils.areAnyRunnable(selection);
            },
            action: (selection: d3.Selection<any, any, any, any>) => {
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
                            revision: this.client.getRevision(d),
                            errorStrategy: 'snackbar'
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
        stop: {
            id: 'stop',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                return this.canvasUtils.areAnyStoppable(selection);
            },
            action: (selection: d3.Selection<any, any, any, any>) => {
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
                            revision: this.client.getRevision(d),
                            errorStrategy: 'snackbar'
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
        enable: {
            id: 'enable',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                return this.canvasUtils.canEnable(selection);
            },
            action: (selection: d3.Selection<any, any, any, any>) => {
                if (selection.empty()) {
                    // attempting to enable the current process group
                    this.store.dispatch(enableCurrentProcessGroup());
                } else {
                    const components: EnableComponentRequest[] = [];
                    const enableable = this.canvasUtils.filterEnable(selection);
                    enableable.each((d: any) => {
                        components.push({
                            id: d.id,
                            uri: d.uri,
                            type: d.type,
                            revision: this.client.getRevision(d),
                            errorStrategy: 'snackbar'
                        });
                    });
                    this.store.dispatch(
                        enableComponents({
                            request: {
                                components
                            }
                        })
                    );
                }
            }
        },
        disable: {
            id: 'disable',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                return this.canvasUtils.canDisable(selection);
            },
            action: (selection: d3.Selection<any, any, any, any>) => {
                if (selection.empty()) {
                    // attempting to disable the current process group
                    this.store.dispatch(disableCurrentProcessGroup());
                } else {
                    const components: DisableComponentRequest[] = [];
                    const disableable = this.canvasUtils.filterDisable(selection);
                    disableable.each((d: any) => {
                        components.push({
                            id: d.id,
                            uri: d.uri,
                            type: d.type,
                            revision: this.client.getRevision(d),
                            errorStrategy: 'snackbar'
                        });
                    });
                    this.store.dispatch(
                        disableComponents({
                            request: {
                                components
                            }
                        })
                    );
                }
            }
        },
        group: {
            id: 'group',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                return this.canvasUtils.isDisconnected(selection) && this.canvasUtils.canModify(selection);
            },
            action: (selection: d3.Selection<any, any, any, any>) => {
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
        changeColor: {
            id: 'changeColor',
            condition: (selection: d3.Selection<any, any, any, any>) => {
                return this.canvasUtils.isColorable(selection);
            },
            action: (selection: d3.Selection<any, any, any, any>) => {
                const changeColorRequests: ChangeColorRequest[] = [];
                selection.each((d) => {
                    let color = null;
                    if (d.component.style) {
                        color = d.component.style['background-color'] || null;
                    }
                    changeColorRequests.push({
                        id: d.id,
                        uri: d.uri,
                        type: d.type,
                        color,
                        style: d.component.style || null,
                        revision: this.client.getRevision(d)
                    });
                });
                this.store.dispatch(
                    openChangeColorDialog({
                        request: changeColorRequests
                    })
                );
            }
        }
    };

    currentProcessGroupId = this.store.selectSignal(selectCurrentProcessGroupId);

    constructor(
        private store: Store<CanvasState>,
        private canvasUtils: CanvasUtils,
        private canvasView: CanvasView,
        private dialog: MatDialog,
        private client: Client,
        private copyService: CopyPasteService
    ) {}

    private select(selection: d3.Selection<any, any, any, any>) {
        const selectedComponents: SelectedComponent[] = [];
        if (selection) {
            selection.each((d: any) => {
                selectedComponents.push({
                    id: d.id,
                    componentType: d.type
                });
            });
        }
        return selectedComponents;
    }

    getAction(id: string): CanvasAction | null {
        if (this._actions && this._actions[id]) {
            return this._actions[id];
        }
        return null;
    }

    getActionFunction(id: string): CanvasActionFunction {
        if (this._actions && this._actions[id]) {
            return this._actions[id].action;
        }
        return () => {};
    }

    getConditionFunction(id: string): CanvasConditionFunction {
        if (this._actions && this._actions[id]) {
            return this._actions[id].condition;
        }
        return () => false;
    }
}
