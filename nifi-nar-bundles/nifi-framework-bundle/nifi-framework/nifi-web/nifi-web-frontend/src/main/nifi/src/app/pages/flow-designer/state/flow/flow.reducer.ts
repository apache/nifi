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

import { createReducer, on } from '@ngrx/store';
import {
    changeVersionComplete,
    changeVersionSuccess,
    clearFlowApiError,
    createComponentComplete,
    createComponentSuccess,
    createConnection,
    createFunnel,
    createLabel,
    createPort,
    createProcessGroup,
    createProcessor,
    deleteComponentsSuccess,
    flowApiError,
    flowVersionBannerError,
    groupComponents,
    groupComponentsSuccess,
    loadChildProcessGroupSuccess,
    loadConnectionSuccess,
    loadInputPortSuccess,
    loadProcessGroup,
    loadProcessGroupSuccess,
    loadProcessorSuccess,
    loadRemoteProcessGroupSuccess,
    navigateWithoutTransform,
    pollChangeVersionSuccess,
    pollRevertChangesSuccess,
    requestRefreshRemoteProcessGroup,
    resetFlowState,
    revertChangesComplete,
    revertChangesSuccess,
    runOnce,
    runOnceSuccess,
    saveToFlowRegistry,
    saveToFlowRegistrySuccess,
    setAllowTransition,
    setDragging,
    setNavigationCollapsed,
    setOperationCollapsed,
    setSkipTransform,
    setTransitionRequired,
    startComponent,
    startComponentSuccess,
    startRemoteProcessGroupPolling,
    stopComponentSuccess,
    stopRemoteProcessGroupPolling,
    stopVersionControl,
    stopVersionControlSuccess,
    updateComponent,
    updateComponentFailure,
    updateComponentSuccess,
    updateConnection,
    updateConnectionSuccess,
    updateProcessor,
    updateProcessorSuccess,
    uploadProcessGroup
} from './flow.actions';
import { FlowState } from './index';
import { ComponentType } from '../../../../state/shared';
import { produce } from 'immer';

export const initialState: FlowState = {
    id: 'root',
    changeVersionRequest: null,
    flow: {
        permissions: {
            canRead: false,
            canWrite: false
        },
        processGroupFlow: {
            id: '',
            uri: '',
            parentGroupId: null,
            breadcrumb: {
                id: '',
                permissions: {
                    canRead: false,
                    canWrite: false
                },
                versionedFlowState: '',
                breadcrumb: {
                    id: '',
                    name: ''
                }
            },
            parameterContext: null,
            flow: {
                processGroups: [],
                remoteProcessGroups: [],
                processors: [],
                inputPorts: [],
                outputPorts: [],
                connections: [],
                labels: [],
                funnels: []
            },
            lastRefreshed: ''
        }
    },
    flowStatus: {
        controllerStatus: {
            activeThreadCount: 0,
            terminatedThreadCount: 0,
            queued: '',
            flowFilesQueued: 0,
            bytesQueued: 0,
            runningCount: 0,
            stoppedCount: 0,
            invalidCount: 0,
            disabledCount: 0,
            activeRemotePortCount: 0,
            inactiveRemotePortCount: 0,
            upToDateCount: undefined,
            locallyModifiedCount: undefined,
            staleCount: undefined,
            locallyModifiedAndStaleCount: undefined,
            syncFailureCount: undefined
        }
    },
    refreshRpgDetails: null,
    controllerBulletins: {
        bulletins: [],
        controllerServiceBulletins: [],
        flowRegistryClientBulletins: [],
        parameterProviderBulletins: [],
        reportingTaskBulletins: []
    },
    dragging: false,
    saving: false,
    versionSaving: false,
    transitionRequired: false,
    skipTransform: false,
    allowTransition: false,
    navigationCollapsed: false,
    operationCollapsed: false,
    error: null,
    status: 'pending'
};

export const flowReducer = createReducer(
    initialState,
    on(resetFlowState, () => ({
        ...initialState
    })),
    on(requestRefreshRemoteProcessGroup, (state, { request }) => ({
        ...state,
        refreshRpgDetails: {
            request,
            polling: false
        }
    })),
    on(startRemoteProcessGroupPolling, (state) => {
        return produce(state, (draftState) => {
            if (draftState.refreshRpgDetails) {
                draftState.refreshRpgDetails.polling = true;
            }
        });
    }),
    on(stopRemoteProcessGroupPolling, (state) => ({
        ...state,
        refreshRpgDetails: null
    })),
    on(loadProcessGroup, (state, { request }) => ({
        ...state,
        transitionRequired: request.transitionRequired,
        status: 'loading' as const
    })),
    on(loadProcessGroupSuccess, (state, { response }) => ({
        ...state,
        id: response.flow.processGroupFlow.id,
        flow: response.flow,
        flowStatus: response.flowStatus,
        controllerBulletins: response.controllerBulletins,
        error: null,
        status: 'success' as const
    })),
    on(loadConnectionSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.flow.processGroupFlow.flow.connections.findIndex(
                (f: any) => response.id === f.id
            );
            if (componentIndex > -1) {
                draftState.flow.processGroupFlow.flow.connections[componentIndex] = response.connection;
            }
        });
    }),
    on(loadProcessorSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.flow.processGroupFlow.flow.processors.findIndex(
                (f: any) => response.id === f.id
            );
            if (componentIndex > -1) {
                draftState.flow.processGroupFlow.flow.processors[componentIndex] = response.processor;
            }
        });
    }),
    on(loadInputPortSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.flow.processGroupFlow.flow.inputPorts.findIndex(
                (f: any) => response.id === f.id
            );
            if (componentIndex > -1) {
                draftState.flow.processGroupFlow.flow.inputPorts[componentIndex] = response.inputPort;
            }
        });
    }),
    on(loadRemoteProcessGroupSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.flow.processGroupFlow.flow.remoteProcessGroups.findIndex(
                (f: any) => response.id === f.id
            );
            if (componentIndex > -1) {
                draftState.flow.processGroupFlow.flow.remoteProcessGroups[componentIndex] = response.remoteProcessGroup;
            }
        });
    }),
    on(flowApiError, (state, { error }) => ({
        ...state,
        dragging: false,
        saving: false,
        error: error,
        status: 'error' as const
    })),
    on(clearFlowApiError, (state) => ({
        ...state,
        error: null,
        status: 'pending' as const
    })),
    on(
        createProcessor,
        createProcessGroup,
        uploadProcessGroup,
        groupComponents,
        createConnection,
        createPort,
        createFunnel,
        createLabel,
        (state) => ({
            ...state,
            saving: true
        })
    ),
    on(createComponentSuccess, groupComponentsSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, response.type);

            if (collection) {
                collection.push(response.payload);
            }
        });
    }),
    on(createComponentComplete, (state) => ({
        ...state,
        dragging: false,
        saving: false
    })),
    on(updateComponent, updateProcessor, updateConnection, startComponent, runOnce, (state) => ({
        ...state,
        saving: true
    })),
    on(updateComponentSuccess, updateProcessorSuccess, updateConnectionSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, response.type);

            if (collection) {
                const componentIndex: number = collection.findIndex((f: any) => response.id === f.id);
                if (componentIndex > -1) {
                    collection[componentIndex] = response.response;
                }
            }

            draftState.saving = false;
        });
    }),
    on(updateComponentFailure, (state, { response }) => {
        return produce(state, (draftState) => {
            if (response.restoreOnFailure) {
                const collection: any[] | null = getComponentCollection(draftState, response.type);

                if (collection) {
                    const componentIndex: number = collection.findIndex((f: any) => response.id === f.id);
                    if (componentIndex > -1) {
                        const currentComponent: any = collection[componentIndex];
                        collection[componentIndex] = {
                            ...currentComponent,
                            ...response.restoreOnFailure
                        };
                    }
                }
            }

            draftState.saving = false;
        });
    }),
    on(deleteComponentsSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            response.forEach((deleteResponse) => {
                const collection: any[] | null = getComponentCollection(draftState, deleteResponse.type);

                if (collection) {
                    const componentIndex: number = collection.findIndex((f: any) => deleteResponse.id === f.id);
                    if (componentIndex > -1) {
                        collection.splice(componentIndex, 1);
                    }
                }
            });
        });
    }),
    on(setDragging, (state, { dragging }) => ({
        ...state,
        dragging
    })),
    on(setTransitionRequired, (state, { transitionRequired }) => ({
        ...state,
        transitionRequired
    })),
    on(setSkipTransform, (state, { skipTransform }) => ({
        ...state,
        skipTransform
    })),
    on(setAllowTransition, (state, { allowTransition }) => ({
        ...state,
        allowTransition
    })),
    on(navigateWithoutTransform, (state) => ({
        ...state,
        skipTransform: true
    })),
    on(setNavigationCollapsed, (state, { navigationCollapsed }) => ({
        ...state,
        navigationCollapsed
    })),
    on(setOperationCollapsed, (state, { operationCollapsed }) => ({
        ...state,
        operationCollapsed
    })),
    on(startComponentSuccess, stopComponentSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, response.type);

            if (collection) {
                const componentIndex: number = collection.findIndex((f: any) => response.component.id === f.id);
                if (componentIndex > -1) {
                    collection[componentIndex] = response.component;
                }
            }

            draftState.saving = false;
        });
    }),

    on(runOnceSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, ComponentType.Processor);

            if (collection) {
                const componentIndex: number = collection.findIndex((f: any) => response.component.id === f.id);
                if (componentIndex > -1) {
                    collection[componentIndex] = response.component;
                }
            }

            draftState.saving = false;
        });
    }),

    on(loadChildProcessGroupSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, ComponentType.ProcessGroup);

            if (collection) {
                const componentIndex: number = collection.findIndex((f: any) => response.id === f.id);
                if (componentIndex > -1) {
                    collection[componentIndex] = response;
                }
            }

            draftState.saving = false;
        });
    }),
    on(saveToFlowRegistry, stopVersionControl, (state) => ({
        ...state,
        versionSaving: true
    })),
    on(saveToFlowRegistrySuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, ComponentType.ProcessGroup);

            if (collection) {
                const componentIndex: number = collection.findIndex(
                    (f: any) => response.versionControlInformation?.groupId === f.id
                );
                if (componentIndex > -1) {
                    collection[componentIndex].revision = response.processGroupRevision;
                    collection[componentIndex].versionedFlowState = response.versionControlInformation?.state;
                }
            }

            draftState.versionSaving = false;
        });
    }),
    on(stopVersionControlSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const collection: any[] | null = getComponentCollection(draftState, ComponentType.ProcessGroup);

            if (collection) {
                const componentIndex: number = collection.findIndex((f: any) => response.processGroupId === f.id);
                if (componentIndex > -1) {
                    collection[componentIndex].revision = response.processGroupRevision;
                    collection[componentIndex].versionedFlowState = null;
                }
            }

            draftState.versionSaving = false;
        });
    }),
    on(
        changeVersionSuccess,
        pollChangeVersionSuccess,
        revertChangesSuccess,
        pollRevertChangesSuccess,
        (state, { response }) => ({
            ...state,
            changeVersionRequest: response
        })
    ),
    on(changeVersionComplete, revertChangesComplete, (state) => ({
        ...state,
        changeVersionRequest: null
    })),
    on(flowVersionBannerError, (state) => ({
        ...state,
        versionSaving: false
    }))
);

function getComponentCollection(draftState: FlowState, componentType: ComponentType): any[] | null {
    let collection: any[] | null = null;
    switch (componentType) {
        case ComponentType.Processor:
            collection = draftState.flow.processGroupFlow.flow.processors;
            break;
        case ComponentType.ProcessGroup:
            collection = draftState.flow.processGroupFlow.flow.processGroups;
            break;
        case ComponentType.RemoteProcessGroup:
            collection = draftState.flow.processGroupFlow.flow.remoteProcessGroups;
            break;
        case ComponentType.InputPort:
            collection = draftState.flow.processGroupFlow.flow.inputPorts;
            break;
        case ComponentType.OutputPort:
            collection = draftState.flow.processGroupFlow.flow.outputPorts;
            break;
        case ComponentType.Label:
            collection = draftState.flow.processGroupFlow.flow.labels;
            break;
        case ComponentType.Funnel:
            collection = draftState.flow.processGroupFlow.flow.funnels;
            break;
        case ComponentType.Connection:
            collection = draftState.flow.processGroupFlow.flow.connections;
            break;
    }
    return collection;
}
