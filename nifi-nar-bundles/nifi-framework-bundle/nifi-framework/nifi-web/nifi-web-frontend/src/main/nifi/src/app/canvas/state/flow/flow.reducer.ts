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
    addSelectedComponents,
    createComponentSuccess,
    enterProcessGroup,
    enterProcessGroupComplete,
    enterProcessGroupSuccess,
    flowApiError,
    removeSelectedComponents,
    setDragging,
    setRenderRequired,
    setSelectedComponents,
    setTransitionRequired,
    updateComponentFailure,
    updateComponentSuccess
} from './flow.actions';
import { ComponentType, FlowState } from '../index';
import { produce } from 'immer';

export const initialState: FlowState = {
    id: 'root',
    flow: {
        permissions: {
            canRead: false,
            canWrite: false
        },
        processGroupFlow: {
            id: '',
            uri: '',
            parentGroupId: '',
            breadcrumb: {},
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
    selection: [],
    dragging: false,
    renderRequired: false,
    transitionRequired: false,
    error: null,
    status: 'pending'
};

export const flowReducer = createReducer(
    initialState,
    on(enterProcessGroup, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(enterProcessGroupSuccess, (state, { response }) => ({
        ...state,
        id: response.flow.processGroupFlow.id,
        flow: response.flow,
        error: null,
        status: 'success' as const
    })),
    on(enterProcessGroupComplete, (state, { response }) => ({
        ...state,
        renderRequired: true,
        selection: response.selection
    })),
    on(flowApiError, (state, { error }) => ({
        ...state,
        dragging: false,
        error: error,
        status: 'error' as const
    })),
    on(addSelectedComponents, (state, { ids }) => ({
        ...state,
        selection: [...state.selection, ...ids]
    })),
    on(setSelectedComponents, (state, { ids }) => ({
        ...state,
        selection: ids
    })),
    on(removeSelectedComponents, (state, { ids }) => ({
        ...state,
        selection: state.selection.filter((id) => !ids.includes(id))
    })),
    on(createComponentSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            let collection: any[] | null = null;
            switch (response.type) {
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

            if (collection) {
                collection.push(response.payload);
            }
        });
    }),
    on(updateComponentSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.dragging = false;

            let collection: any[] | null = null;
            switch (response.type) {
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

            if (collection) {
                const componentIndex: number = collection.findIndex((f: any) => response.id === f.id);
                if (componentIndex > -1) {
                    collection[componentIndex] = response.response;
                }
            }
        });
    }),
    on(updateComponentFailure, (state, { response }) => {
        return produce(state, (draftState) => {
            if (response.restoreOnFailure) {
                let collection: any[] | null = null;
                switch (response.type) {
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
        });
    }),
    on(setDragging, (state, { dragging }) => ({
        ...state,
        dragging: dragging
    })),
    on(setTransitionRequired, (state, { transitionRequired }) => ({
        ...state,
        transitionRequired: transitionRequired
    })),
    on(setRenderRequired, (state, { renderRequired }) => ({
        ...state,
        renderRequired: renderRequired
    }))
);
