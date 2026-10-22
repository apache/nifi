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
import { initialConnectorCanvasState } from './index';
import * as ConnectorCanvasActions from './connector-canvas.actions';
import { ComponentType, Revision } from '@nifi/shared';

function hasCurrentOrNewerRevision(current: Revision, incoming: Revision): boolean {
    return incoming.version >= current.version;
}

function replaceIfCurrentOrNewer<E extends { id: string; revision: Revision }>(
    collection: E[],
    id: string,
    incoming: E
): E[] {
    return collection.map((entity) =>
        entity.id === id && hasCurrentOrNewerRevision(entity.revision, incoming.revision) ? incoming : entity
    );
}

function reconcileIfCurrentOrNewer<E extends { id: string; revision: Revision }>(collection: E[], incoming: E): E[] {
    const current = collection.find((entity) => entity.id === incoming.id);
    if (!current) {
        return [...collection, incoming];
    }
    return hasCurrentOrNewerRevision(current.revision, incoming.revision)
        ? collection.map((entity) => (entity.id === incoming.id ? incoming : entity))
        : collection;
}

function processComponentCollection<E extends { id: string; revision: Revision }>(
    proposedComponents: E[],
    currentComponents: E[],
    addedCache: string[],
    removedCache: string[]
): E[] {
    const currentById = new Map(currentComponents.map((component) => [component.id, component]));
    const proposedIds = new Set(proposedComponents.map((component) => component.id));
    const components = proposedComponents
        .filter((component) => !removedCache.includes(component.id))
        .map((proposedComponent) => {
            const currentComponent = currentById.get(proposedComponent.id);
            return currentComponent && !hasCurrentOrNewerRevision(currentComponent.revision, proposedComponent.revision)
                ? currentComponent
                : proposedComponent;
        });

    currentComponents.forEach((currentComponent) => {
        if (!proposedIds.has(currentComponent.id) && addedCache.includes(currentComponent.id)) {
            components.push(currentComponent);
        }
    });

    return components;
}

export const connectorCanvasReducer = createReducer(
    initialConnectorCanvasState,

    on(ConnectorCanvasActions.loadConnectorFlow, (state, { connectorId, processGroupId }) => ({
        ...state,
        connectorId,
        processGroupId,
        loadingStatus: 'loading' as const,
        error: null,
        ...(connectorId !== state.connectorId || processGroupId !== state.processGroupId
            ? {
                  labels: [],
                  funnels: [],
                  inputPorts: [],
                  outputPorts: [],
                  remoteProcessGroups: [],
                  processGroups: [],
                  processors: [],
                  connections: [],
                  addedCache: [],
                  removedCache: [],
                  hasLoadedFlow: false,
                  parameterContext: null
              }
            : {})
    })),

    on(ConnectorCanvasActions.resetConnectorCanvasState, () => ({
        ...initialConnectorCanvasState
    })),

    on(
        ConnectorCanvasActions.loadConnectorFlowSuccess,
        (
            state,
            {
                connectorId,
                processGroupId,
                parentProcessGroupId,
                breadcrumb,
                labels,
                funnels,
                inputPorts,
                outputPorts,
                remoteProcessGroups,
                processGroups,
                processors,
                connections
            }
        ) => {
            const snapshotIds = new Set(
                [
                    ...labels,
                    ...funnels,
                    ...inputPorts,
                    ...outputPorts,
                    ...remoteProcessGroups,
                    ...processGroups,
                    ...processors,
                    ...connections
                ].map((entity) => entity.id)
            );
            const snapshotIdsOrConnectionEndpoints = new Set([
                ...snapshotIds,
                ...connections.flatMap((connection) => [connection.sourceId, connection.destinationId])
            ]);
            const reconciledConnections = connections.filter(
                (connection) =>
                    !state.removedCache.includes(connection.sourceId) &&
                    !state.removedCache.includes(connection.destinationId)
            );
            return {
                ...state,
                connectorId,
                processGroupId,
                parentProcessGroupId,
                breadcrumb,
                labels: processComponentCollection(labels, state.labels, state.addedCache, state.removedCache),
                funnels: processComponentCollection(funnels, state.funnels, state.addedCache, state.removedCache),
                inputPorts: processComponentCollection(
                    inputPorts,
                    state.inputPorts,
                    state.addedCache,
                    state.removedCache
                ),
                outputPorts: processComponentCollection(
                    outputPorts,
                    state.outputPorts,
                    state.addedCache,
                    state.removedCache
                ),
                remoteProcessGroups: processComponentCollection(
                    remoteProcessGroups,
                    state.remoteProcessGroups,
                    state.addedCache,
                    state.removedCache
                ),
                processGroups: processComponentCollection(
                    processGroups,
                    state.processGroups,
                    state.addedCache,
                    state.removedCache
                ),
                processors: processComponentCollection(
                    processors,
                    state.processors,
                    state.addedCache,
                    state.removedCache
                ),
                connections: processComponentCollection(
                    reconciledConnections,
                    state.connections,
                    state.addedCache,
                    state.removedCache
                ),
                addedCache: state.addedCache.filter((id) => !snapshotIds.has(id)),
                removedCache: state.removedCache.filter((id) => snapshotIdsOrConnectionEndpoints.has(id)),
                hasLoadedFlow: true,
                loadingStatus: 'success' as const,
                error: null
            };
        }
    ),

    on(ConnectorCanvasActions.loadConnectorFlowFailure, (state, { errorContext, isRefresh }) => ({
        ...state,
        loadingStatus: isRefresh ? ('refresh-error' as const) : ('error' as const),
        error: errorContext.errors.join(', ')
    })),

    on(ConnectorCanvasActions.navigateWithoutTransform, (state) => ({
        ...state,
        skipTransform: true
    })),

    on(ConnectorCanvasActions.setSkipTransform, (state, { skipTransform }) => ({
        ...state,
        skipTransform
    })),

    on(
        ConnectorCanvasActions.updateComponentPositionsBulk,
        ConnectorCanvasActions.updateConnectionBendPoints,
        ConnectorCanvasActions.updateConnectionLabelIndex,
        ConnectorCanvasActions.updateConnectionDestination,
        ConnectorCanvasActions.saveEditedConnection,
        ConnectorCanvasActions.resizeLabel,
        ConnectorCanvasActions.submitCreateConnection,
        (state) => ({ ...state, saving: true })
    ),

    on(ConnectorCanvasActions.moveCanvasComponents, (state, { components }) => {
        const movedIds = new Set(components.map((component) => component.id));
        const keep = <E extends { id: string }>(entity: E): boolean => !movedIds.has(entity.id);
        return {
            ...state,
            saving: true,
            processors: state.processors.filter(keep),
            processGroups: state.processGroups.filter(keep),
            remoteProcessGroups: state.remoteProcessGroups.filter(keep),
            funnels: state.funnels.filter(keep),
            labels: state.labels.filter(keep),
            inputPorts: state.inputPorts.filter(keep),
            outputPorts: state.outputPorts.filter(keep),
            connections: state.connections.filter(
                (connection) =>
                    keep(connection) && !movedIds.has(connection.sourceId) && !movedIds.has(connection.destinationId)
            ),
            removedCache: [...new Set([...state.removedCache, ...movedIds])]
        };
    }),

    on(
        ConnectorCanvasActions.updateComponentPositionsBulkComplete,
        ConnectorCanvasActions.moveCanvasComponentsSuccess,
        (state) => ({
            ...state,
            saving: false
        })
    ),

    on(ConnectorCanvasActions.moveCanvasComponentsFailure, (state, { components }) => {
        const movedIds = new Set(components.map((component) => component.id));
        return {
            ...state,
            saving: false,
            removedCache: state.removedCache.filter((id) => !movedIds.has(id))
        };
    }),

    on(ConnectorCanvasActions.updateComponentPositionSuccess, (state, { componentId, componentType, component }) => {
        switch (componentType) {
            case ComponentType.Processor:
                return {
                    ...state,
                    processors: replaceIfCurrentOrNewer(
                        state.processors,
                        componentId,
                        component as (typeof state.processors)[number]
                    )
                };
            case ComponentType.InputPort:
                return {
                    ...state,
                    inputPorts: replaceIfCurrentOrNewer(
                        state.inputPorts,
                        componentId,
                        component as (typeof state.inputPorts)[number]
                    )
                };
            case ComponentType.OutputPort:
                return {
                    ...state,
                    outputPorts: replaceIfCurrentOrNewer(
                        state.outputPorts,
                        componentId,
                        component as (typeof state.outputPorts)[number]
                    )
                };
            case ComponentType.ProcessGroup:
                return {
                    ...state,
                    processGroups: replaceIfCurrentOrNewer(
                        state.processGroups,
                        componentId,
                        component as (typeof state.processGroups)[number]
                    )
                };
            case ComponentType.RemoteProcessGroup:
                return {
                    ...state,
                    remoteProcessGroups: replaceIfCurrentOrNewer(
                        state.remoteProcessGroups,
                        componentId,
                        component as (typeof state.remoteProcessGroups)[number]
                    )
                };
            case ComponentType.Funnel:
                return {
                    ...state,
                    funnels: replaceIfCurrentOrNewer(
                        state.funnels,
                        componentId,
                        component as (typeof state.funnels)[number]
                    )
                };
            case ComponentType.Label:
                return {
                    ...state,
                    labels: replaceIfCurrentOrNewer(
                        state.labels,
                        componentId,
                        component as (typeof state.labels)[number]
                    )
                };
            default:
                return state;
        }
    }),

    on(
        ConnectorCanvasActions.updateConnectionBendPointsSuccess,
        ConnectorCanvasActions.updateConnectionLabelIndexSuccess,
        ConnectorCanvasActions.updateConnectionDestinationSuccess,
        (state, { connectionId, connection }) => ({
            ...state,
            saving: false,
            connections: replaceIfCurrentOrNewer(state.connections, connectionId, connection)
        })
    ),

    on(
        ConnectorCanvasActions.updateConnectionBendPointsFailure,
        ConnectorCanvasActions.updateConnectionLabelIndexFailure,
        ConnectorCanvasActions.updateConnectionDestinationFailure,
        (state) => ({ ...state, saving: false })
    ),

    on(ConnectorCanvasActions.resizeLabelSuccess, (state, { labelId, label }) => ({
        ...state,
        saving: false,
        labels: replaceIfCurrentOrNewer(state.labels, labelId, label)
    })),

    on(ConnectorCanvasActions.resizeLabelFailure, (state) => ({ ...state, saving: false })),

    on(ConnectorCanvasActions.createConnectionSuccess, (state, { connection }) => ({
        ...state,
        saving: false,
        connections: reconcileIfCurrentOrNewer(state.connections, connection),
        addedCache: [...new Set([...state.addedCache, connection.id])]
    })),

    on(ConnectorCanvasActions.createConnectionFailure, (state) => ({ ...state, saving: false })),

    on(ConnectorCanvasActions.loadConnectorParameterContextSuccess, (state, { parameterContext }) => ({
        ...state,
        parameterContext
    })),

    on(ConnectorCanvasActions.loadConnectorParameterContextFailure, (state) => ({
        ...state,
        parameterContext: null
    }))
);
