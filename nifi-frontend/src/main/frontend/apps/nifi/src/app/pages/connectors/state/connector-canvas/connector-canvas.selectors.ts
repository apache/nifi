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

import { createFeatureSelector, createSelector } from '@ngrx/store';
import { ConnectionEntity, Position, selectRouteParams } from '@nifi/shared';
import { ConnectorCanvasState, connectorCanvasFeatureKey } from './index';

export const selectConnectorCanvasState = createFeatureSelector<ConnectorCanvasState>(connectorCanvasFeatureKey);

// Route selectors - pattern: /connectors/:id/canvas/:processGroupId
export const selectConnectorIdFromRoute = createSelector(selectRouteParams, (params) => {
    return params?.['id'] ?? null;
});

export const selectProcessGroupIdFromRoute = createSelector(selectRouteParams, (params) => {
    return params?.['processGroupId'] ?? null;
});

// Flow data selectors
export const selectLabels = createSelector(selectConnectorCanvasState, (state) => state.labels);

export const selectProcessors = createSelector(selectConnectorCanvasState, (state) => state.processors);

export const selectFunnels = createSelector(selectConnectorCanvasState, (state) => state.funnels);

export const selectInputPorts = createSelector(selectConnectorCanvasState, (state) => state.inputPorts);

export const selectOutputPorts = createSelector(selectConnectorCanvasState, (state) => state.outputPorts);

export const selectAllPorts = createSelector(selectInputPorts, selectOutputPorts, (inputPorts, outputPorts) => [
    ...inputPorts,
    ...outputPorts
]);

export const selectRemoteProcessGroups = createSelector(
    selectConnectorCanvasState,
    (state) => state.remoteProcessGroups
);

export const selectProcessGroups = createSelector(selectConnectorCanvasState, (state) => state.processGroups);

export const selectConnections = createSelector(selectConnectorCanvasState, (state) =>
    // Deep copy connections to prevent state mutation
    state.connections.map((conn: ConnectionEntity) => ({
        ...conn,
        bends: conn.bends ? [...conn.bends.map((bend: Position) => ({ ...bend }))] : []
    }))
);

export const selectConnectorId = createSelector(selectConnectorCanvasState, (state) => state.connectorId);

export const selectProcessGroupId = createSelector(selectConnectorCanvasState, (state) => state.processGroupId);

export const selectParentProcessGroupId = createSelector(
    selectConnectorCanvasState,
    (state) => state.parentProcessGroupId
);

export const selectLoadingStatus = createSelector(selectConnectorCanvasState, (state) => state.loadingStatus);

export const selectError = createSelector(selectConnectorCanvasState, (state) => state.error);

export const selectRegistryClients = createSelector(selectConnectorCanvasState, (state) => state.registryClients);

export const selectBreadcrumbs = createSelector(selectConnectorCanvasState, (state) => state.breadcrumb);

export const selectSkipTransform = createSelector(selectConnectorCanvasState, (state) => state.skipTransform);

export const selectConnectorParameterContext = createSelector(
    selectConnectorCanvasState,
    (state) => state.parameterContext
);

// Entity-by-id factory selectors for provenance eligibility checks
export const selectProcessor = (id: string) =>
    createSelector(selectProcessors, (processors) => processors.find((processor) => processor.id === id));

export const selectInputPort = (id: string) =>
    createSelector(selectInputPorts, (ports) => ports.find((port) => port.id === id));

export const selectOutputPort = (id: string) =>
    createSelector(selectOutputPorts, (ports) => ports.find((port) => port.id === id));

export const selectConnection = (id: string) =>
    createSelector(selectConnections, (connections) => connections.find((connection) => connection.id === id));

export const selectRemoteProcessGroup = (id: string) =>
    createSelector(selectRemoteProcessGroups, (remoteProcessGroups) =>
        remoteProcessGroups.find((remoteProcessGroup) => remoteProcessGroup.id === id)
    );

export const selectProcessGroup = (id: string) =>
    createSelector(selectProcessGroups, (processGroups) =>
        processGroups.find((processGroup) => processGroup.id === id)
    );

export const selectFunnel = (id: string) =>
    createSelector(selectFunnels, (funnels) => funnels.find((funnel) => funnel.id === id));
