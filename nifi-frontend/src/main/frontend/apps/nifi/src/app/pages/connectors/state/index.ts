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

import { Action, combineReducers, createFeatureSelector } from '@ngrx/store';
import { Bundle, ConnectorEntity, Revision } from '@nifi/shared';
import { connectorsListingReducer } from './connectors-listing/connectors-listing.reducer';
import { purgeConnectorFeatureKey, PurgeConnectorState } from './purge-connector';
import { purgeConnectorReducer } from './purge-connector/purge-connector.reducer';
import { connectorCanvasEntityFeatureKey, ConnectorCanvasEntityState } from './connector-canvas-entity';
import { connectorCanvasEntityReducer } from './connector-canvas-entity/connector-canvas-entity.reducer';

export const connectorsFeatureKey = 'connectors';
export const connectorsListingFeatureKey = 'connectorsListing';

export interface ConnectorsListingState {
    connectors: ConnectorEntity[];
    saving: boolean;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success' | 'error';
}

export interface SelectConnectorRequest {
    id: string;
}

export interface CreateConnectorRequest {
    revision: Revision;
    connectorType: string;
    connectorBundle: Bundle;
}

export interface CreateConnectorSuccess {
    connector: ConnectorEntity;
}

export interface DeleteConnectorRequest {
    connector: ConnectorEntity;
}

export interface DeleteConnectorSuccess {
    connector: ConnectorEntity;
}

export interface ConfigureConnectorRequest {
    id: string;
    connector: ConnectorEntity;
}

export interface ConnectorsResponse {
    connectors: ConnectorEntity[];
    currentTime: string;
}

export interface LoadConnectorsListingResponse {
    connectors: ConnectorEntity[];
    loadedTimestamp: string;
}

export interface StartConnectorRequest {
    connector: ConnectorEntity;
}

export interface StartConnectorSuccess {
    connector: ConnectorEntity;
}

export interface StopConnectorRequest {
    connector: ConnectorEntity;
}

export interface StopConnectorSuccess {
    connector: ConnectorEntity;
}

export interface RenameConnectorRequest {
    connector: ConnectorEntity;
    newName: string;
}

export interface RenameConnectorSuccess {
    connector: ConnectorEntity;
}

export interface DiscardConnectorConfigRequest {
    connector: ConnectorEntity;
}

export interface DrainConnectorRequest {
    connector: ConnectorEntity;
}

export interface CancelDrainConnectorRequest {
    connector: ConnectorEntity;
}

export interface ConnectorActionSuccess {
    connector: ConnectorEntity;
}

export interface ViewConnectorRequest {
    connectorId: string;
    processGroupId: string;
}

export interface ConnectorsState {
    [connectorsListingFeatureKey]: ConnectorsListingState;
    [purgeConnectorFeatureKey]: PurgeConnectorState;
    [connectorCanvasEntityFeatureKey]: ConnectorCanvasEntityState;
}

export function reducers(state: ConnectorsState | undefined, action: Action) {
    return combineReducers({
        [connectorsListingFeatureKey]: connectorsListingReducer,
        [purgeConnectorFeatureKey]: purgeConnectorReducer,
        [connectorCanvasEntityFeatureKey]: connectorCanvasEntityReducer
    })(state, action);
}

export const selectConnectorsState = createFeatureSelector<ConnectorsState>(connectorsFeatureKey);
