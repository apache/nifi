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
import { ConnectorsListingState } from '../index';
import {
    cancelConnectorDrain,
    cancelConnectorDrainSuccess,
    connectorsListingBannerApiError,
    createConnector,
    createConnectorSuccess,
    deleteConnector,
    deleteConnectorSuccess,
    discardConnectorConfig,
    discardConnectorConfigSuccess,
    drainConnector,
    drainConnectorSuccess,
    loadConnectorsListing,
    loadConnectorsListingError,
    loadConnectorsListingSuccess,
    renameConnector,
    renameConnectorApiError,
    renameConnectorSuccess,
    resetConnectorsListingState,
    startConnector,
    startConnectorSuccess,
    stopConnector,
    stopConnectorSuccess
} from './connectors-listing.actions';

export const initialState: ConnectorsListingState = {
    connectors: [],
    saving: false,
    loadedTimestamp: '',
    status: 'pending'
};

export const connectorsListingReducer = createReducer(
    initialState,

    on(resetConnectorsListingState, () => ({
        ...initialState
    })),

    on(loadConnectorsListing, (state) => ({
        ...state,
        status: 'loading' as const
    })),

    on(loadConnectorsListingSuccess, (state, { response }) => ({
        ...state,
        connectors: response.connectors,
        loadedTimestamp: response.loadedTimestamp,
        status: 'success' as const
    })),

    on(createConnector, (state) => ({
        ...state,
        saving: true
    })),

    on(createConnectorSuccess, (state, { response }) => ({
        ...state,
        connectors: [...state.connectors, response.connector],
        saving: false
    })),

    on(deleteConnector, (state) => ({
        ...state,
        saving: true
    })),

    on(deleteConnectorSuccess, (state, { response }) => ({
        ...state,
        connectors: state.connectors.filter((c) => c.id !== response.connector.id),
        saving: false
    })),

    on(startConnector, (state) => ({
        ...state,
        saving: true
    })),

    on(startConnectorSuccess, (state, { response }) => ({
        ...state,
        connectors: state.connectors.map((c) => (c.id === response.connector.id ? response.connector : c)),
        saving: false
    })),

    on(stopConnector, (state) => ({
        ...state,
        saving: true
    })),

    on(stopConnectorSuccess, (state, { response }) => ({
        ...state,
        connectors: state.connectors.map((c) => (c.id === response.connector.id ? response.connector : c)),
        saving: false
    })),

    on(renameConnector, (state) => ({
        ...state,
        saving: true
    })),

    on(renameConnectorSuccess, (state, { response }) => ({
        ...state,
        connectors: state.connectors.map((c) => (c.id === response.connector.id ? response.connector : c)),
        saving: false
    })),

    on(renameConnectorApiError, (state) => ({
        ...state,
        saving: false
    })),

    on(loadConnectorsListingError, (state, { status }) => ({
        ...state,
        status
    })),

    on(connectorsListingBannerApiError, (state) => ({
        ...state,
        saving: false
    })),

    on(discardConnectorConfig, (state) => ({
        ...state,
        saving: true
    })),

    on(discardConnectorConfigSuccess, (state, { response }) => ({
        ...state,
        connectors: state.connectors.map((c) => (c.id === response.connector.id ? response.connector : c)),
        saving: false
    })),

    on(drainConnector, (state) => ({
        ...state,
        saving: true
    })),

    on(drainConnectorSuccess, (state, { response }) => ({
        ...state,
        connectors: state.connectors.map((c) => (c.id === response.connector.id ? response.connector : c)),
        saving: false
    })),

    on(cancelConnectorDrain, (state) => ({
        ...state,
        saving: true
    })),

    on(cancelConnectorDrainSuccess, (state, { response }) => ({
        ...state,
        connectors: state.connectors.map((c) => (c.id === response.connector.id ? response.connector : c)),
        saving: false
    }))
);
