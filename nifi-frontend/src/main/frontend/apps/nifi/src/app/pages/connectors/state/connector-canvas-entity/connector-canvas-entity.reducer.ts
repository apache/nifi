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
import { ConnectorEntity } from '@nifi/shared';
import { initialConnectorCanvasEntityState } from './index';
import {
    loadConnectorEntity,
    loadConnectorEntitySuccess,
    loadConnectorEntityFailure,
    drainConnector,
    drainConnectorSuccess,
    cancelConnectorDrain,
    cancelConnectorDrainSuccess,
    startConnector,
    startConnectorSuccess,
    stopConnector,
    stopConnectorSuccess,
    enterTroubleshooting,
    enterTroubleshootingSuccess,
    endTroubleshooting,
    endTroubleshootingSuccess,
    connectorActionApiError,
    resetConnectorCanvasEntityState
} from './connector-canvas-entity.actions';

function reconcileConnectorEntity(current: ConnectorEntity | null, incoming: ConnectorEntity): ConnectorEntity {
    return current?.id === incoming.id && incoming.revision.version < current.revision.version ? current : incoming;
}

export const connectorCanvasEntityReducer = createReducer(
    initialConnectorCanvasEntityState,

    on(loadConnectorEntity, (state) => ({
        ...state,
        loadingStatus: 'loading' as const,
        error: null
    })),

    on(loadConnectorEntitySuccess, (state, { connectorEntity }) => ({
        ...state,
        connectorEntity: reconcileConnectorEntity(state.connectorEntity, connectorEntity),
        loadingStatus: 'success' as const,
        error: null
    })),

    on(loadConnectorEntityFailure, (state, { error }) => ({
        ...state,
        loadingStatus: 'error' as const,
        error
    })),

    on(
        drainConnector,
        cancelConnectorDrain,
        startConnector,
        stopConnector,
        enterTroubleshooting,
        endTroubleshooting,
        (state) => ({
            ...state,
            saving: true,
            error: null
        })
    ),

    on(
        drainConnectorSuccess,
        cancelConnectorDrainSuccess,
        startConnectorSuccess,
        stopConnectorSuccess,
        enterTroubleshootingSuccess,
        endTroubleshootingSuccess,
        (state, { connector }) => ({
            ...state,
            connectorEntity: connector,
            saving: false,
            error: null
        })
    ),

    on(connectorActionApiError, (state, { error }) => ({
        ...state,
        saving: false,
        error
    })),

    on(resetConnectorCanvasEntityState, () => ({
        ...initialConnectorCanvasEntityState
    }))
);
