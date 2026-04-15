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

import { createSelector } from '@ngrx/store';
import { connectorCanvasEntityFeatureKey, ConnectorCanvasEntityState } from './index';
import { selectConnectorsState } from '../index';

export const selectConnectorCanvasEntityState = createSelector(
    selectConnectorsState,
    (state) => state[connectorCanvasEntityFeatureKey]
);

export const selectConnectorCanvasEntity = createSelector(
    selectConnectorCanvasEntityState,
    (state: ConnectorCanvasEntityState) => state.connectorEntity
);

export const selectConnectorCanvasEntitySaving = createSelector(
    selectConnectorCanvasEntityState,
    (state: ConnectorCanvasEntityState) => state.saving
);

export const selectConnectorCanvasEntityError = createSelector(
    selectConnectorCanvasEntityState,
    (state: ConnectorCanvasEntityState) => state.error
);

export const selectConnectorCanvasEntityLoadingStatus = createSelector(
    selectConnectorCanvasEntityState,
    (state: ConnectorCanvasEntityState) => state.loadingStatus
);
