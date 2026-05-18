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

import { createAction, props } from '@ngrx/store';
import { ConnectorEntity } from '@nifi/shared';

const PREFIX = '[Connector Canvas Entity]';

export const loadConnectorEntity = createAction(`${PREFIX} Load Connector Entity`, props<{ connectorId: string }>());

export const loadConnectorEntitySuccess = createAction(
    `${PREFIX} Load Connector Entity Success`,
    props<{ connectorEntity: ConnectorEntity }>()
);

export const loadConnectorEntityFailure = createAction(
    `${PREFIX} Load Connector Entity Failure`,
    props<{ error: string }>()
);

/**
 * Prompt user to confirm draining FlowFiles.
 */
export const promptDrainConnector = createAction(
    `${PREFIX} Prompt Drain Connector`,
    props<{ connector: ConnectorEntity }>()
);

/**
 * Initiate draining of FlowFiles for a connector.
 */
export const drainConnector = createAction(`${PREFIX} Drain Connector`, props<{ connector: ConnectorEntity }>());

export const drainConnectorSuccess = createAction(
    `${PREFIX} Drain Connector Success`,
    props<{ connector: ConnectorEntity }>()
);

/**
 * Cancel an ongoing drain operation (no confirmation needed).
 */
export const cancelConnectorDrain = createAction(
    `${PREFIX} Cancel Connector Drain`,
    props<{ connector: ConnectorEntity }>()
);

export const cancelConnectorDrainSuccess = createAction(
    `${PREFIX} Cancel Connector Drain Success`,
    props<{ connector: ConnectorEntity }>()
);

/**
 * Start a connector (transition to RUNNING).
 */
export const startConnector = createAction(`${PREFIX} Start Connector`, props<{ connector: ConnectorEntity }>());

export const startConnectorSuccess = createAction(
    `${PREFIX} Start Connector Success`,
    props<{ connector: ConnectorEntity }>()
);

/**
 * Stop a connector (transition to STOPPED).
 */
export const stopConnector = createAction(`${PREFIX} Stop Connector`, props<{ connector: ConnectorEntity }>());

export const stopConnectorSuccess = createAction(
    `${PREFIX} Stop Connector Success`,
    props<{ connector: ConnectorEntity }>()
);

/**
 * API error during any connector action (drain, cancel drain, start, stop).
 */
export const connectorActionApiError = createAction(`${PREFIX} Connector Action Api Error`, props<{ error: string }>());

export const resetConnectorCanvasEntityState = createAction(`${PREFIX} Reset State`);
