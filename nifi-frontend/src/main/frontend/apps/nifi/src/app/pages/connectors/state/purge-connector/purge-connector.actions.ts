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
import { PollPurgeConnectorSuccess, PurgeConnectorRequest, ShowPurgeConnectorResults } from './index';

const PURGE_CONNECTOR_PREFIX = '[Purge Connector]';

export const promptPurgeConnector = createAction(
    `${PURGE_CONNECTOR_PREFIX} Prompt Purge Connector`,
    props<{ request: PurgeConnectorRequest }>()
);

export const submitPurgeConnector = createAction(
    `${PURGE_CONNECTOR_PREFIX} Submit Purge Connector`,
    props<{ request: PurgeConnectorRequest }>()
);

export const submitPurgeConnectorSuccess = createAction(
    `${PURGE_CONNECTOR_PREFIX} Submit Purge Connector Success`,
    props<{ response: PollPurgeConnectorSuccess }>()
);

export const startPollingPurgeConnector = createAction(`${PURGE_CONNECTOR_PREFIX} Start Polling Purge Connector`);

export const pollPurgeConnector = createAction(`${PURGE_CONNECTOR_PREFIX} Poll Purge Connector`);

export const pollPurgeConnectorSuccess = createAction(
    `${PURGE_CONNECTOR_PREFIX} Poll Purge Connector Success`,
    props<{ response: PollPurgeConnectorSuccess }>()
);

export const stopPollingPurgeConnector = createAction(`${PURGE_CONNECTOR_PREFIX} Stop Polling Purge Connector`);

export const deletePurgeRequest = createAction(`${PURGE_CONNECTOR_PREFIX} Delete Purge Request`);

export const showPurgeConnectorResults = createAction(
    `${PURGE_CONNECTOR_PREFIX} Show Purge Connector Results`,
    props<{ request: ShowPurgeConnectorResults }>()
);

export const resetPurgeConnectorState = createAction(`${PURGE_CONNECTOR_PREFIX} Reset State`);

export const purgeConnectorApiError = createAction(
    `${PURGE_CONNECTOR_PREFIX} Purge Connector Api Error`,
    props<{ error: string }>()
);
