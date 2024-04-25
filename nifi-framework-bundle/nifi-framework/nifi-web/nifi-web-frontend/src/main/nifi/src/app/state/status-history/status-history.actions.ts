/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { createAction, props } from '@ngrx/store';
import { NodeStatusHistoryRequest, StatusHistoryRequest, StatusHistoryResponse } from './index';

const STATUS_HISTORY_PREFIX = '[Status History]';

export const reloadStatusHistory = createAction(
    `${STATUS_HISTORY_PREFIX} Reload Status History`,
    props<{ request: StatusHistoryRequest }>()
);

export const getStatusHistoryAndOpenDialog = createAction(
    `${STATUS_HISTORY_PREFIX} Get Status History and Open Dialog`,
    props<{ request: StatusHistoryRequest }>()
);

export const getNodeStatusHistoryAndOpenDialog = createAction(
    `${STATUS_HISTORY_PREFIX} Get Node Status History and Open Dialog`,
    props<{ request: NodeStatusHistoryRequest }>()
);

export const reloadStatusHistorySuccess = createAction(
    `${STATUS_HISTORY_PREFIX} Reload Status History Success`,
    props<{ response: StatusHistoryResponse }>()
);

export const loadStatusHistorySuccess = createAction(
    `${STATUS_HISTORY_PREFIX} Load Status History Success`,
    props<{ request: StatusHistoryRequest | NodeStatusHistoryRequest; response: StatusHistoryResponse }>()
);

export const openStatusHistoryDialog = createAction(
    `${STATUS_HISTORY_PREFIX} Open Status History Dialog`,
    props<{ request: StatusHistoryRequest | NodeStatusHistoryRequest }>()
);

export const statusHistoryBannerError = createAction(
    `${STATUS_HISTORY_PREFIX} Status History Banner Error`,
    props<{ error: string }>()
);

export const clearStatusHistory = createAction(`${STATUS_HISTORY_PREFIX} Clear Status History`);

export const viewStatusHistoryComplete = createAction(
    `${STATUS_HISTORY_PREFIX} View Status History Complete`,
    props<{ request: StatusHistoryRequest }>()
);

export const viewNodeStatusHistoryComplete = createAction(
    `${STATUS_HISTORY_PREFIX} View Node Status History Complete`,
    props<{ request: NodeStatusHistoryRequest }>()
);
