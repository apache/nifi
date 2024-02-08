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
import {
    ActionEntity,
    HistoryEntity,
    HistoryQueryRequest,
    PurgeHistoryRequest,
    SelectFlowConfigurationHistoryRequest
} from './index';
import { HttpErrorResponse } from '@angular/common/http';

const HISTORY_PREFIX = '[Flow Configuration History Listing]';

export const loadHistory = createAction(`${HISTORY_PREFIX} Load Listing`, props<{ request: HistoryQueryRequest }>());

export const loadHistorySuccess = createAction(
    `${HISTORY_PREFIX} Load Listing Success`,
    props<{ response: HistoryEntity }>()
);

export const resetHistoryState = createAction(`${HISTORY_PREFIX} Reset History State`);

export const clearHistorySelection = createAction(`${HISTORY_PREFIX} Clear Selection`);

export const selectHistoryItem = createAction(
    `${HISTORY_PREFIX} Select History Item`,
    props<{ request: SelectFlowConfigurationHistoryRequest }>()
);

export const flowConfigurationHistorySnackbarError = createAction(
    `${HISTORY_PREFIX} Flow Configuration History Snackbar Error`,
    props<{ errorResponse: HttpErrorResponse }>()
);

export const openMoreDetailsDialog = createAction(
    `${HISTORY_PREFIX} Open More Details Dialog`,
    props<{ request: ActionEntity }>()
);

export const openPurgeHistoryDialog = createAction(`${HISTORY_PREFIX} Open Purge History Dialog`);

export const purgeHistory = createAction(`${HISTORY_PREFIX} Purge History`, props<{ request: PurgeHistoryRequest }>());

export const purgeHistorySuccess = createAction(`${HISTORY_PREFIX} Purge History Success`);
