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
import { LineageQueryResponse, LineageRequest } from './index';

export const resetLineage = createAction('[Lineage] Reset Lineage');

export const submitLineageQuery = createAction('[Lineage] Submit Lineage Query', props<{ request: LineageRequest }>());

export const submitLineageQuerySuccess = createAction(
    '[Lineage] Submit Lineage Query Success',
    props<{ response: LineageQueryResponse }>()
);

export const startPollingLineageQuery = createAction('[Lineage] Start Polling Lineage Query');

export const pollLineageQuery = createAction('[Lineage] Poll Lineage Query');

export const pollLineageQuerySuccess = createAction(
    '[Lineage] Poll Lineage Query Success',
    props<{ response: LineageQueryResponse }>()
);

export const stopPollingLineageQuery = createAction('[Lineage] Stop Polling Lineage Query');

export const deleteLineageQuery = createAction('[Lineage] Delete Lineage Query');

export const deleteLineageQuerySuccess = createAction('[Lineage] Delete Lineage Query Success');

export const lineageApiError = createAction(
    '[Lineage] Load Parameter Context Listing Error',
    props<{ error: string }>()
);
