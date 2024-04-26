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
import {
    GoToProvenanceEventSourceRequest,
    OpenSearchRequest,
    ProvenanceEventRequest,
    ProvenanceOptionsResponse,
    ProvenanceQueryResponse,
    ProvenanceRequest
} from './index';

export const resetProvenanceState = createAction('[Provenance Event Listing] Reset Provenance State');

export const loadProvenanceOptions = createAction('[Provenance Event Listing] Load Provenance Options');

export const loadProvenanceOptionsSuccess = createAction(
    '[Provenance Event Listing] Load Provenance Options Success',
    props<{ response: ProvenanceOptionsResponse }>()
);

export const submitProvenanceQuery = createAction(
    '[Provenance Event Listing] Submit Provenance Query',
    props<{ request: ProvenanceRequest }>()
);

export const resubmitProvenanceQuery = createAction(
    '[Provenance Event Listing] Resubmit Provenance Query',
    props<{ request: ProvenanceRequest }>()
);

export const submitProvenanceQuerySuccess = createAction(
    '[Provenance Event Listing] Submit Provenance Query Success',
    props<{ response: ProvenanceQueryResponse }>()
);

export const startPollingProvenanceQuery = createAction('[Provenance Event Listing] Start Polling Provenance Query');

export const pollProvenanceQuery = createAction('[Provenance Event Listing] Poll Provenance Query');

export const pollProvenanceQuerySuccess = createAction(
    '[Provenance Event Listing] Poll Provenance Query Success',
    props<{ response: ProvenanceQueryResponse }>()
);

export const stopPollingProvenanceQuery = createAction('[Provenance Event Listing] Stop Polling Provenance Query');

export const deleteProvenanceQuery = createAction('[Provenance Event Listing] Delete Provenance Query');

export const deleteProvenanceQuerySuccess = createAction('[Provenance Event Listing] Delete Provenance Query Success');

export const provenanceApiError = createAction(
    '[Provenance Event Listing] Provenance Api Error',
    props<{ error: string }>()
);

export const openProvenanceEventDialog = createAction(
    '[Provenance Event Listing] Open Provenance Event Dialog',
    props<{ request: ProvenanceEventRequest }>()
);

export const goToProvenanceEventSource = createAction(
    '[Provenance Event Listing] Go To Provenance Event Source',
    props<{ request: GoToProvenanceEventSourceRequest }>()
);

export const loadClusterNodesAndOpenSearchDialog = createAction(
    '[Provenance Event Listing] Load Cluster Nodes And Open Search Dialog'
);

export const openSearchDialog = createAction(
    '[Provenance Event Listing] Open Search Dialog',
    props<{ request: OpenSearchRequest }>()
);

export const saveProvenanceRequest = createAction(
    '[Provenance Event Listing] Save Provenance Request',
    props<{ request: ProvenanceRequest }>()
);

export const clearProvenanceRequest = createAction('[Provenance Event Listing] Clear Provenance Request');

export const showOkDialog = createAction(
    '[Provenance Event Listing] Show Ok Dialog',
    props<{ title: string; message: string }>()
);
