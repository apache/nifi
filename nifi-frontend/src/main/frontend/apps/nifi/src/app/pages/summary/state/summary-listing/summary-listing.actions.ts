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
    SelectConnectionStatusRequest,
    SelectPortStatusRequest,
    SelectProcessGroupStatusRequest,
    SelectProcessorStatusRequest,
    SelectRemoteProcessGroupStatusRequest,
    SummaryListingResponse
} from './index';
import { NodeSearchResult } from '../../../../state/cluster-summary';

const SUMMARY_LISTING_PREFIX = '[Summary Listing]';

export const loadSummaryListing = createAction(
    `${SUMMARY_LISTING_PREFIX} Load Summary Listing`,
    props<{ recursive: boolean }>()
);

export const loadSummaryListingSuccess = createAction(
    `${SUMMARY_LISTING_PREFIX} Load Summary Listing Success`,
    props<{ response: SummaryListingResponse }>()
);

export const selectProcessorStatus = createAction(
    `${SUMMARY_LISTING_PREFIX} Select Processor Status`,
    props<{ request: SelectProcessorStatusRequest }>()
);

export const clearProcessorStatusSelection = createAction(`${SUMMARY_LISTING_PREFIX} Clear Processor Status Selection`);

export const selectProcessGroupStatus = createAction(
    `${SUMMARY_LISTING_PREFIX} Select Process Group Status`,
    props<{ request: SelectProcessGroupStatusRequest }>()
);

export const clearProcessGroupStatusSelection = createAction(
    `${SUMMARY_LISTING_PREFIX} Clear Process Group Status Selection`
);

export const selectInputPortStatus = createAction(
    `${SUMMARY_LISTING_PREFIX} Select Input Port Status`,
    props<{ request: SelectPortStatusRequest }>()
);

export const clearInputPortStatusSelection = createAction(
    `${SUMMARY_LISTING_PREFIX} Clear Input Port Status Selection`
);

export const selectOutputPortStatus = createAction(
    `${SUMMARY_LISTING_PREFIX} Select Output Port Status`,
    props<{ request: SelectPortStatusRequest }>()
);

export const clearOutputPortStatusSelection = createAction(
    `${SUMMARY_LISTING_PREFIX} Clear Output Port Status Selection`
);

export const selectConnectionStatus = createAction(
    `${SUMMARY_LISTING_PREFIX} Select Connection Status`,
    props<{ request: SelectConnectionStatusRequest }>()
);

export const clearConnectionStatusSelection = createAction(
    `${SUMMARY_LISTING_PREFIX} Clear Connection Status Selection`
);

export const selectRemoteProcessGroupStatus = createAction(
    `${SUMMARY_LISTING_PREFIX} Select Remote Process Group Status`,
    props<{ request: SelectRemoteProcessGroupStatusRequest }>()
);

export const clearRemoteProcessGroupStatusSelection = createAction(
    `${SUMMARY_LISTING_PREFIX} Clear Remote Process Group Status Selection`
);

export const navigateToViewProcessorStatusHistory = createAction(
    `${SUMMARY_LISTING_PREFIX} Navigate To Processor Status History`,
    props<{ id: string }>()
);

export const navigateToViewProcessGroupStatusHistory = createAction(
    `${SUMMARY_LISTING_PREFIX} Navigate To Process Group Status History`,
    props<{ id: string }>()
);

export const navigateToViewConnectionStatusHistory = createAction(
    `${SUMMARY_LISTING_PREFIX} Navigate To Connection Status History`,
    props<{ id: string }>()
);

export const navigateToViewRemoteProcessGroupStatusHistory = createAction(
    `${SUMMARY_LISTING_PREFIX} Navigate To Remote Process Group Status History`,
    props<{ id: string }>()
);

export const resetSummaryState = createAction(`${SUMMARY_LISTING_PREFIX} Reset Summary State`);

export const selectClusterNode = createAction(
    `${SUMMARY_LISTING_PREFIX} Cluster Node Selected`,
    props<{ clusterNode: NodeSearchResult }>()
);
