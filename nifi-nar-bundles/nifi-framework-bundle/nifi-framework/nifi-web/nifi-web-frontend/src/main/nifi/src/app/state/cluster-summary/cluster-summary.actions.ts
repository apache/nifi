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
import { ClusterSearchRequest, ClusterSearchResults, LoadClusterSummaryResponse } from './index';

const CLUSTER_SUMMARY_STATE_PREFIX = '[Cluster Summary State]';

export const startClusterSummaryPolling = createAction(`${CLUSTER_SUMMARY_STATE_PREFIX} Start Cluster Summary Polling`);

export const stopClusterSummaryPolling = createAction(`${CLUSTER_SUMMARY_STATE_PREFIX} Stop Cluster Summary Polling`);

export const loadClusterSummary = createAction(`${CLUSTER_SUMMARY_STATE_PREFIX} Load Cluster Summary`);

export const loadClusterSummarySuccess = createAction(
    `${CLUSTER_SUMMARY_STATE_PREFIX} Load Cluster Summary Success`,
    props<{ response: LoadClusterSummaryResponse }>()
);

export const acknowledgeClusterConnectionChange = createAction(
    `${CLUSTER_SUMMARY_STATE_PREFIX} Acknowledge Cluster Connection Change`,
    props<{ connectedToCluster: boolean }>()
);

export const setDisconnectionAcknowledged = createAction(
    `${CLUSTER_SUMMARY_STATE_PREFIX} Set Disconnection Acknowledged`,
    props<{ disconnectionAcknowledged: boolean }>()
);

export const clusterSummaryApiError = createAction(
    `${CLUSTER_SUMMARY_STATE_PREFIX} Cluster Summary Api Error`,
    props<{ error: string }>()
);

export const clearClusterSummaryApiError = createAction(`${CLUSTER_SUMMARY_STATE_PREFIX} Clear About Api Error`);

export const searchCluster = createAction(
    `${CLUSTER_SUMMARY_STATE_PREFIX} Search Cluster`,
    props<{ request: ClusterSearchRequest }>()
);

export const searchClusterSuccess = createAction(
    `${CLUSTER_SUMMARY_STATE_PREFIX} Search Cluster Success`,
    props<{ response: ClusterSearchResults }>()
);
