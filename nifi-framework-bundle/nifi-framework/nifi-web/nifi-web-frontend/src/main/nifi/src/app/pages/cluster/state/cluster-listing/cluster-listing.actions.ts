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
import { ClusterListingEntity, ClusterNode, ClusterNodeEntity, SelectClusterNodeRequest } from './index';
import { HttpErrorResponse } from '@angular/common/http';

const CLUSTER_LISTING_PREFIX = '[Cluster Listing]';

export const loadClusterListing = createAction(`${CLUSTER_LISTING_PREFIX} Load Cluster Listing`);

export const loadClusterListingSuccess = createAction(
    `${CLUSTER_LISTING_PREFIX} Load Cluster Listing Success`,
    props<{ response: ClusterListingEntity }>()
);

export const confirmAndDisconnectNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Confirm And Disconnect Node`,
    props<{ request: ClusterNode }>()
);

export const disconnectNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Disconnect Node`,
    props<{ request: ClusterNode }>()
);

export const confirmAndConnectNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Confirm And Connect Node`,
    props<{ request: ClusterNode }>()
);

export const connectNode = createAction(`${CLUSTER_LISTING_PREFIX} Connect Node`, props<{ request: ClusterNode }>());

export const confirmAndOffloadNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Confirm And Offload Node`,
    props<{ request: ClusterNode }>()
);

export const offloadNode = createAction(`${CLUSTER_LISTING_PREFIX} Offload Node`, props<{ request: ClusterNode }>());

export const updateNodeSuccess = createAction(
    `${CLUSTER_LISTING_PREFIX} Update Node Success`,
    props<{ response: ClusterNodeEntity }>()
);

export const confirmAndRemoveNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Confirm And Remove Node`,
    props<{ request: ClusterNode }>()
);

export const removeNode = createAction(`${CLUSTER_LISTING_PREFIX} Remove Node`, props<{ request: ClusterNode }>());

export const removeNodeSuccess = createAction(
    `${CLUSTER_LISTING_PREFIX} Remove Node Success`,
    props<{ response: ClusterNode }>()
);

export const clusterNodeSnackbarError = createAction(
    `${CLUSTER_LISTING_PREFIX} Cluster Node Snackbar Error`,
    props<{ errorResponse: HttpErrorResponse }>()
);

export const selectClusterNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Select Cluster Node`,
    props<{ request: SelectClusterNodeRequest }>()
);
export const selectSystemNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Select System Node`,
    props<{ request: SelectClusterNodeRequest }>()
);
export const selectJvmNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Select JVM Node`,
    props<{ request: SelectClusterNodeRequest }>()
);
export const selectFlowFileStorageNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Select FlowFile Storage Node`,
    props<{ request: SelectClusterNodeRequest }>()
);
export const selectContentStorageNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Select Content Storage Node`,
    props<{ request: SelectClusterNodeRequest }>()
);
export const selectProvenanceStorageNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Select Provenance Storage Node`,
    props<{ request: SelectClusterNodeRequest }>()
);
export const selectVersionNode = createAction(
    `${CLUSTER_LISTING_PREFIX} Select Version Node`,
    props<{ request: SelectClusterNodeRequest }>()
);

export const clearClusterNodeSelection = createAction(`${CLUSTER_LISTING_PREFIX} Clear Cluster Node Selection`);
export const clearSystemNodeSelection = createAction(`${CLUSTER_LISTING_PREFIX} Clear System Node Selection`);
export const clearJvmNodeSelection = createAction(`${CLUSTER_LISTING_PREFIX} Clear JVM Node Selection`);
export const clearFlowFileStorageNodeSelection = createAction(
    `${CLUSTER_LISTING_PREFIX} Clear FlowFile Storage Node Selection`
);
export const clearContentStorageNodeSelection = createAction(
    `${CLUSTER_LISTING_PREFIX} Clear Content Storage Node Selection`
);
export const clearProvenanceStorageNodeSelection = createAction(
    `${CLUSTER_LISTING_PREFIX} Clear Provenance Storage Node Selection`
);
export const clearVersionsNodeSelection = createAction(`${CLUSTER_LISTING_PREFIX} Clear Versions Node Selection`);

export const showClusterNodeDetails = createAction(
    `${CLUSTER_LISTING_PREFIX} Show Cluster Node Details`,
    props<{ request: ClusterNode }>()
);

export const navigateToClusterNodeListing = createAction(`${CLUSTER_LISTING_PREFIX} Navigate to Cluster Node Listing`);
export const navigateHome = createAction(`${CLUSTER_LISTING_PREFIX} Navigate to Home`);

export const resetClusterState = createAction(`${CLUSTER_LISTING_PREFIX} Reset Cluster State`);
