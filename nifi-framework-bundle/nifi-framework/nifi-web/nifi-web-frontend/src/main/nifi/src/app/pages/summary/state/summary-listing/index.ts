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

import {
    ConnectionStatusSnapshotEntity,
    PortStatusSnapshotEntity,
    ProcessGroupStatusEntity,
    ProcessGroupStatusSnapshotEntity,
    ProcessorStatusSnapshotEntity,
    RemoteProcessGroupStatusSnapshotEntity
} from '../index';
import { NodeSearchResult } from '../../../../state/cluster-summary';

export const summaryListingFeatureKey = 'summary-listing';

export interface SummaryListingResponse {
    status: ProcessGroupStatusEntity;
}

export interface SelectProcessorStatusRequest {
    id: string;
}

export interface SelectProcessGroupStatusRequest {
    id: string;
}

export interface SelectPortStatusRequest {
    id: string;
}

export interface SelectConnectionStatusRequest {
    id: string;
}

export interface SelectRemoteProcessGroupStatusRequest {
    id: string;
}

export interface LoadSummaryRequest {
    recursive?: boolean;
    clusterNodeId?: string;
}

export interface SummaryListingState {
    processGroupStatus: ProcessGroupStatusEntity | null;
    processorStatusSnapshots: ProcessorStatusSnapshotEntity[];
    processGroupStatusSnapshots: ProcessGroupStatusSnapshotEntity[];
    inputPortStatusSnapshots: PortStatusSnapshotEntity[];
    outputPortStatusSnapshots: PortStatusSnapshotEntity[];
    connectionStatusSnapshots: ConnectionStatusSnapshotEntity[];
    remoteProcessGroupStatusSnapshots: RemoteProcessGroupStatusSnapshotEntity[];
    selectedClusterNode: NodeSearchResult | null;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success';
}
