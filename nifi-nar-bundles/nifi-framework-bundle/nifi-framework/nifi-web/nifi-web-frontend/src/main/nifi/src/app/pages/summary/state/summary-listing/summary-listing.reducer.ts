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

import { createReducer, on } from '@ngrx/store';
import {
    ConnectionStatusSnapshotEntity,
    PortStatusSnapshotEntity,
    ProcessGroupStatusSnapshot,
    ProcessGroupStatusSnapshotEntity,
    ProcessorStatusSnapshotEntity,
    RemoteProcessGroupStatusSnapshotEntity
} from '../index';
import {
    loadSummaryListing,
    loadSummaryListingSuccess,
    resetSummaryState,
    selectClusterNode
} from './summary-listing.actions';
import { SummaryListingState } from './index';

export const initialState: SummaryListingState = {
    processGroupStatus: null,
    processorStatusSnapshots: [],
    processGroupStatusSnapshots: [],
    inputPortStatusSnapshots: [],
    outputPortStatusSnapshots: [],
    connectionStatusSnapshots: [],
    remoteProcessGroupStatusSnapshots: [],
    selectedClusterNode: null,
    status: 'pending',
    loadedTimestamp: ''
};

export const summaryListingReducer = createReducer(
    initialState,

    on(loadSummaryListing, (state) => ({
        ...state,
        status: 'loading' as const
    })),

    on(loadSummaryListingSuccess, (state, { response }) => {
        const processors: ProcessorStatusSnapshotEntity[] = flattenProcessorStatusSnapshots(
            response.status.processGroupStatus.aggregateSnapshot
        );

        // get the root pg entity
        const root: ProcessGroupStatusSnapshotEntity = {
            id: response.status.processGroupStatus.id,
            canRead: response.status.canRead,
            processGroupStatusSnapshot: response.status.processGroupStatus.aggregateSnapshot
        };

        const childProcessGroups: ProcessGroupStatusSnapshotEntity[] = flattenProcessGroupStatusSnapshots(
            response.status.processGroupStatus.aggregateSnapshot
        );

        const inputPorts: PortStatusSnapshotEntity[] = flattenInputPortStatusSnapshots(
            response.status.processGroupStatus.aggregateSnapshot
        );
        const outputPorts: PortStatusSnapshotEntity[] = flattenOutputPortStatusSnapshots(
            response.status.processGroupStatus.aggregateSnapshot
        );
        const connections: ConnectionStatusSnapshotEntity[] = flattenConnectionStatusSnapshots(
            response.status.processGroupStatus.aggregateSnapshot
        );
        const rpgs: RemoteProcessGroupStatusSnapshotEntity[] = flattenRpgStatusSnapshots(
            response.status.processGroupStatus.aggregateSnapshot
        );

        return {
            ...state,
            status: 'success' as const,
            loadedTimestamp: response.status.processGroupStatus.statsLastRefreshed,
            processGroupStatus: response.status,
            processorStatusSnapshots: processors,
            processGroupStatusSnapshots: [root, ...childProcessGroups],
            inputPortStatusSnapshots: inputPorts,
            outputPortStatusSnapshots: outputPorts,
            connectionStatusSnapshots: connections,
            remoteProcessGroupStatusSnapshots: rpgs
        };
    }),

    on(resetSummaryState, () => ({
        ...initialState
    })),

    on(selectClusterNode, (state, { clusterNode }) => ({
        ...state,
        selectedClusterNode: clusterNode
    }))
);

function flattenProcessorStatusSnapshots(
    snapshot: ProcessGroupStatusSnapshot,
    parentPath = ''
): ProcessorStatusSnapshotEntity[] {
    const path = `${parentPath}/${snapshot.name}`;
    // supplement the processors with the parent process group name
    const processors = snapshot.processorStatusSnapshots.map((p) => {
        return {
            ...p,
            processorStatusSnapshot: {
                ...p.processorStatusSnapshot,
                parentProcessGroupName: snapshot.name,
                processGroupNamePath: path
            }
        };
    });

    if (snapshot.processGroupStatusSnapshots?.length > 0) {
        const children = snapshot.processGroupStatusSnapshots
            .map((pg) => pg.processGroupStatusSnapshot)
            .flatMap((pg) => flattenProcessorStatusSnapshots(pg, path));
        return [...processors, ...children];
    } else {
        return processors;
    }
}

function flattenProcessGroupStatusSnapshots(snapshot: ProcessGroupStatusSnapshot): ProcessGroupStatusSnapshotEntity[] {
    const processGroups = [...snapshot.processGroupStatusSnapshots];

    if (snapshot.processGroupStatusSnapshots?.length > 0) {
        const children = snapshot.processGroupStatusSnapshots
            .map((pg) => pg.processGroupStatusSnapshot)
            .flatMap((pg) => flattenProcessGroupStatusSnapshots(pg));
        return [...processGroups, ...children];
    } else {
        return [...processGroups];
    }
}

function flattenInputPortStatusSnapshots(snapshot: ProcessGroupStatusSnapshot): PortStatusSnapshotEntity[] {
    const ports = [...snapshot.inputPortStatusSnapshots];

    if (snapshot.processGroupStatusSnapshots?.length > 0) {
        const children = snapshot.processGroupStatusSnapshots
            .map((pg) => pg.processGroupStatusSnapshot)
            .flatMap((pg) => flattenInputPortStatusSnapshots(pg));
        return [...ports, ...children];
    } else {
        return ports;
    }
}

function flattenOutputPortStatusSnapshots(snapshot: ProcessGroupStatusSnapshot): PortStatusSnapshotEntity[] {
    const ports = [...snapshot.outputPortStatusSnapshots];

    if (snapshot.processGroupStatusSnapshots?.length > 0) {
        const children = snapshot.processGroupStatusSnapshots
            .map((pg) => pg.processGroupStatusSnapshot)
            .flatMap((pg) => flattenOutputPortStatusSnapshots(pg));
        return [...ports, ...children];
    } else {
        return ports;
    }
}

function flattenConnectionStatusSnapshots(snapshot: ProcessGroupStatusSnapshot): ConnectionStatusSnapshotEntity[] {
    const connections = [...snapshot.connectionStatusSnapshots];

    if (snapshot.processGroupStatusSnapshots?.length > 0) {
        const children = snapshot.processGroupStatusSnapshots
            .map((pg) => pg.processGroupStatusSnapshot)
            .flatMap((pg) => flattenConnectionStatusSnapshots(pg));
        return [...connections, ...children];
    } else {
        return connections;
    }
}

function flattenRpgStatusSnapshots(snapshot: ProcessGroupStatusSnapshot): RemoteProcessGroupStatusSnapshotEntity[] {
    const rpgs = [...snapshot.remoteProcessGroupStatusSnapshots];

    if (snapshot.processGroupStatusSnapshots?.length > 0) {
        const children = snapshot.processGroupStatusSnapshots
            .map((pg) => pg.processGroupStatusSnapshot)
            .flatMap((pg) => flattenRpgStatusSnapshots(pg));
        return [...rpgs, ...children];
    } else {
        return rpgs;
    }
}
