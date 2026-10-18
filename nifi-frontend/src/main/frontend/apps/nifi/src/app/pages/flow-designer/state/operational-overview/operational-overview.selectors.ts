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

import { createSelector } from '@ngrx/store';
import { ComponentType } from '@nifi/shared';
import { FlowState } from '../flow';
import { selectFlowState, selectProcessGroupIdFromRoute, selectSingleSelectedComponent } from '../flow/flow.selectors';

interface ObservedEntity {
    id: string;
    permissions?: { canRead: boolean };
    component?: {
        name?: string;
        comments?: string;
        config?: { comments?: string; schedulingPeriod?: string; executionNode?: string };
        state?: string;
        validationStatus?: string;
        type?: string;
        backPressureObjectThreshold?: number;
        backPressureDataSizeThreshold?: string;
    };
    status?: {
        runStatus?: string;
        sourceName?: string;
        destinationName?: string;
        aggregateSnapshot?: {
            runStatus?: string;
            flowFilesQueued?: number;
            bytesQueued?: number;
            percentUseCount?: number;
            percentUseBytes?: number;
        };
    };
}

export interface OperationalOverview {
    status: 'loading' | 'forbidden' | 'error' | 'ready';
    refreshFailed?: boolean;
    name: string;
    lastRefreshed: string;
    refreshing: boolean;
    processors: Record<'running' | 'stopped' | 'invalid' | 'disabled' | 'unknown', number>;
    visibleProcessors: number;
    visibleConnections: number;
    restricted: boolean;
    queuedFlowFiles: number | null;
    queuedBytes: number | null;
    measuredConnections: number;
    highestUtilization: { id: string; name: string; percent: number } | null;
    controllerBulletins: { message: string; level: string }[];
}

export interface OperationalSelection {
    id: string;
    kind: 'processor' | 'connection';
    readable: boolean;
    name: string;
    state?: string;
    validation?: string;
    comments?: string;
    configuration?: { type?: string; schedulingPeriod?: string; executionNode?: string };
    source?: string;
    destination?: string;
    queuedFlowFiles?: number;
    queuedBytes?: number;
    objectThreshold?: number;
    dataThreshold?: string;
    percentCount?: number;
    percentBytes?: number;
}

function number(value: unknown): number | undefined {
    return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : undefined;
}

function utilization(entity: ObservedEntity): number | undefined {
    const snapshot = entity.status?.aggregateSnapshot;
    const values = [number(snapshot?.percentUseCount), number(snapshot?.percentUseBytes)].filter(
        (value): value is number => value !== undefined
    );
    return values.length ? Math.max(...values) : undefined;
}

export function summarizeOperations(state: FlowState, requestedId: string | null | undefined): OperationalOverview {
    const group = state.flow.processGroupFlow;
    const sameGroup =
        !requestedId ||
        requestedId === group.id ||
        (requestedId === 'root' && (group.parentGroupId === null || group.parentGroupId === undefined));
    const model: OperationalOverview = {
        status: 'loading',
        name: 'Current process group',
        lastRefreshed: '',
        refreshing: state.status === 'loading',
        processors: { running: 0, stopped: 0, invalid: 0, disabled: 0, unknown: 0 },
        visibleProcessors: 0,
        visibleConnections: 0,
        restricted: false,
        queuedFlowFiles: null,
        queuedBytes: null,
        measuredConnections: 0,
        highestUtilization: null,
        controllerBulletins: []
    };
    const failed = state.loadError && (!requestedId || state.loadError.id === requestedId);
    if (failed && [401, 403].includes(state.loadError!.status)) return { ...model, status: 'forbidden' };
    if (!group.id || !sameGroup || state.status === 'pending') {
        return failed ? { ...model, status: 'error', refreshing: false } : model;
    }
    model.refreshFailed = !!failed;
    if (!state.flow.permissions.canRead) return { ...model, status: 'forbidden' };
    model.status = 'ready';
    model.name = group.breadcrumb.breadcrumb.name || 'Current process group';
    model.lastRefreshed = group.lastRefreshed;
    const processors: ObservedEntity[] = group.flow.processors ?? [];
    const connections: ObservedEntity[] = group.flow.connections ?? [];
    const visibleProcessors = processors.filter((entity) => entity.permissions?.canRead === true);
    const visibleConnections = connections.filter((entity) => entity.permissions?.canRead === true);
    model.visibleProcessors = visibleProcessors.length;
    model.visibleConnections = visibleConnections.length;
    model.restricted =
        visibleProcessors.length !== processors.length || visibleConnections.length !== connections.length;
    for (const processor of visibleProcessors) {
        const component = processor.component;
        const state = (
            processor.status?.aggregateSnapshot?.runStatus ??
            processor.status?.runStatus ??
            component?.state ??
            ''
        ).toUpperCase();
        if (state === 'DISABLED') model.processors.disabled++;
        else if (state === 'INVALID' || component?.validationStatus === 'INVALID') model.processors.invalid++;
        else if (state === 'RUNNING') model.processors.running++;
        else if (state === 'STOPPED') model.processors.stopped++;
        else model.processors.unknown++;
    }
    function total(field: 'flowFilesQueued' | 'bytesQueued'): number | null {
        const values = visibleConnections
            .map((entity) => number(entity.status?.aggregateSnapshot?.[field]))
            .filter((value): value is number => value !== undefined);
        return values.length === visibleConnections.length ? values.reduce((sum, value) => sum + value, 0) : null;
    }
    model.queuedFlowFiles = total('flowFilesQueued');
    model.queuedBytes = total('bytesQueued');
    for (const connection of visibleConnections) {
        const percent = utilization(connection);
        if (percent !== undefined) {
            model.measuredConnections++;
            if (!model.highestUtilization || percent > model.highestUtilization.percent) {
                model.highestUtilization = {
                    id: connection.id,
                    name: connection.component?.name || 'Unnamed connection',
                    percent
                };
            }
        }
    }
    model.controllerBulletins = (state.controllerBulletins.bulletins ?? []).flatMap((entity) =>
        entity.canRead && entity.bulletin ? [{ message: entity.bulletin.message, level: entity.bulletin.level }] : []
    );
    return model;
}

export const selectOperationalOverview = createSelector(
    selectFlowState,
    selectProcessGroupIdFromRoute,
    summarizeOperations
);

export const selectOperationalSelection = createSelector(
    selectFlowState,
    selectSingleSelectedComponent,
    selectOperationalOverview,
    (state, selection, overview): OperationalSelection | null => {
        if (!selection || overview.status !== 'ready') return null;
        const kind =
            selection.componentType === ComponentType.Processor
                ? 'processor'
                : selection.componentType === ComponentType.Connection
                  ? 'connection'
                  : null;
        if (!kind) return null;
        const collection: ObservedEntity[] =
            kind === 'processor'
                ? state.flow.processGroupFlow.flow.processors
                : state.flow.processGroupFlow.flow.connections;
        const entity = collection.find((entry) => entry.id === selection.id);
        if (!entity) return null;
        const model: OperationalSelection = {
            id: entity.id,
            kind,
            readable: entity.permissions?.canRead === true,
            name: 'Restricted component'
        };
        if (!model.readable) return model;
        const component = entity.component;
        const snapshot = entity.status?.aggregateSnapshot;
        return {
            ...model,
            name: component?.name || 'Unnamed component',
            state: snapshot?.runStatus ?? entity.status?.runStatus ?? component?.state,
            validation: component?.validationStatus,
            comments: component?.config?.comments ?? component?.comments,
            configuration: {
                type: component?.type,
                schedulingPeriod: component?.config?.schedulingPeriod,
                executionNode: component?.config?.executionNode
            },
            source: entity.status?.sourceName,
            destination: entity.status?.destinationName,
            queuedFlowFiles: number(snapshot?.flowFilesQueued),
            queuedBytes: number(snapshot?.bytesQueued),
            objectThreshold: number(component?.backPressureObjectThreshold),
            dataThreshold: component?.backPressureDataSizeThreshold,
            percentCount: number(snapshot?.percentUseCount),
            percentBytes: number(snapshot?.percentUseBytes)
        };
    }
);
