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

import { ComponentType } from '@nifi/shared';
import {
    remoteProcessGroupSupportsModification,
    runnableSupportsModification
} from '../../utils/component-state.utils';
import { isProcessorTarget, SelectionTarget } from './selection.utils';

type RunStatusEntity = SelectionTarget['entity'];

function canOperateEntity(entity: RunStatusEntity): boolean {
    const operateWrite = 'operatePermissions' in entity && entity.operatePermissions?.canWrite === true;
    return entity.permissions.canWrite === true || operateWrite;
}

function getAggregateRunStatus(entity: RunStatusEntity): string | undefined {
    if (!('status' in entity) || !entity.status) {
        return undefined;
    }
    const snapshot = entity.status.aggregateSnapshot;
    return snapshot && 'runStatus' in snapshot ? snapshot.runStatus : undefined;
}

function isEntityRunnable(entity: RunStatusEntity, type: ComponentType): boolean {
    switch (type) {
        case ComponentType.ProcessGroup:
            return true;
        case ComponentType.Processor:
        case ComponentType.InputPort:
        case ComponentType.OutputPort:
            return (
                runnableSupportsModification(entity) &&
                getAggregateRunStatus(entity) === 'Stopped' &&
                canOperateEntity(entity)
            );
        default:
            return false;
    }
}

function isEntityStoppable(entity: RunStatusEntity, type: ComponentType): boolean {
    switch (type) {
        case ComponentType.ProcessGroup:
            return true;
        case ComponentType.Processor:
        case ComponentType.InputPort:
        case ComponentType.OutputPort: {
            if (!canOperateEntity(entity)) {
                return false;
            }
            const runStatus = getAggregateRunStatus(entity);
            if (type === ComponentType.Processor && runStatus === 'Invalid') {
                return 'physicalState' in entity && entity.physicalState === 'STARTING';
            }
            return runStatus === 'Running';
        }
        default:
            return false;
    }
}

function canStartTransmitting(entity: RunStatusEntity, type: ComponentType): boolean {
    return (
        type === ComponentType.RemoteProcessGroup &&
        canOperateEntity(entity) &&
        remoteProcessGroupSupportsModification(entity)
    );
}

function canStopTransmitting(entity: RunStatusEntity, type: ComponentType): boolean {
    if (type !== ComponentType.RemoteProcessGroup) {
        return false;
    }
    const transmissionStatus =
        'status' in entity && entity.status && 'transmissionStatus' in entity.status
            ? entity.status.transmissionStatus
            : undefined;
    return canOperateEntity(entity) && transmissionStatus === 'Transmitting';
}

export function getStartable(components: SelectionTarget[]): SelectionTarget[] {
    return components.filter(
        (component) =>
            isEntityRunnable(component.entity, component.ui.componentType) ||
            canStartTransmitting(component.entity, component.ui.componentType)
    );
}

export function getStoppable(components: SelectionTarget[]): SelectionTarget[] {
    return components.filter(
        (component) =>
            isEntityStoppable(component.entity, component.ui.componentType) ||
            canStopTransmitting(component.entity, component.ui.componentType)
    );
}

export function areAnyRunnable(components: SelectionTarget[]): boolean {
    return getStartable(components).length > 0;
}

export function areAnyStoppable(components: SelectionTarget[]): boolean {
    return getStoppable(components).length > 0;
}

function isEntityEnableable(entity: RunStatusEntity, type: ComponentType): boolean {
    switch (type) {
        case ComponentType.ProcessGroup:
            return true;
        case ComponentType.Processor:
        case ComponentType.InputPort:
        case ComponentType.OutputPort:
            return (
                canOperateEntity(entity) &&
                runnableSupportsModification(entity) &&
                getAggregateRunStatus(entity) === 'Disabled'
            );
        default:
            return false;
    }
}

function isEntityDisableable(entity: RunStatusEntity, type: ComponentType): boolean {
    switch (type) {
        case ComponentType.ProcessGroup:
            return true;
        case ComponentType.Processor:
        case ComponentType.InputPort:
        case ComponentType.OutputPort: {
            if (!canOperateEntity(entity) || !runnableSupportsModification(entity)) {
                return false;
            }
            const runStatus = getAggregateRunStatus(entity);
            return runStatus === 'Stopped' || runStatus === 'Invalid';
        }
        default:
            return false;
    }
}

export function getEnableable(components: SelectionTarget[]): SelectionTarget[] {
    return components.filter((component) => isEntityEnableable(component.entity, component.ui.componentType));
}

export function getDisableable(components: SelectionTarget[]): SelectionTarget[] {
    return components.filter((component) => isEntityDisableable(component.entity, component.ui.componentType));
}

export function areAllEnableable(components: SelectionTarget[]): boolean {
    return components.length > 0 && getEnableable(components).length === components.length;
}

export function areAllDisableable(components: SelectionTarget[]): boolean {
    return components.length > 0 && getDisableable(components).length === components.length;
}

export function canRunOnce(components: SelectionTarget[]): boolean {
    if (components.length !== 1 || components[0].ui.componentType !== ComponentType.Processor) {
        return false;
    }
    return isEntityRunnable(components[0].entity, ComponentType.Processor);
}

export function canTerminate(components: SelectionTarget[]): boolean {
    if (components.length !== 1 || !isProcessorTarget(components[0])) {
        return false;
    }
    const entity = components[0].entity;
    const snapshot = entity.status?.aggregateSnapshot;
    return canOperateEntity(entity) && snapshot?.runStatus !== 'Running' && (snapshot?.activeThreadCount ?? 0) > 0;
}
