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

import { ComponentType, ConnectableDTO } from '@nifi/shared';
import {
    remoteProcessGroupSupportsModification,
    runnableSupportsModification
} from '../../utils/component-state.utils';
import { isConnectionTarget, SelectionTarget } from './selection.utils';

/**
 * Whether a connection endpoint allows the connection to be removed. Funnels are always
 * running but never block removal; endpoints in a child or remote process group are always
 * configurable; other local endpoints must be stopped.
 */
function connectableSupportsModification(connectable: ConnectableDTO, processGroupId: string | null): boolean {
    if (
        connectable.type === 'FUNNEL' ||
        connectable.type === 'REMOTE_INPUT_PORT' ||
        connectable.type === 'REMOTE_OUTPUT_PORT' ||
        connectable.groupId !== processGroupId
    ) {
        return true;
    }
    return !connectable.running;
}

/**
 * Whether the target can be deleted: it must be writable and in a state that
 * supports modification (stopped runnables, non-transmitting remote process
 * groups, and connections whose local endpoints are stopped).
 */
export function isDeletable(target: SelectionTarget, processGroupId: string | null): boolean {
    if (target.entity.permissions?.canWrite !== true) {
        return false;
    }

    if (isConnectionTarget(target)) {
        const connection = target.entity.component;
        // Unreadable connections omit their endpoints; the server still enforces the running check.
        if (!connection) {
            return true;
        }
        return (
            connectableSupportsModification(connection.source, processGroupId) &&
            connectableSupportsModification(connection.destination, processGroupId)
        );
    }

    switch (target.ui.componentType) {
        case ComponentType.Processor:
        case ComponentType.InputPort:
        case ComponentType.OutputPort:
            return runnableSupportsModification(target.entity);
        case ComponentType.RemoteProcessGroup:
            return remoteProcessGroupSupportsModification(target.entity);
        default:
            return true;
    }
}

export function areDeletable(targets: SelectionTarget[], processGroupId: string | null): boolean {
    return targets.length > 0 && targets.every((target) => isDeletable(target, processGroupId));
}
