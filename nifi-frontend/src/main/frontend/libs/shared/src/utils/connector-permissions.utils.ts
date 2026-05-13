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

import { ConnectorAction, ConnectorActionName, ConnectorEntity, ConnectorState, StatusVariant } from '../types';

export function canReadConnector(entity: ConnectorEntity): boolean {
    return entity.permissions.canRead;
}

export function canModifyConnector(entity: ConnectorEntity): boolean {
    return entity.permissions.canWrite;
}

export function canOperateConnector(entity: ConnectorEntity): boolean {
    return canModifyConnector(entity) || !!entity.operatePermissions?.canWrite;
}

export function getConnectorAction(
    entity: ConnectorEntity,
    actionName: ConnectorActionName
): ConnectorAction | undefined {
    return entity.component.availableActions?.find((action) => action.name === actionName);
}

export function isConnectorActionAllowed(entity: ConnectorEntity, actionName: ConnectorActionName): boolean {
    const action = getConnectorAction(entity, actionName);
    return action?.allowed ?? false;
}

export function getConnectorActionDisabledReason(entity: ConnectorEntity, actionName: ConnectorActionName): string {
    const action = getConnectorAction(entity, actionName);
    return action?.reasonNotAllowed ?? '';
}

export function getConnectorStateVariant(state: string): StatusVariant {
    switch (state) {
        case ConnectorState.RUNNING:
            return 'success';
        case ConnectorState.STOPPED:
        case ConnectorState.DISABLED:
            return 'neutral';
        case ConnectorState.STARTING:
        case ConnectorState.UPDATING:
        case ConnectorState.STOPPING:
        case ConnectorState.DRAINING:
        case ConnectorState.PREPARING_FOR_UPDATE:
            return 'info';
        case ConnectorState.UPDATE_FAILED:
            return 'critical';
        default:
            return 'neutral';
    }
}
