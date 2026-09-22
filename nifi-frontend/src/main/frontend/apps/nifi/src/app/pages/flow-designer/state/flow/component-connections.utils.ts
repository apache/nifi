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

import { ConnectionEntity, ProcessGroupFlowEntity } from './index';

const REMOTE_PORT_TYPES: string[] = ['REMOTE_INPUT_PORT', 'REMOTE_OUTPUT_PORT'];

/**
 * Collects the groups, other than the one that defines the given connections, that hold an end of one
 * of them. Reporting a connection means reporting both of its ends, and a connection is readable only
 * when the current user can read both, so each end has to be looked up in its own group to be named on
 * its own permission.
 *
 * A Remote Process Group is left out: it is not a group whose flow can be loaded, and the ports inside
 * it are reported through the Remote Process Group itself.
 *
 * @param connections the connections being reported
 * @param definingGroupId the group that defines them, whose flow is already loaded
 * @returns the id of each other group holding an end, without repeats
 */
export function collectEndpointGroupIds(connections: ConnectionEntity[], definingGroupId: string): string[] {
    const groupIds = new Set<string>();

    connections.forEach((connection) => {
        if (connection.sourceGroupId !== definingGroupId && !REMOTE_PORT_TYPES.includes(connection.sourceType)) {
            groupIds.add(connection.sourceGroupId);
        }
        if (
            connection.destinationGroupId !== definingGroupId &&
            !REMOTE_PORT_TYPES.includes(connection.destinationType)
        ) {
            groupIds.add(connection.destinationGroupId);
        }
    });

    return [...groupIds];
}

/**
 * Collects the name of every component the current user can read across the given flows. Each component
 * is listed by its own group with its own permission, which is what lets one end of a connection be
 * named while the other is reported as unauthorized.
 *
 * @param flowEntities the flow of each group holding an end of the connections being reported
 * @returns the name of each readable component, by id
 */
export function buildComponentIdToNameMap(flowEntities: ProcessGroupFlowEntity[]): Map<string, string> {
    const idToName = new Map<string, string>();

    flowEntities.forEach((flowEntity) => {
        const flow = flowEntity.processGroupFlow.flow;

        [
            ...(flow.processors ?? []),
            ...(flow.inputPorts ?? []),
            ...(flow.outputPorts ?? []),
            ...(flow.funnels ?? [])
        ].forEach((component) => {
            if (component.permissions.canRead) {
                idToName.set(component.id, component.component.name ?? '');
            }
        });
    });

    return idToName;
}
