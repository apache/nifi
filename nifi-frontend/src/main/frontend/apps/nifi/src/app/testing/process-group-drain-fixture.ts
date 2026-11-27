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

import {
    DrainBlockingComponent,
    ProcessGroupDrainRequest,
    ProcessGroupDrainRequestEntity,
    ProcessGroupDrainSummary
} from '@nifi/shared';

export function createDrainBlockingComponent(overrides: Partial<DrainBlockingComponent> = {}): DrainBlockingComponent {
    return {
        id: 'processor-1',
        groupId: 'pg-1',
        type: 'PROCESSOR',
        name: 'GenerateFlowFile',
        reason: 'INVALID',
        validationErrors: ["'Run Schedule' is invalid"],
        queuedCount: 3,
        ...overrides
    };
}

export function createProcessGroupDrainSummary(
    overrides: Partial<ProcessGroupDrainSummary> = {}
): ProcessGroupDrainSummary {
    return {
        id: 'drain-1',
        prepared: true,
        originalCount: 10,
        originalSize: 1024,
        currentCount: 4,
        currentSize: 400,
        ...overrides
    };
}

export function createProcessGroupDrainRequest(
    overrides: Partial<ProcessGroupDrainRequest> = {}
): ProcessGroupDrainRequest {
    return {
        id: 'drain-1',
        processGroupId: 'pg-1',
        uri: '../nifi-api/process-groups/pg-1/drain-requests/drain-1',
        state: 'DRAINING',
        prepared: true,
        originalCount: 10,
        originalSize: 1024,
        currentCount: 4,
        currentSize: 400,
        blockingComponents: [],
        ...overrides
    };
}

export function createProcessGroupDrainRequestEntity(
    drainRequestOverrides: Partial<ProcessGroupDrainRequest> | null = {}
): ProcessGroupDrainRequestEntity {
    if (drainRequestOverrides === null) {
        return {};
    }

    return {
        drainRequest: createProcessGroupDrainRequest(drainRequestOverrides)
    };
}
