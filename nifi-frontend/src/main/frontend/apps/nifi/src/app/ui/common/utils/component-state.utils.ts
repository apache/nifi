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

/**
 * Determines whether a runnable component (Processor or Port) currently supports modification.
 * A component cannot be modified if it is Running or has active threads.
 */
export function runnableSupportsModification(entity: any): boolean {
    return !(
        entity.status?.aggregateSnapshot?.runStatus === 'Running' ||
        (entity.status?.aggregateSnapshot?.activeThreadCount ?? 0) > 0
    );
}

/**
 * Determines whether a Remote Process Group currently supports modification.
 * An RPG cannot be modified if it is Transmitting or has active threads.
 */
export function remoteProcessGroupSupportsModification(entity: any): boolean {
    return !(
        entity.status?.transmissionStatus === 'Transmitting' ||
        (entity.status?.aggregateSnapshot?.activeThreadCount ?? 0) > 0
    );
}

/**
 * Returns the ComponentType for a given NiFi connectable source type string.
 */
export function getComponentTypeForSource(connectableType: string): ComponentType | null {
    switch (connectableType) {
        case 'PROCESSOR':
            return ComponentType.Processor;
        case 'REMOTE_OUTPUT_PORT':
            return ComponentType.RemoteProcessGroup;
        case 'OUTPUT_PORT':
            return ComponentType.ProcessGroup;
        case 'INPUT_PORT':
            return ComponentType.InputPort;
        case 'FUNNEL':
            return ComponentType.Funnel;
        default:
            return null;
    }
}

/**
 * Returns the ComponentType for a given NiFi connectable destination type string.
 */
export function getComponentTypeForDestination(connectableType: string): ComponentType | null {
    switch (connectableType) {
        case 'PROCESSOR':
            return ComponentType.Processor;
        case 'REMOTE_INPUT_PORT':
            return ComponentType.RemoteProcessGroup;
        case 'INPUT_PORT':
            return ComponentType.ProcessGroup;
        case 'OUTPUT_PORT':
            return ComponentType.OutputPort;
        case 'FUNNEL':
            return ComponentType.Funnel;
        default:
            return null;
    }
}

/**
 * Returns the NiFi connectable type string for a given ComponentType destination.
 */
export function getConnectableTypeForDestination(type: ComponentType): string {
    switch (type) {
        case ComponentType.Processor:
            return 'PROCESSOR';
        case ComponentType.RemoteProcessGroup:
            return 'REMOTE_INPUT_PORT';
        case ComponentType.ProcessGroup:
            return 'INPUT_PORT';
        case ComponentType.OutputPort:
            return 'OUTPUT_PORT';
        case ComponentType.Funnel:
            return 'FUNNEL';
        default:
            return '';
    }
}
