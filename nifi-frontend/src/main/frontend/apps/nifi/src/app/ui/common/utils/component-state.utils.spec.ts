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
    getComponentTypeForDestination,
    getComponentTypeForSource,
    getConnectableTypeForDestination,
    remoteProcessGroupSupportsModification,
    runnableSupportsModification
} from './component-state.utils';

describe('component-state.utils', () => {
    describe('runnableSupportsModification', () => {
        function createEntity(runStatus: string, activeThreadCount = 0): any {
            return {
                status: {
                    aggregateSnapshot: { runStatus, activeThreadCount }
                }
            };
        }

        it('should return true when stopped with no active threads', () => {
            expect(runnableSupportsModification(createEntity('Stopped'))).toBe(true);
        });

        it('should return false when Running', () => {
            expect(runnableSupportsModification(createEntity('Running'))).toBe(false);
        });

        it('should return false when stopped but has active threads', () => {
            expect(runnableSupportsModification(createEntity('Stopped', 2))).toBe(false);
        });

        it('should return true when Disabled', () => {
            expect(runnableSupportsModification(createEntity('Disabled'))).toBe(true);
        });

        it('should handle missing status gracefully', () => {
            expect(runnableSupportsModification({})).toBe(true);
        });
    });

    describe('remoteProcessGroupSupportsModification', () => {
        function createRpgEntity(transmissionStatus: string, activeThreadCount = 0): any {
            return {
                status: {
                    transmissionStatus,
                    aggregateSnapshot: { activeThreadCount }
                }
            };
        }

        it('should return true when not transmitting with no active threads', () => {
            expect(remoteProcessGroupSupportsModification(createRpgEntity('NotTransmitting'))).toBe(true);
        });

        it('should return false when Transmitting', () => {
            expect(remoteProcessGroupSupportsModification(createRpgEntity('Transmitting'))).toBe(false);
        });

        it('should return false when not transmitting but has active threads', () => {
            expect(remoteProcessGroupSupportsModification(createRpgEntity('NotTransmitting', 1))).toBe(false);
        });

        it('should handle missing status gracefully', () => {
            expect(remoteProcessGroupSupportsModification({})).toBe(true);
        });
    });

    describe('getComponentTypeForSource', () => {
        it('should map PROCESSOR to ComponentType.Processor', () => {
            expect(getComponentTypeForSource('PROCESSOR')).toBe(ComponentType.Processor);
        });

        it('should map REMOTE_OUTPUT_PORT to ComponentType.RemoteProcessGroup', () => {
            expect(getComponentTypeForSource('REMOTE_OUTPUT_PORT')).toBe(ComponentType.RemoteProcessGroup);
        });

        it('should map OUTPUT_PORT to ComponentType.ProcessGroup', () => {
            expect(getComponentTypeForSource('OUTPUT_PORT')).toBe(ComponentType.ProcessGroup);
        });

        it('should map INPUT_PORT to ComponentType.InputPort', () => {
            expect(getComponentTypeForSource('INPUT_PORT')).toBe(ComponentType.InputPort);
        });

        it('should map FUNNEL to ComponentType.Funnel', () => {
            expect(getComponentTypeForSource('FUNNEL')).toBe(ComponentType.Funnel);
        });

        it('should return null for unknown type', () => {
            expect(getComponentTypeForSource('UNKNOWN')).toBeNull();
        });
    });

    describe('getComponentTypeForDestination', () => {
        it('should map PROCESSOR to ComponentType.Processor', () => {
            expect(getComponentTypeForDestination('PROCESSOR')).toBe(ComponentType.Processor);
        });

        it('should map REMOTE_INPUT_PORT to ComponentType.RemoteProcessGroup', () => {
            expect(getComponentTypeForDestination('REMOTE_INPUT_PORT')).toBe(ComponentType.RemoteProcessGroup);
        });

        it('should map INPUT_PORT to ComponentType.ProcessGroup', () => {
            expect(getComponentTypeForDestination('INPUT_PORT')).toBe(ComponentType.ProcessGroup);
        });

        it('should map OUTPUT_PORT to ComponentType.OutputPort', () => {
            expect(getComponentTypeForDestination('OUTPUT_PORT')).toBe(ComponentType.OutputPort);
        });

        it('should map FUNNEL to ComponentType.Funnel', () => {
            expect(getComponentTypeForDestination('FUNNEL')).toBe(ComponentType.Funnel);
        });

        it('should return null for unknown type', () => {
            expect(getComponentTypeForDestination('UNKNOWN')).toBeNull();
        });
    });

    describe('getConnectableTypeForDestination', () => {
        it('should map ComponentType.Processor to PROCESSOR', () => {
            expect(getConnectableTypeForDestination(ComponentType.Processor)).toBe('PROCESSOR');
        });

        it('should map ComponentType.RemoteProcessGroup to REMOTE_INPUT_PORT', () => {
            expect(getConnectableTypeForDestination(ComponentType.RemoteProcessGroup)).toBe('REMOTE_INPUT_PORT');
        });

        it('should map ComponentType.ProcessGroup to INPUT_PORT', () => {
            expect(getConnectableTypeForDestination(ComponentType.ProcessGroup)).toBe('INPUT_PORT');
        });

        it('should map ComponentType.OutputPort to OUTPUT_PORT', () => {
            expect(getConnectableTypeForDestination(ComponentType.OutputPort)).toBe('OUTPUT_PORT');
        });

        it('should map ComponentType.Funnel to FUNNEL', () => {
            expect(getConnectableTypeForDestination(ComponentType.Funnel)).toBe('FUNNEL');
        });

        it('should return empty string for unknown type', () => {
            expect(getConnectableTypeForDestination(ComponentType.Label)).toBe('');
        });
    });
});
