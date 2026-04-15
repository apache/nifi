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

import { ConnectorEntity } from '@nifi/shared';
import { connectorCanvasEntityReducer } from './connector-canvas-entity.reducer';
import { ConnectorCanvasEntityState, initialConnectorCanvasEntityState } from './index';
import {
    loadConnectorEntity,
    loadConnectorEntitySuccess,
    loadConnectorEntityFailure,
    resetConnectorCanvasEntityState
} from './connector-canvas-entity.actions';

describe('connectorCanvasEntityReducer', () => {
    function createMockConnectorEntity(overrides: Partial<ConnectorEntity> = {}): ConnectorEntity {
        return {
            id: 'connector-123',
            uri: '/connectors/connector-123',
            permissions: { canRead: true, canWrite: true },
            operatePermissions: { canRead: true, canWrite: true },
            revision: { version: 0 },
            bulletins: [],
            status: {},
            component: {
                id: 'connector-123',
                name: 'Test Connector',
                type: 'org.test.TestConnector',
                state: 'STOPPED',
                bundle: { group: 'org.test', artifact: 'test', version: '1.0' },
                availableActions: [],
                managedProcessGroupId: 'pg-123'
            },
            ...overrides
        } as ConnectorEntity;
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should return initial state for unknown action', () => {
        const result = connectorCanvasEntityReducer(undefined, { type: 'UNKNOWN' });

        expect(result).toEqual(initialConnectorCanvasEntityState);
    });

    describe('loadConnectorEntity', () => {
        it('should set loadingStatus to loading and clear error', () => {
            const priorState: ConnectorCanvasEntityState = {
                ...initialConnectorCanvasEntityState,
                error: 'previous error'
            };

            const result = connectorCanvasEntityReducer(priorState, loadConnectorEntity({ connectorId: 'c-1' }));

            expect(result.loadingStatus).toBe('loading');
            expect(result.error).toBeNull();
        });
    });

    describe('loadConnectorEntitySuccess', () => {
        it('should store the entity and set loadingStatus to success', () => {
            const entity = createMockConnectorEntity();
            const priorState: ConnectorCanvasEntityState = {
                ...initialConnectorCanvasEntityState,
                loadingStatus: 'loading'
            };

            const result = connectorCanvasEntityReducer(
                priorState,
                loadConnectorEntitySuccess({ connectorEntity: entity })
            );

            expect(result.connectorEntity).toEqual(entity);
            expect(result.loadingStatus).toBe('success');
            expect(result.error).toBeNull();
        });
    });

    describe('loadConnectorEntityFailure', () => {
        it('should set loadingStatus to error and store the error', () => {
            const priorState: ConnectorCanvasEntityState = {
                ...initialConnectorCanvasEntityState,
                loadingStatus: 'loading'
            };

            const result = connectorCanvasEntityReducer(priorState, loadConnectorEntityFailure({ error: 'Not found' }));

            expect(result.loadingStatus).toBe('error');
            expect(result.error).toBe('Not found');
        });
    });

    describe('resetConnectorCanvasEntityState', () => {
        it('should return to initial state', () => {
            const entity = createMockConnectorEntity();
            const priorState: ConnectorCanvasEntityState = {
                connectorEntity: entity,
                loadingStatus: 'success',
                saving: true,
                error: 'some error'
            };

            const result = connectorCanvasEntityReducer(priorState, resetConnectorCanvasEntityState());

            expect(result).toEqual(initialConnectorCanvasEntityState);
        });
    });
});
