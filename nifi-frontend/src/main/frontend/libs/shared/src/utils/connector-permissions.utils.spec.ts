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

import { ConnectorEntity } from '../types';
import {
    canReadConnector,
    canModifyConnector,
    canOperateConnector,
    getConnectorAction,
    isConnectorActionAllowed,
    getConnectorActionDisabledReason
} from './connector-permissions.utils';

function createMockEntity(overrides: Partial<ConnectorEntity> = {}): ConnectorEntity {
    return {
        id: 'test-id',
        uri: 'http://localhost/connectors/test-id',
        revision: { version: 0 },
        bulletins: [],
        status: {},
        permissions: { canRead: true, canWrite: true },
        component: {
            id: 'test-id',
            name: 'Test Connector',
            type: 'org.example.TestConnector',
            state: 'STOPPED',
            bundle: { group: 'org.example', artifact: 'test', version: '1.0' },
            availableActions: [
                { name: 'START', description: 'Start', allowed: true },
                { name: 'STOP', description: 'Stop', allowed: false, reasonNotAllowed: 'Not running' },
                { name: 'CONFIGURE', description: 'Configure', allowed: true },
                { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: true },
                {
                    name: 'CANCEL_DRAIN_FLOWFILES',
                    description: 'Cancel drain',
                    allowed: false,
                    reasonNotAllowed: 'Not draining'
                },
                { name: 'DELETE', description: 'Delete', allowed: false, reasonNotAllowed: 'Connector is running' }
            ],
            managedProcessGroupId: 'pg-1'
        },
        ...overrides
    };
}

describe('connector-permissions.utils', () => {
    describe('canReadConnector', () => {
        it('should return true when canRead is true', () => {
            const entity = createMockEntity({ permissions: { canRead: true, canWrite: false } });
            expect(canReadConnector(entity)).toBe(true);
        });

        it('should return false when canRead is false', () => {
            const entity = createMockEntity({ permissions: { canRead: false, canWrite: false } });
            expect(canReadConnector(entity)).toBe(false);
        });
    });

    describe('canModifyConnector', () => {
        it('should return true when canWrite is true', () => {
            const entity = createMockEntity({ permissions: { canRead: true, canWrite: true } });
            expect(canModifyConnector(entity)).toBe(true);
        });

        it('should return false when canWrite is false', () => {
            const entity = createMockEntity({ permissions: { canRead: true, canWrite: false } });
            expect(canModifyConnector(entity)).toBe(false);
        });
    });

    describe('canOperateConnector', () => {
        it('should return true when canWrite is true', () => {
            const entity = createMockEntity({ permissions: { canRead: true, canWrite: true } });
            expect(canOperateConnector(entity)).toBe(true);
        });

        it('should return true when operatePermissions.canWrite is true', () => {
            const entity = createMockEntity({
                permissions: { canRead: true, canWrite: false },
                operatePermissions: { canRead: false, canWrite: true }
            });
            expect(canOperateConnector(entity)).toBe(true);
        });

        it('should return false when neither canWrite nor operatePermissions.canWrite is true', () => {
            const entity = createMockEntity({
                permissions: { canRead: true, canWrite: false },
                operatePermissions: { canRead: false, canWrite: false }
            });
            expect(canOperateConnector(entity)).toBe(false);
        });

        it('should return false when operatePermissions is undefined', () => {
            const entity = createMockEntity({
                permissions: { canRead: true, canWrite: false }
            });
            delete entity.operatePermissions;
            expect(canOperateConnector(entity)).toBe(false);
        });
    });

    describe('getConnectorAction', () => {
        it('should return the action when found', () => {
            const entity = createMockEntity();
            const action = getConnectorAction(entity, 'START');
            expect(action).toBeDefined();
            expect(action!.name).toBe('START');
            expect(action!.allowed).toBe(true);
        });

        it('should return undefined when action is not found', () => {
            const entity = createMockEntity();
            const action = getConnectorAction(entity, 'PURGE_FLOWFILES');
            expect(action).toBeUndefined();
        });

        it('should return undefined when availableActions is undefined', () => {
            const entity = createMockEntity();
            (entity.component as any).availableActions = undefined;
            const action = getConnectorAction(entity, 'START');
            expect(action).toBeUndefined();
        });
    });

    describe('isConnectorActionAllowed', () => {
        it('should return true when action is allowed', () => {
            const entity = createMockEntity();
            expect(isConnectorActionAllowed(entity, 'START')).toBe(true);
        });

        it('should return false when action is not allowed', () => {
            const entity = createMockEntity();
            expect(isConnectorActionAllowed(entity, 'STOP')).toBe(false);
        });

        it('should return false when action does not exist', () => {
            const entity = createMockEntity();
            expect(isConnectorActionAllowed(entity, 'PURGE_FLOWFILES')).toBe(false);
        });

        it('should return false when availableActions is undefined', () => {
            const entity = createMockEntity();
            (entity.component as any).availableActions = undefined;
            expect(isConnectorActionAllowed(entity, 'START')).toBe(false);
        });
    });

    describe('getConnectorActionDisabledReason', () => {
        it('should return the reason when action is not allowed', () => {
            const entity = createMockEntity();
            expect(getConnectorActionDisabledReason(entity, 'STOP')).toBe('Not running');
        });

        it('should return empty string when action is allowed', () => {
            const entity = createMockEntity();
            expect(getConnectorActionDisabledReason(entity, 'START')).toBe('');
        });

        it('should return empty string when action does not exist', () => {
            const entity = createMockEntity();
            expect(getConnectorActionDisabledReason(entity, 'PURGE_FLOWFILES')).toBe('');
        });

        it('should return empty string when availableActions is undefined', () => {
            const entity = createMockEntity();
            (entity.component as any).availableActions = undefined;
            expect(getConnectorActionDisabledReason(entity, 'START')).toBe('');
        });
    });
});
