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

import { connectorControllerServicesReducer } from './connector-controller-services.reducer';
import {
    loadConnectorControllerServices,
    loadConnectorControllerServicesFailure,
    loadConnectorControllerServicesSuccess,
    resetConnectorControllerServicesState
} from './connector-controller-services.actions';
import { ConnectorControllerServicesState, initialConnectorControllerServicesState } from './index';
import { ErrorContextKey } from '../../../../state/error';
import { BreadcrumbEntity, ControllerServiceEntity } from '../../../../state/shared';

function buildBreadcrumb(): BreadcrumbEntity {
    return {
        id: 'pg-1',
        permissions: { canRead: true, canWrite: false },
        breadcrumb: { id: 'pg-1', name: 'Root' },
        parentBreadcrumb: undefined,
        versionedFlowState: undefined
    } as unknown as BreadcrumbEntity;
}

function buildService(overrides: Partial<ControllerServiceEntity> = {}): ControllerServiceEntity {
    return {
        id: 'svc-1',
        permissions: { canRead: true, canWrite: false },
        component: { id: 'svc-1', name: 'My Service', state: 'DISABLED' },
        ...overrides
    } as unknown as ControllerServiceEntity;
}

describe('connectorControllerServicesReducer', () => {
    it('should mark the slice as loading and capture connector + process group IDs on load', () => {
        const next = connectorControllerServicesReducer(
            initialConnectorControllerServicesState,
            loadConnectorControllerServices({
                request: { connectorId: 'conn-1', processGroupId: 'pg-1' }
            })
        );

        expect(next.status).toBe('loading');
        expect(next.connectorId).toBe('conn-1');
        expect(next.processGroupId).toBe('pg-1');
        expect(next.error).toBeNull();
        expect(next.hasAttemptedLoad).toBe(false);
    });

    it('should populate breadcrumbs, services, and timestamp on load success', () => {
        const breadcrumb = buildBreadcrumb();
        const service = buildService();

        const next = connectorControllerServicesReducer(
            initialConnectorControllerServicesState,
            loadConnectorControllerServicesSuccess({
                response: {
                    connectorId: 'conn-1',
                    processGroupId: 'pg-1',
                    breadcrumb,
                    controllerServices: [service],
                    loadedTimestamp: '2023-03-01T12:00:00Z'
                }
            })
        );

        expect(next.status).toBe('success');
        expect(next.breadcrumb).toEqual(breadcrumb);
        expect(next.controllerServices).toEqual([service]);
        expect(next.loadedTimestamp).toBe('2023-03-01T12:00:00Z');
        expect(next.hasAttemptedLoad).toBe(true);
    });

    it('should mark the slice as error and surface the first error message on load failure', () => {
        const next = connectorControllerServicesReducer(
            initialConnectorControllerServicesState,
            loadConnectorControllerServicesFailure({
                errorContext: {
                    errors: ['Boom', 'Other'],
                    context: ErrorContextKey.CONTROLLER_SERVICES
                }
            })
        );

        expect(next.status).toBe('error');
        expect(next.error).toBe('Boom');
        expect(next.hasAttemptedLoad).toBe(true);
    });

    it('should fall back to a default error message when no errors are reported', () => {
        const next = connectorControllerServicesReducer(
            initialConnectorControllerServicesState,
            loadConnectorControllerServicesFailure({
                errorContext: {
                    errors: [],
                    context: ErrorContextKey.CONTROLLER_SERVICES
                }
            })
        );

        expect(next.status).toBe('error');
        expect(next.error).toBe('Failed to load controller services');
    });

    it('should reset to the initial state', () => {
        const populated: ConnectorControllerServicesState = {
            ...initialConnectorControllerServicesState,
            connectorId: 'conn-1',
            processGroupId: 'pg-1',
            controllerServices: [buildService()],
            status: 'success'
        };

        const next = connectorControllerServicesReducer(populated, resetConnectorControllerServicesState());

        expect(next).toEqual(initialConnectorControllerServicesState);
    });
});
