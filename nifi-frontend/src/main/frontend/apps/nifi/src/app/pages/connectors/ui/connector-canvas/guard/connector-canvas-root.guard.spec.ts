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

import { TestBed } from '@angular/core/testing';
import { ActivatedRouteSnapshot, Router, UrlTree } from '@angular/router';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { HttpErrorResponse } from '@angular/common/http';
import { firstValueFrom, of, throwError } from 'rxjs';

import { ConnectorEntity } from '@nifi/shared';
import { connectorCanvasRootGuard } from './connector-canvas-root.guard';
import { ConnectorService } from '../../../service/connector.service';
import { ErrorHelper } from '../../../../../service/error-helper.service';
import {
    loadConnectorEntityFailure,
    loadConnectorEntitySuccess
} from '../../../state/connector-canvas-entity/connector-canvas-entity.actions';

function createMockConnector(overrides: Partial<ConnectorEntity> = {}): ConnectorEntity {
    return {
        id: 'connector-1',
        uri: '/connectors/connector-1',
        revision: { version: 1 },
        permissions: { canRead: true, canWrite: true },
        bulletins: [],
        status: {},
        component: {
            id: 'connector-1',
            name: 'Test Connector',
            type: 'org.apache.nifi.TestConnector',
            state: 'STOPPED',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-test-nar',
                version: '1.0.0'
            },
            managedProcessGroupId: 'managed-pg-1',
            availableActions: []
        },
        ...overrides
    };
}

interface SetupOptions {
    connectorResponse?: ConnectorEntity;
    connectorError?: HttpErrorResponse;
}

async function setup(options: SetupOptions = {}) {
    const { connectorResponse, connectorError } = options;

    const mockConnectorService = {
        getConnector: vi.fn()
    };

    if (connectorError) {
        mockConnectorService.getConnector.mockReturnValue(throwError(() => connectorError));
    } else if (connectorResponse) {
        mockConnectorService.getConnector.mockReturnValue(of(connectorResponse));
    }

    const mockUrlTree = {} as UrlTree;

    const mockRouter = {
        parseUrl: vi.fn().mockReturnValue(mockUrlTree),
        navigate: vi.fn().mockResolvedValue(true)
    };

    const mockErrorHelper = {
        getErrorString: vi.fn().mockReturnValue('Connector load failed')
    };

    await TestBed.configureTestingModule({
        providers: [
            provideMockStore(),
            { provide: ConnectorService, useValue: mockConnectorService },
            { provide: Router, useValue: mockRouter },
            { provide: ErrorHelper, useValue: mockErrorHelper }
        ]
    }).compileComponents();

    const store = TestBed.inject(MockStore);
    const dispatchSpy = vi.spyOn(store, 'dispatch');

    return {
        store,
        dispatchSpy,
        mockConnectorService,
        mockRouter,
        mockErrorHelper,
        mockUrlTree
    };
}

describe('connectorCanvasRootGuard', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should redirect to `/connectors` when no connectorId is found', async () => {
        const route = { params: {}, parent: null } as ActivatedRouteSnapshot;
        const { mockRouter, mockConnectorService, dispatchSpy, mockUrlTree } = await setup();

        const result = await firstValueFrom(TestBed.runInInjectionContext(() => connectorCanvasRootGuard(route)));

        expect(mockRouter.parseUrl).toHaveBeenCalledWith('/connectors');
        expect(result).toBe(mockUrlTree);
        expect(mockConnectorService.getConnector).not.toHaveBeenCalled();
        expect(dispatchSpy).not.toHaveBeenCalled();
    });

    it('should call ConnectorService.getConnector and navigate to managed PG on success', async () => {
        const connector = createMockConnector();
        const { mockConnectorService, mockRouter } = await setup({ connectorResponse: connector });
        const route = { params: { id: connector.id }, parent: null } as ActivatedRouteSnapshot;

        await firstValueFrom(TestBed.runInInjectionContext(() => connectorCanvasRootGuard(route)));

        expect(mockConnectorService.getConnector).toHaveBeenCalledWith(connector.id);
        expect(mockRouter.navigate).toHaveBeenCalledWith([
            '/connectors',
            connector.id,
            'canvas',
            connector.component.managedProcessGroupId
        ]);
    });

    it('should dispatch loadConnectorEntitySuccess on success', async () => {
        const connector = createMockConnector();
        const { dispatchSpy } = await setup({ connectorResponse: connector });
        const route = { params: { id: connector.id }, parent: null } as ActivatedRouteSnapshot;

        await firstValueFrom(TestBed.runInInjectionContext(() => connectorCanvasRootGuard(route)));

        expect(dispatchSpy).toHaveBeenCalledWith(loadConnectorEntitySuccess({ connectorEntity: connector }));
    });

    it('should dispatch loadConnectorEntityFailure on API error', async () => {
        const httpError = new HttpErrorResponse({
            status: 404,
            statusText: 'Not Found',
            error: { message: 'Missing connector' }
        });
        const { dispatchSpy, mockErrorHelper, mockRouter } = await setup({ connectorError: httpError });
        const route = { params: { id: 'missing-id' }, parent: null } as ActivatedRouteSnapshot;

        const result = await firstValueFrom(TestBed.runInInjectionContext(() => connectorCanvasRootGuard(route)));

        expect(mockErrorHelper.getErrorString).toHaveBeenCalledWith(httpError);
        expect(dispatchSpy).toHaveBeenCalledWith(loadConnectorEntityFailure({ error: 'Connector load failed' }));
        expect(result).toBe(true);
        expect(mockRouter.navigate).not.toHaveBeenCalled();
    });

    it('should find connectorId from parent route params', async () => {
        const connector = createMockConnector();
        const { mockConnectorService, mockRouter } = await setup({ connectorResponse: connector });
        const parent = { params: { id: connector.id }, parent: null } as ActivatedRouteSnapshot;
        const route = { params: {}, parent } as ActivatedRouteSnapshot;

        await firstValueFrom(TestBed.runInInjectionContext(() => connectorCanvasRootGuard(route)));

        expect(mockConnectorService.getConnector).toHaveBeenCalledWith(connector.id);
        expect(mockRouter.navigate).toHaveBeenCalledWith([
            '/connectors',
            connector.id,
            'canvas',
            connector.component.managedProcessGroupId
        ]);
    });
});
