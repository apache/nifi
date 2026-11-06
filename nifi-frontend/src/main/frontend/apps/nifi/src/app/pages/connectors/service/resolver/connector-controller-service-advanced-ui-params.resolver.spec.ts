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
import { ActivatedRouteSnapshot, convertToParamMap, RouterStateSnapshot } from '@angular/router';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { Observable, of, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import type { Mock } from 'vitest';
import { AdvancedUiParams } from '../../../../state/shared';
import { connectorControllerServiceAdvancedUiParamsResolver } from './connector-controller-service-advanced-ui-params.resolver';
import { ControllerServiceService } from '../../../flow-designer/service/controller-service.service';
import { Client } from '../../../../service/client.service';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { fullScreenError } from '../../../../state/error/error.actions';
import {
    connectorControllerServicesFeatureKey,
    initialConnectorControllerServicesState
} from '../../state/connector-controller-services';

function createMockService(
    overrides: { runStatus?: string; customUiUrl?: string | null; canWrite?: boolean; readable?: boolean } = {}
) {
    const runStatus = overrides.runStatus ?? 'ENABLED';
    const customUiUrl =
        overrides.customUiUrl === undefined ? 'http://custom-ui/cs-1' : (overrides.customUiUrl ?? undefined);
    return {
        id: 'cs-1',
        uri: '/nifi-api/controller-services/cs-1',
        revision: { clientId: 'client-abc', version: 3 },
        status: { runStatus },
        permissions: { canRead: true, canWrite: overrides.canWrite ?? true },
        component:
            overrides.readable === false
                ? undefined
                : {
                      id: 'cs-1',
                      customUiUrl
                  }
    };
}

function createRouteSnapshot(serviceId: string): ActivatedRouteSnapshot {
    return {
        paramMap: convertToParamMap({ serviceId })
    } as unknown as ActivatedRouteSnapshot;
}

interface SetupOptions {
    serviceInStore?: ReturnType<typeof createMockService> | null;
    controllerServiceServiceEntity?: ReturnType<typeof createMockService> | null;
    controllerServiceServiceError?: HttpErrorResponse | null;
    disconnectedNodeAcknowledged?: boolean;
}

async function setup(options: SetupOptions = {}) {
    const storeService = options.serviceInStore === undefined ? createMockService() : options.serviceInStore;
    const revision = { clientId: 'client-abc', version: 3 };

    const mockControllerServiceService = {
        getControllerService: vi.fn()
    };

    if (options.controllerServiceServiceError) {
        mockControllerServiceService.getControllerService.mockReturnValue(
            throwError(() => options.controllerServiceServiceError)
        );
    } else {
        const fetchedService =
            options.controllerServiceServiceEntity === undefined
                ? createMockService()
                : options.controllerServiceServiceEntity;
        mockControllerServiceService.getControllerService.mockReturnValue(of(fetchedService));
    }

    const mockClient = {
        getRevision: vi.fn().mockReturnValue(revision)
    };

    const mockClusterConnectionService = {
        isDisconnectionAcknowledged: vi.fn().mockReturnValue(options.disconnectedNodeAcknowledged ?? false)
    };

    const mockErrorHelper = {
        getErrorString: vi.fn().mockReturnValue('Network error')
    };

    await TestBed.configureTestingModule({
        providers: [
            { provide: ControllerServiceService, useValue: mockControllerServiceService },
            { provide: Client, useValue: mockClient },
            { provide: ClusterConnectionService, useValue: mockClusterConnectionService },
            { provide: ErrorHelper, useValue: mockErrorHelper },
            provideMockStore({
                initialState: {
                    [connectorControllerServicesFeatureKey]: {
                        ...initialConnectorControllerServicesState,
                        controllerServices: storeService ? [storeService] : []
                    }
                }
            })
        ]
    }).compileComponents();

    const store = TestBed.inject(MockStore);
    const dispatchSpy: Mock = vi.spyOn(store, 'dispatch') as unknown as Mock;

    return {
        store,
        dispatchSpy,
        mockControllerServiceService,
        mockClient,
        mockClusterConnectionService,
        mockErrorHelper
    };
}

describe('connectorControllerServiceAdvancedUiParamsResolver', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    function resolve(route: ActivatedRouteSnapshot): Observable<AdvancedUiParams> {
        return TestBed.runInInjectionContext(() =>
            connectorControllerServiceAdvancedUiParamsResolver(route, {} as unknown as RouterStateSnapshot)
        ) as Observable<AdvancedUiParams>;
    }

    it('resolves AdvancedUiParams from the store cache when the service is already loaded', async () => {
        const { mockControllerServiceService } = await setup({
            serviceInStore: createMockService({ customUiUrl: 'http://custom-ui/cs-1' })
        });

        const result = await new Promise<AdvancedUiParams>((res) => {
            resolve(createRouteSnapshot('cs-1')).subscribe({ next: res });
        });

        expect(result.url).toBe('http://custom-ui/cs-1');
        expect(result.id).toBe('cs-1');
        expect(result.clientId).toBe('client-abc');
        expect(result.revision).toBe(3);
        expect(mockControllerServiceService.getControllerService).not.toHaveBeenCalled();
    });

    it('falls back to ControllerServiceService.getControllerService when the service is not in the store', async () => {
        const { mockControllerServiceService } = await setup({ serviceInStore: null });

        const result = await new Promise<AdvancedUiParams>((res) => {
            resolve(createRouteSnapshot('cs-1')).subscribe({ next: res });
        });

        expect(mockControllerServiceService.getControllerService).toHaveBeenCalledWith('cs-1');
        expect(result.url).toBe('http://custom-ui/cs-1');
    });

    it.each([
        ['DISABLED', true],
        ['ENABLED', false],
        ['ENABLING', false],
        ['DISABLING', false]
    ] as const)('computes editable=%s when runStatus is %s', async (runStatus, expected) => {
        await setup({ serviceInStore: createMockService({ runStatus }) });

        const result = await new Promise<AdvancedUiParams>((res) => {
            resolve(createRouteSnapshot('cs-1')).subscribe({ next: res });
        });

        expect(result.editable).toBe(expected);
    });

    it('computes editable=false for a disabled service without write permission', async () => {
        await setup({ serviceInStore: createMockService({ runStatus: 'DISABLED', canWrite: false }) });

        const result = await new Promise<AdvancedUiParams>((res) => {
            resolve(createRouteSnapshot('cs-1')).subscribe({ next: res });
        });

        expect(result.editable).toBe(false);
    });

    it('passes disconnectedNodeAcknowledged from ClusterConnectionService', async () => {
        await setup({ disconnectedNodeAcknowledged: true });

        const result = await new Promise<AdvancedUiParams>((res) => {
            resolve(createRouteSnapshot('cs-1')).subscribe({ next: res });
        });

        expect(result.disconnectedNodeAcknowledged).toBe(true);
    });

    it('dispatches fullScreenError with "Unable to Open Advanced UI" title on HTTP error and emits no value', async () => {
        const { dispatchSpy } = await setup({
            serviceInStore: null,
            controllerServiceServiceError: new HttpErrorResponse({ status: 404, statusText: 'Not Found' })
        });

        // catchError -> EMPTY collapses synchronously inside switchMap, so we
        // capture any emissions into a sync array and unsubscribe immediately.
        // We assert both that fullScreenError was dispatched AND that no
        // AdvancedUiParams value was emitted on the error path.
        const emittedValues: AdvancedUiParams[] = [];
        resolve(createRouteSnapshot('cs-1'))
            .subscribe({ next: (v) => emittedValues.push(v) })
            .unsubscribe();

        expect(emittedValues).toHaveLength(0);
        expect(dispatchSpy).toHaveBeenCalledWith(
            fullScreenError({
                errorDetail: {
                    title: 'Unable to Open Advanced UI',
                    message: 'Network error'
                }
            })
        );
    });

    it.each([
        ['an unreadable service', createMockService({ readable: false })],
        ['a service without a custom UI URL', createMockService({ customUiUrl: null })]
    ])('dispatches fullScreenError and emits no value for %s', async (_description, service) => {
        const { dispatchSpy } = await setup({ serviceInStore: service });
        const emittedValues: AdvancedUiParams[] = [];

        resolve(createRouteSnapshot('cs-1'))
            .subscribe({ next: (value) => emittedValues.push(value) })
            .unsubscribe();

        expect(emittedValues).toHaveLength(0);
        expect(dispatchSpy).toHaveBeenCalledWith(
            fullScreenError({
                errorDetail: {
                    title: 'Unable to Open Advanced UI',
                    message: 'The Controller Service does not provide an Advanced UI.'
                }
            })
        );
    });

    it('returns EMPTY (no next emission) when the route has a falsy serviceId param', async () => {
        await setup();

        const emittedValues: AdvancedUiParams[] = [];
        await new Promise<void>((res) => {
            resolve(createRouteSnapshot('')).subscribe({ next: (v) => emittedValues.push(v), complete: res });
        });

        expect(emittedValues).toHaveLength(0);
    });
});
