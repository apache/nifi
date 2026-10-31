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
import { ProcessorEntity } from '@nifi/shared';
import { firstValueFrom, Observable, of } from 'rxjs';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FlowService } from '../../../flow-designer/service/flow.service';
import { Client } from '../../../../service/client.service';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { connectorProcessorAdvancedUiParamsResolver } from './connector-processor-advanced-ui-params.resolver';

function processor(parentGroupId = 'pg-1', customUiUrl = '/advanced'): ProcessorEntity {
    return {
        id: 'processor-1',
        revision: { version: 2, clientId: 'client-1' },
        permissions: { canRead: true, canWrite: true },
        component: {
            parentGroupId,
            extensionMissing: false,
            config: { customUiUrl }
        },
        status: { aggregateSnapshot: { runStatus: 'Stopped', activeThreadCount: 0 } }
    } as ProcessorEntity;
}

async function setup(entity: ProcessorEntity | undefined) {
    const flowService = { getProcessor: vi.fn().mockReturnValue(of(processor())) };
    await TestBed.configureTestingModule({
        providers: [
            provideMockStore({
                initialState: {
                    connectorCanvas: {
                        processors: entity ? [entity] : []
                    }
                }
            }),
            { provide: FlowService, useValue: flowService },
            { provide: Client, useValue: { getRevision: vi.fn(() => ({ version: 2, clientId: 'client-1' })) } },
            {
                provide: ClusterConnectionService,
                useValue: { isDisconnectionAcknowledged: vi.fn(() => false) }
            },
            { provide: ErrorHelper, useValue: { getErrorString: vi.fn(() => 'failed') } }
        ]
    }).compileComponents();
    return { flowService, store: TestBed.inject(MockStore) };
}

async function resolve(route: ActivatedRouteSnapshot) {
    const result = TestBed.runInInjectionContext(() =>
        connectorProcessorAdvancedUiParamsResolver(route, {} as RouterStateSnapshot)
    );
    return firstValueFrom(result as Observable<unknown>, { defaultValue: null });
}

function route(processGroupId = 'pg-1'): ActivatedRouteSnapshot {
    return {
        paramMap: convertToParamMap({ processorId: 'processor-1', processGroupId })
    } as ActivatedRouteSnapshot;
}

describe('connectorProcessorAdvancedUiParamsResolver', () => {
    beforeEach(() => {
        TestBed.resetTestingModule();
        vi.clearAllMocks();
    });

    it('resolves a stored processor in the routed process group', async () => {
        const { flowService } = await setup(processor());

        expect(await resolve(route())).toMatchObject({ id: 'processor-1', url: '/advanced', editable: true });
        expect(flowService.getProcessor).not.toHaveBeenCalled();
    });

    it('loads a processor when it is not in connector canvas state', async () => {
        const { flowService } = await setup(undefined);

        expect(await resolve(route())).toMatchObject({ id: 'processor-1', url: '/advanced' });
        expect(flowService.getProcessor).toHaveBeenCalledWith('processor-1');
    });

    it('rejects a processor outside the routed process group', async () => {
        const { store } = await setup(processor('other-pg'));
        const dispatch = vi.spyOn(store, 'dispatch');

        expect(await resolve(route())).toBeNull();
        expect(dispatch).toHaveBeenCalled();
    });

    it('rejects a processor without an available Advanced UI', async () => {
        const missingExtension = processor();
        missingExtension.component!.extensionMissing = true;
        await setup(missingExtension);

        expect(await resolve(route())).toBeNull();
    });

    it('rejects a processor with an empty custom UI URL', async () => {
        await setup(processor('pg-1', ''));

        expect(await resolve(route())).toBeNull();
    });

    it('marks the Advanced UI read-only without processor write permission', async () => {
        const readOnly = processor();
        readOnly.permissions.canWrite = false;
        await setup(readOnly);

        expect(await resolve(route())).toMatchObject({ editable: false });
    });
});
