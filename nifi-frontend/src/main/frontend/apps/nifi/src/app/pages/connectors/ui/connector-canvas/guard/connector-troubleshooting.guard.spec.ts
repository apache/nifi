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
import { ActivatedRouteSnapshot, convertToParamMap, Router, RouterStateSnapshot, UrlTree } from '@angular/router';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { ConnectorEntity } from '@nifi/shared';
import { firstValueFrom, isObservable, Observable, of } from 'rxjs';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ConnectorService } from '../../../service/connector.service';
import { ErrorHelper } from '../../../../../service/error-helper.service';
import { selectConnectorCanvasEntity } from '../../../state/connector-canvas-entity/connector-canvas-entity.selectors';
import { connectorTroubleshootingGuard } from './connector-troubleshooting.guard';

function connector(state = 'TROUBLESHOOTING'): ConnectorEntity {
    return {
        id: 'connector-1',
        component: { id: 'connector-1', state }
    } as ConnectorEntity;
}

async function setup(cachedEntity: ConnectorEntity | null, response = connector()) {
    const connectorService = { getConnector: vi.fn().mockReturnValue(of(response)) };
    const router = {
        createUrlTree: vi.fn((commands: unknown[]) => commands as unknown as UrlTree)
    };
    await TestBed.configureTestingModule({
        providers: [
            provideMockStore({ selectors: [{ selector: selectConnectorCanvasEntity, value: cachedEntity }] }),
            { provide: ConnectorService, useValue: connectorService },
            { provide: Router, useValue: router },
            { provide: ErrorHelper, useValue: { getErrorString: vi.fn(() => 'failed') } }
        ]
    }).compileComponents();
    const route = {
        paramMap: convertToParamMap({ id: 'connector-1', processGroupId: 'pg-1' }),
        data: {}
    } as ActivatedRouteSnapshot;
    return { connectorService, router, route, store: TestBed.inject(MockStore) };
}

async function run(route: ActivatedRouteSnapshot): Promise<boolean | UrlTree> {
    const result = TestBed.runInInjectionContext(() => connectorTroubleshootingGuard(route, {} as RouterStateSnapshot));
    return isObservable(result)
        ? firstValueFrom(result as Observable<boolean | UrlTree>)
        : Promise.resolve(result as boolean | UrlTree);
}

describe('connectorTroubleshootingGuard', () => {
    beforeEach(() => {
        TestBed.resetTestingModule();
        vi.clearAllMocks();
    });

    it('allows a matching cached troubleshooting connector', async () => {
        const { connectorService, route } = await setup(connector());

        expect(await run(route)).toBe(true);
        expect(connectorService.getConnector).not.toHaveBeenCalled();
    });

    it('redirects a matching connector outside troubleshooting', async () => {
        const { router, route } = await setup(connector('RUNNING'));

        expect(await run(route)).toBe(router.createUrlTree.mock.results[0].value);
        expect(router.createUrlTree).toHaveBeenCalledWith(['/connectors', 'connector-1', 'canvas', 'pg-1']);
    });

    it('loads a connector for a cold deep link', async () => {
        const { connectorService, route, store } = await setup(null);
        const dispatch = vi.spyOn(store, 'dispatch');

        expect(await run(route)).toBe(true);
        expect(connectorService.getConnector).toHaveBeenCalledWith('connector-1');
        expect(dispatch).toHaveBeenCalled();
    });
});
