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
import { Observable, of } from 'rxjs';
import { ControllerServiceEntity } from '@nifi/shared';
import { AdvancedUiParams } from '../../../../state/shared';
import { Client } from '../../../../service/client.service';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { fullScreenError } from '../../../../state/error/error.actions';
import { settingsFeatureKey } from '../../state';
import { managementControllerServicesFeatureKey } from '../../state/management-controller-services';
import { ManagementControllerServiceService } from '../management-controller-service.service';
import { controllerServiceAdvancedUiParamsResolver } from './controller-service-advanced-ui-params.resolver';

function service(component: ControllerServiceEntity['component'], canWrite = true): ControllerServiceEntity {
    return {
        id: 'service-1',
        revision: { clientId: 'client-1', version: 1 },
        permissions: { canRead: component != null, canWrite },
        operatePermissions: { canRead: true, canWrite: true },
        status: { runStatus: 'DISABLED', validationStatus: 'VALID' },
        component
    } as ControllerServiceEntity;
}

async function setup(fetchedService: ControllerServiceEntity) {
    const managementControllerServiceService = {
        getControllerService: vi.fn().mockReturnValue(of(fetchedService))
    };

    await TestBed.configureTestingModule({
        providers: [
            { provide: ManagementControllerServiceService, useValue: managementControllerServiceService },
            { provide: Client, useValue: { getRevision: () => fetchedService.revision } },
            { provide: ClusterConnectionService, useValue: { isDisconnectionAcknowledged: () => false } },
            provideMockStore({
                initialState: {
                    [settingsFeatureKey]: {
                        [managementControllerServicesFeatureKey]: { controllerServices: [] }
                    }
                }
            })
        ]
    }).compileComponents();

    const store = TestBed.inject(MockStore);
    return { dispatch: vi.spyOn(store, 'dispatch') };
}

function resolve(): Observable<AdvancedUiParams> {
    const route = {
        paramMap: convertToParamMap({ id: 'service-1' })
    } as unknown as ActivatedRouteSnapshot;
    return TestBed.runInInjectionContext(() =>
        controllerServiceAdvancedUiParamsResolver(route, {} as RouterStateSnapshot)
    ) as Observable<AdvancedUiParams>;
}

describe('controllerServiceAdvancedUiParamsResolver', () => {
    it.each([
        ['an unreadable service', service(undefined)],
        ['a service without a custom UI URL', service({ id: 'service-1' } as ControllerServiceEntity['component'])]
    ])('dispatches fullScreenError and emits no value for %s', async (_description, fetchedService) => {
        const { dispatch } = await setup(fetchedService);
        const emitted: AdvancedUiParams[] = [];

        resolve()
            .subscribe({ next: (value) => emitted.push(value) })
            .unsubscribe();

        expect(emitted).toHaveLength(0);
        expect(dispatch).toHaveBeenCalledWith(
            fullScreenError({
                errorDetail: {
                    title: 'Unable to Open Advanced UI',
                    message: 'The Controller Service does not provide an Advanced UI.'
                }
            })
        );
    });

    it('requires write permission for an editable Advanced UI', async () => {
        await setup(
            service({ id: 'service-1', customUiUrl: '/advanced' } as ControllerServiceEntity['component'], false)
        );

        const result = await new Promise<AdvancedUiParams>((resolveResult) => {
            resolve().subscribe({ next: resolveResult });
        });

        expect(result.editable).toBe(false);
    });
});
