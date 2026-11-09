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
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { firstValueFrom, Observable } from 'rxjs';
import { ControllerServiceEntity, UnsafeApiPathError } from '@nifi/shared';
import { Client } from '../../../service/client.service';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { ManagementControllerServiceService } from './management-controller-service.service';

describe('ManagementControllerServiceService', () => {
    async function setup() {
        TestBed.configureTestingModule({
            providers: [
                ManagementControllerServiceService,
                provideHttpClient(),
                provideHttpClientTesting(),
                { provide: Client, useValue: { getRevision: () => ({ version: 1 }) } },
                { provide: ClusterConnectionService, useValue: { isDisconnectionAcknowledged: () => false } }
            ]
        });

        return {
            service: TestBed.inject(ManagementControllerServiceService),
            httpTesting: TestBed.inject(HttpTestingController)
        };
    }

    afterEach(() => TestBed.inject(HttpTestingController).verify());

    it.each([
        [
            'getControllerService',
            (service: ManagementControllerServiceService) => service.getControllerService('../bad')
        ],
        [
            'getPropertyDescriptor',
            (service: ManagementControllerServiceService) => service.getPropertyDescriptor('../bad', 'name', false)
        ],
        [
            'updateControllerService',
            (service: ManagementControllerServiceService) =>
                service.updateControllerService({ id: '../bad', payload: {} } as never)
        ],
        [
            'deleteControllerService',
            (service: ManagementControllerServiceService) =>
                service.deleteControllerService({
                    controllerService: { id: '../bad', revision: { version: 1 } } as ControllerServiceEntity
                })
        ],
        [
            'clearBulletins',
            (service: ManagementControllerServiceService) =>
                service.clearBulletins({ id: '../bad', fromTimestamp: '2026-11-06T00:00:00Z' })
        ]
    ] as const)('rejects an unsafe id in %s before issuing a request', async (_name, invoke) => {
        const { service, httpTesting } = await setup();

        await expect(firstValueFrom(invoke(service) as Observable<unknown>)).rejects.toBeInstanceOf(UnsafeApiPathError);
        httpTesting.expectNone(() => true);
    });
});
