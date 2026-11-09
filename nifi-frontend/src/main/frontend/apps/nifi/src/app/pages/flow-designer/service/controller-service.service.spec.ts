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

import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { UnsafeApiPathError } from '@nifi/shared';
import { Observable } from 'rxjs';
import { Client } from '../../../service/client.service';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { ControllerServiceService } from './controller-service.service';

describe('ControllerServiceService', () => {
    async function setup() {
        await TestBed.configureTestingModule({
            providers: [
                ControllerServiceService,
                provideHttpClient(),
                provideHttpClientTesting(),
                { provide: Client, useValue: { getRevision: () => ({ version: 1 }) } },
                {
                    provide: ClusterConnectionService,
                    useValue: { isDisconnectionAcknowledged: () => false }
                }
            ]
        }).compileComponents();

        return {
            service: TestBed.inject(ControllerServiceService),
            http: TestBed.inject(HttpTestingController)
        };
    }

    it('should reject traversal in Controller Service route segments without issuing HTTP', async () => {
        const { service, http } = await setup();
        const unsafeId = '..%2Fcontroller-services';
        const requests: Array<() => Observable<unknown>> = [
            () => service.getControllerServices(unsafeId),
            () => service.getFlow(unsafeId),
            () => service.getControllerService(unsafeId),
            () =>
                service.createControllerService({
                    processGroupId: unsafeId,
                    controllerServiceType: 'org.apache.nifi.Service',
                    controllerServiceBundle: { group: 'g', artifact: 'a', version: '1' },
                    revision: { version: 0 }
                }),
            () => service.getPropertyDescriptor(unsafeId, 'property', false),
            () =>
                service.updateControllerService({
                    id: unsafeId,
                    uri: `/controller-services/${unsafeId}`,
                    payload: { revision: { version: 1 }, component: { id: unsafeId } }
                }),
            () => service.clearBulletins({ id: unsafeId, fromTimestamp: '2023-04-01T00:00:00Z' }),
            () =>
                service.deleteControllerService({
                    controllerService: {
                        id: unsafeId,
                        uri: `/controller-services/${unsafeId}`,
                        revision: { version: 1 },
                        permissions: { canRead: true, canWrite: true },
                        operatePermissions: { canRead: true, canWrite: true },
                        parentGroupId: 'pg-1',
                        status: { runStatus: 'DISABLED', validationStatus: 'VALID' },
                        bulletins: [],
                        component: {
                            id: unsafeId,
                            parentGroupId: 'pg-1',
                            name: 'Service',
                            type: 'org.apache.nifi.Service',
                            bundle: { group: 'g', artifact: 'a', version: '1' },
                            controllerServiceApis: [],
                            state: 'DISABLED',
                            bulletinLevel: 'WARN',
                            persistsState: false,
                            restricted: false,
                            deprecated: false,
                            extensionMissing: false,
                            multipleVersionsAvailable: false,
                            supportsSensitiveDynamicProperties: false,
                            properties: {},
                            descriptors: {},
                            validationStatus: 'VALID'
                        }
                    }
                })
        ];

        for (const request of requests) {
            let error: unknown;
            request().subscribe({ error: (responseError) => (error = responseError) });
            expect(error).toBeInstanceOf(UnsafeApiPathError);
        }
        http.expectNone(() => true);
    });
});
