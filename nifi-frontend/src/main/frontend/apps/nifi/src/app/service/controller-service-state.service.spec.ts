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
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { provideHttpClient } from '@angular/common/http';
import { ControllerServiceEntity, ControllerServiceReferencingComponentEntity, UnsafeApiPathError } from '@nifi/shared';
import { firstValueFrom, Observable } from 'rxjs';
import { ControllerServiceStateService } from './controller-service-state.service';
import { Client } from './client.service';
import { ClusterConnectionService } from './cluster-connection.service';

describe('ControllerServiceStateService', () => {
    function createReferencingEntity(
        id: string,
        referenceType: string,
        children: ControllerServiceReferencingComponentEntity[] = [],
        options: { referenceCycle?: boolean; omitReferencingComponents?: boolean; version?: number } = {}
    ): ControllerServiceReferencingComponentEntity {
        return {
            id,
            revision: { version: options.version ?? 1 },
            permissions: { canRead: true, canWrite: true },
            operatePermissions: { canRead: true, canWrite: true },
            bulletins: [],
            component: {
                groupId: 'group-1',
                id,
                name: id,
                type: referenceType,
                state: 'ENABLED',
                properties: {},
                descriptors: {},
                validationErrors: [],
                referenceType,
                referenceCycle: options.referenceCycle ?? false,
                ...(options.omitReferencingComponents ? {} : { referencingComponents: children })
            }
        };
    }

    // the server omits referencingComponents entirely on leaf components
    function createLeafEntity(id: string, referenceType: string): ControllerServiceReferencingComponentEntity {
        return createReferencingEntity(id, referenceType, [], { omitReferencingComponents: true });
    }

    function createControllerServiceEntity(
        referencingComponents: ControllerServiceReferencingComponentEntity[]
    ): ControllerServiceEntity {
        return {
            id: 'cs-root',
            uri: '../nifi-api/controller-services/cs-root',
            permissions: { canRead: true, canWrite: true },
            operatePermissions: { canRead: true, canWrite: true },
            revision: { version: 1 },
            bulletins: [],
            status: { runStatus: 'ENABLED', validationStatus: 'VALID' },
            component: { id: 'cs-root', referencingComponents }
        } as unknown as ControllerServiceEntity;
    }

    async function setup() {
        TestBed.configureTestingModule({
            providers: [
                ControllerServiceStateService,
                provideHttpClient(),
                provideHttpClientTesting(),
                {
                    provide: Client,
                    useValue: {
                        getRevision: (entity: ControllerServiceReferencingComponentEntity) => ({
                            clientId: 'client-1',
                            version: entity.revision.version
                        })
                    }
                },
                { provide: ClusterConnectionService, useValue: { isDisconnectionAcknowledged: () => false } }
            ]
        });

        const service = TestBed.inject(ControllerServiceStateService);
        const httpTesting = TestBed.inject(HttpTestingController);

        return { service, httpTesting };
    }

    afterEach(() => {
        TestBed.inject(HttpTestingController).verify();
    });

    describe('path safety', () => {
        it.each([
            [
                'getControllerService',
                (service: ControllerServiceStateService) => service.getControllerService('../bad')
            ],
            [
                'setEnable',
                (service: ControllerServiceStateService) =>
                    service.setEnable({ ...createControllerServiceEntity([]), id: '../bad' }, true)
            ],
            [
                'updateReferencingServices',
                (service: ControllerServiceStateService) =>
                    service.updateReferencingServices({ ...createControllerServiceEntity([]), id: '../bad' }, true)
            ],
            [
                'updateReferencingSchedulableComponents',
                (service: ControllerServiceStateService) =>
                    service.updateReferencingSchedulableComponents(
                        { ...createControllerServiceEntity([]), id: '../bad' },
                        true
                    )
            ]
        ] as const)('rejects an unsafe id in %s before issuing a request', async (_name, invoke) => {
            const { service, httpTesting } = await setup();

            await expect(firstValueFrom(invoke(service) as Observable<unknown>)).rejects.toBeInstanceOf(
                UnsafeApiPathError
            );
            httpTesting.expectNone(() => true);
        });
    });

    describe('updateReferencingServices', () => {
        it('includes nested Controller Services and omits processors', async () => {
            const { service, httpTesting } = await setup();
            const controllerService = createControllerServiceEntity([
                createReferencingEntity('cs-1', 'ControllerService', [
                    createReferencingEntity('cs-2', 'ControllerService'),
                    createLeafEntity('proc-1', 'Processor')
                ])
            ]);

            service.updateReferencingServices(controllerService, true).subscribe();

            const req = httpTesting.expectOne('../nifi-api/controller-services/cs-root/references');
            expect(Object.keys(req.request.body.referencingComponentRevisions).sort()).toEqual(['cs-1', 'cs-2']);
            req.flush({});
        });

        it('includes a reference cycle node whose children the server omits', async () => {
            const { service, httpTesting } = await setup();
            const cycleNode = createReferencingEntity('cs-2', 'ControllerService', [], {
                referenceCycle: true,
                omitReferencingComponents: true,
                version: 7
            });
            const controllerService = createControllerServiceEntity([
                createReferencingEntity('cs-1', 'ControllerService', [cycleNode])
            ]);

            service.updateReferencingServices(controllerService, true).subscribe();

            const req = httpTesting.expectOne('../nifi-api/controller-services/cs-root/references');
            expect(Object.keys(req.request.body.referencingComponentRevisions).sort()).toEqual(['cs-1', 'cs-2']);
            expect(req.request.body.referencingComponentRevisions['cs-2']).toEqual({
                clientId: 'client-1',
                version: 7
            });
            req.flush({});
        });

        it('includes a reference cycle node but does not walk children it still carries', async () => {
            const { service, httpTesting } = await setup();
            const cycleNode = createReferencingEntity(
                'cs-2',
                'ControllerService',
                [createReferencingEntity('cs-3', 'ControllerService'), createLeafEntity('proc-1', 'Processor')],
                { referenceCycle: true, version: 7 }
            );
            const controllerService = createControllerServiceEntity([
                createReferencingEntity('cs-1', 'ControllerService', [cycleNode])
            ]);

            service.updateReferencingServices(controllerService, true).subscribe();

            const req = httpTesting.expectOne('../nifi-api/controller-services/cs-root/references');
            expect(Object.keys(req.request.body.referencingComponentRevisions).sort()).toEqual(['cs-1', 'cs-2']);
            expect(req.request.body.referencingComponentRevisions['cs-2']).toEqual({
                clientId: 'client-1',
                version: 7
            });
            req.flush({});
        });
    });

    describe('updateReferencingSchedulableComponents', () => {
        it('includes processors and reporting tasks beneath nested Controller Services', async () => {
            const { service, httpTesting } = await setup();
            const controllerService = createControllerServiceEntity([
                createReferencingEntity('cs-1', 'ControllerService', [
                    createReferencingEntity('cs-2', 'ControllerService', [createLeafEntity('proc-1', 'Processor')]),
                    createLeafEntity('rt-1', 'ReportingTask')
                ])
            ]);

            service.updateReferencingSchedulableComponents(controllerService, false).subscribe();

            const req = httpTesting.expectOne('../nifi-api/controller-services/cs-root/references');
            expect(Object.keys(req.request.body.referencingComponentRevisions).sort()).toEqual(['proc-1', 'rt-1']);
            req.flush({});
        });

        it('does not walk children a reference cycle node still carries', async () => {
            const { service, httpTesting } = await setup();
            const cycleNode = createReferencingEntity(
                'cs-2',
                'ControllerService',
                [createLeafEntity('proc-1', 'Processor')],
                { referenceCycle: true }
            );
            const controllerService = createControllerServiceEntity([
                createReferencingEntity('cs-1', 'ControllerService', [cycleNode]),
                createLeafEntity('proc-2', 'Processor')
            ]);

            service.updateReferencingSchedulableComponents(controllerService, false).subscribe();

            const req = httpTesting.expectOne('../nifi-api/controller-services/cs-root/references');
            expect(Object.keys(req.request.body.referencingComponentRevisions).sort()).toEqual(['proc-2']);
            req.flush({});
        });
    });
});
