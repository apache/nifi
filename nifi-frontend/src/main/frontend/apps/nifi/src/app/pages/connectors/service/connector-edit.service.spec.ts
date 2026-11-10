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
import { ComponentType, NiFiCommon } from '@nifi/shared';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { ConnectionCreateRequest } from '../../../state/flow-shared';
import { ConnectorEditService } from './connector-edit.service';

describe('ConnectorEditService', () => {
    function setup() {
        const clusterConnectionService = {
            isDisconnectionAcknowledged: vi.fn().mockReturnValue(true)
        };
        const nifiCommon = {
            getComponentTypeApiPath: vi.fn().mockReturnValue('processors')
        };
        TestBed.configureTestingModule({
            providers: [
                ConnectorEditService,
                provideHttpClient(),
                provideHttpClientTesting(),
                { provide: ClusterConnectionService, useValue: clusterConnectionService },
                { provide: NiFiCommon, useValue: nifiCommon }
            ]
        });

        return {
            service: TestBed.inject(ConnectorEditService),
            http: TestBed.inject(HttpTestingController)
        };
    }

    it('updates components through the connector-local REST boundary', () => {
        const { service, http } = setup();
        const payload = { revision: { version: 3 }, component: { id: 'processor-1', position: { x: 1, y: 2 } } };

        service
            .updateComponent({ id: 'processor-1', type: ComponentType.Processor, payload })
            .subscribe((response) => expect(response).toEqual({ id: 'processor-1' }));

        const request = http.expectOne('../nifi-api/processors/processor-1');
        expect(request.request.method).toBe('PUT');
        expect(request.request.body).toEqual(payload);
        request.flush({ id: 'processor-1' });
        http.verify();
    });

    it('fails closed without a request for an unsafe component id on update', () => {
        const { service, http } = setup();
        const error = vi.fn();

        service
            .updateComponent({ id: '../controller', type: ComponentType.Processor, payload: {} })
            .subscribe({ error });

        expect(error).toHaveBeenCalledWith(expect.objectContaining({ name: 'UnsafeApiPathError' }));
        http.verify();
    });

    it('fails closed without a request for an unsafe process group id when creating a connection', () => {
        const { service, http } = setup();
        const error = vi.fn();

        service
            .createConnection('../pg-1', {
                payload: {
                    revision: { version: 0, clientId: 'local-client' },
                    component: {
                        labelIndex: 0,
                        source: { id: 'source-1', groupId: 'pg-1', type: 'FUNNEL' },
                        destination: { id: 'destination-1', groupId: 'pg-1', type: 'FUNNEL' }
                    }
                }
            })
            .subscribe({ error });

        expect(error).toHaveBeenCalledWith(expect.objectContaining({ name: 'UnsafeApiPathError' }));
        http.verify();
    });

    it('posts only the dialog create payload when creating a connection', () => {
        const { service, http } = setup();
        const payload: ConnectionCreateRequest = {
            revision: { version: 0, clientId: 'local-client' },
            disconnectedNodeAcknowledged: true,
            component: {
                name: '',
                labelIndex: 0,
                source: { id: 'source-1', groupId: 'pg-1', type: 'FUNNEL' },
                destination: { id: 'destination-1', groupId: 'pg-1', type: 'FUNNEL' }
            }
        };

        service.createConnection('pg-1', { payload }).subscribe();

        const request = http.expectOne('../nifi-api/process-groups/pg-1/connections');
        expect(request.request.method).toBe('POST');
        expect(request.request.body).toEqual(payload);
        expect(request.request.body).not.toHaveProperty('id');
        expect(request.request.body).not.toHaveProperty('uri');
        expect(request.request.body).not.toHaveProperty('permissions');
        request.flush({ id: 'connection-1' });
        http.verify();
    });

    it('creates and moves a revision-safe snippet locally', () => {
        const { service, http } = setup();

        service
            .moveComponents(
                [
                    {
                        id: 'processor-1',
                        type: ComponentType.Processor,
                        revision: { version: 7, clientId: 'local-client' }
                    }
                ],
                'source-pg',
                'target-pg'
            )
            .subscribe();

        const create = http.expectOne('../nifi-api/snippets');
        expect(create.request.body.snippet.processors).toEqual({
            'processor-1': { version: 7, clientId: 'local-client' }
        });
        expect(create.request.body.disconnectedNodeAcknowledged).toBe(true);
        create.flush({ snippet: { id: 'snippet-1' } });

        const move = http.expectOne('../nifi-api/snippets/snippet-1');
        expect(move.request.body).toEqual({
            disconnectedNodeAcknowledged: true,
            snippet: { id: 'snippet-1', parentGroupId: 'target-pg' }
        });
        move.flush({});
        http.verify();
    });

    describe('createComponent', () => {
        const revision = { clientId: 'local-client', version: 0 as const };
        const position = { x: 10, y: 20 };

        it('creates a processor and tags the result with its type', () => {
            const { service, http } = setup();
            const bundle = { group: 'org.apache.nifi', artifact: 'nifi-standard-nar', version: '2.0.0' };
            let result: unknown;

            service
                .createComponent('pg-1', {
                    type: ComponentType.Processor,
                    revision,
                    position,
                    processorType: 'org.apache.nifi.processors.standard.GenerateFlowFile',
                    processorBundle: bundle
                })
                .subscribe((created) => (result = created));

            const request = http.expectOne('../nifi-api/process-groups/pg-1/processors');
            expect(request.request.method).toBe('POST');
            expect(request.request.body).toEqual({
                revision,
                disconnectedNodeAcknowledged: true,
                component: {
                    position,
                    type: 'org.apache.nifi.processors.standard.GenerateFlowFile',
                    bundle
                }
            });
            request.flush({ id: 'processor-1' });
            expect(result).toEqual({ type: ComponentType.Processor, payload: { id: 'processor-1' } });
            http.verify();
        });

        it.each([
            [ComponentType.InputPort, 'input-ports'],
            [ComponentType.OutputPort, 'output-ports']
        ] as const)('routes a %s to the %s endpoint', (type, endpoint) => {
            const { service, http } = setup();

            service
                .createComponent('pg-1', { type, revision, position, name: 'port', allowRemoteAccess: false })
                .subscribe();

            const request = http.expectOne(`../nifi-api/process-groups/pg-1/${endpoint}`);
            expect(request.request.body.component).toEqual({ position, name: 'port', allowRemoteAccess: false });
            request.flush({ id: 'port-1' });
            http.verify();
        });

        it('includes the parameter context only when one is selected', () => {
            const { service, http } = setup();

            service
                .createComponent('pg-1', {
                    type: ComponentType.ProcessGroup,
                    revision,
                    position,
                    name: 'child',
                    parameterContextId: null
                })
                .subscribe();

            const request = http.expectOne('../nifi-api/process-groups/pg-1/process-groups');
            expect(request.request.body.component).toEqual({ position, name: 'child' });
            request.flush({ id: 'child-pg' });
            http.verify();
        });

        it('creates a remote process group with its transport settings', () => {
            const { service, http } = setup();
            const settings = {
                targetUris: 'https://remote:8443/nifi',
                transportProtocol: 'HTTP',
                localNetworkInterface: '',
                proxyHost: '',
                proxyPort: '',
                proxyUser: '',
                proxyPassword: '',
                communicationsTimeout: '30 sec',
                yieldDuration: '10 sec'
            };

            service
                .createComponent('pg-1', { type: ComponentType.RemoteProcessGroup, revision, position, ...settings })
                .subscribe();

            const request = http.expectOne('../nifi-api/process-groups/pg-1/remote-process-groups');
            expect(request.request.body.component).toEqual({ position, ...settings });
            request.flush({ id: 'rpg-1' });
            http.verify();
        });

        it('creates a funnel and a label with its z-index', () => {
            const { service, http } = setup();

            service.createComponent('pg-1', { type: ComponentType.Funnel, revision, position }).subscribe();
            service.createComponent('pg-1', { type: ComponentType.Label, revision, position, zIndex: 4 }).subscribe();

            const funnel = http.expectOne('../nifi-api/process-groups/pg-1/funnels');
            expect(funnel.request.body.component).toEqual({ position });
            funnel.flush({ id: 'funnel-1' });

            const label = http.expectOne('../nifi-api/process-groups/pg-1/labels');
            expect(label.request.body.component).toEqual({ position, zIndex: 4 });
            label.flush({ id: 'label-1' });
            http.verify();
        });

        it('fails closed without a request for an unsafe process group id', () => {
            const { service, http } = setup();
            const error = vi.fn();

            service.createComponent('../pg-1', { type: ComponentType.Funnel, revision, position }).subscribe({ error });

            expect(error).toHaveBeenCalledWith(expect.objectContaining({ name: 'UnsafeApiPathError' }));
            http.verify();
        });
    });

    it('uploads a flow definition as multipart form data', () => {
        const { service, http } = setup();
        const flowDefinition = new File(['{}'], 'flow.json');

        service
            .uploadProcessGroup('pg-1', {
                type: ComponentType.ProcessGroup,
                revision: { clientId: 'local-client', version: 0 },
                position: { x: 1, y: 2 },
                name: 'uploaded',
                flowDefinition
            })
            .subscribe();

        const request = http.expectOne('../nifi-api/process-groups/pg-1/process-groups/upload');
        const body = request.request.body as FormData;
        expect(body.get('groupName')).toBe('uploaded');
        expect(body.get('positionX')).toBe('1');
        expect(body.get('positionY')).toBe('2');
        expect(body.get('clientId')).toBe('local-client');
        expect(body.get('file')).toBe(flowDefinition);
        request.flush({ id: 'uploaded-pg' });
        http.verify();
    });

    it('gets a single component', () => {
        const { service, http } = setup();
        const loaded = vi.fn();

        service.getComponent(ComponentType.Processor, 'processor-1').subscribe(loaded);

        const request = http.expectOne(
            (req) => req.method === 'GET' && req.url === '../nifi-api/processors/processor-1'
        );
        request.flush({ id: 'processor-1' });
        expect(loaded).toHaveBeenCalledWith({ id: 'processor-1' });
        http.verify();
    });

    it('rejects a component id that traverses the API path without issuing a request', () => {
        const { service, http } = setup();
        const failed = vi.fn();

        service.getComponent(ComponentType.Processor, '../controller').subscribe({ error: failed });

        expect(failed).toHaveBeenCalled();
        http.verify();
    });

    it('deletes a single component with its revision', () => {
        const { service, http } = setup();

        service
            .deleteComponent({
                id: 'processor-1',
                type: ComponentType.Processor,
                revision: { clientId: 'local-client', version: 4 }
            })
            .subscribe();

        const request = http.expectOne(
            (req) => req.method === 'DELETE' && req.url === '../nifi-api/processors/processor-1'
        );
        expect(request.request.params.get('version')).toBe('4');
        expect(request.request.params.get('clientId')).toBe('local-client');
        expect(request.request.params.get('disconnectedNodeAcknowledged')).toBe('true');
        request.flush({});
        http.verify();
    });

    it('deletes several components through a snippet', () => {
        const { service, http } = setup();

        service
            .deleteComponents(
                [
                    { id: 'processor-1', type: ComponentType.Processor, revision: { version: 1 } },
                    { id: 'funnel-1', type: ComponentType.Funnel, revision: { version: 2 } }
                ],
                'pg-1'
            )
            .subscribe();

        const create = http.expectOne('../nifi-api/snippets');
        expect(create.request.body.snippet).toEqual(
            expect.objectContaining({
                parentGroupId: 'pg-1',
                processors: { 'processor-1': { version: 1 } },
                funnels: { 'funnel-1': { version: 2 } }
            })
        );
        create.flush({ snippet: { id: 'snippet-1' } });

        const remove = http.expectOne((req) => req.method === 'DELETE' && req.url === '../nifi-api/snippets/snippet-1');
        expect(remove.request.params.get('disconnectedNodeAcknowledged')).toBe('true');
        remove.flush({});
        http.verify();
    });
});
