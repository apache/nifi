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

    it('posts only the dialog create payload when creating a connection', () => {
        const { service, http } = setup();
        const payload = {
            revision: { version: 0, clientId: 'local-client' },
            disconnectedNodeAcknowledged: true,
            component: {
                name: '',
                labelIndex: 0,
                source: { id: 'source-1', groupId: 'pg-1', type: 'FUNNEL' as const },
                destination: { id: 'destination-1', groupId: 'pg-1', type: 'FUNNEL' as const }
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
});
