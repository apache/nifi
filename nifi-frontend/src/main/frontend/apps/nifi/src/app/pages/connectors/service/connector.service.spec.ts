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
import { ConnectorEntity } from '@nifi/shared';
import { TestBed } from '@angular/core/testing';
import { UnsafeApiPathError } from '@nifi/shared';
import { Client } from '../../../service/client.service';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { ConnectorService } from './connector.service';

describe('ConnectorService', () => {
    async function setup() {
        await TestBed.configureTestingModule({
            providers: [
                ConnectorService,
                provideHttpClient(),
                provideHttpClientTesting(),
                { provide: Client, useValue: { getRevision: () => ({ version: 3, clientId: 'client-1' }) } },
                { provide: ClusterConnectionService, useValue: { isDisconnectionAcknowledged: () => false } }
            ]
        }).compileComponents();

        return {
            service: TestBed.inject(ConnectorService),
            httpMock: TestBed.inject(HttpTestingController),
            open: vi.spyOn(window, 'open').mockImplementation(() => null)
        };
    }

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('should encode Connector and Process Group identifiers in the download URL', async () => {
        const { service, open } = await setup();

        service.downloadFlow({
            connectorId: 'connector id',
            processGroupId: 'process?group',
            includeReferencedServices: true
        });

        expect(open).toHaveBeenCalledWith(
            '../nifi-api/connectors/connector%20id/process-groups/process%3Fgroup/download?includeReferencedServices=true',
            '_blank',
            'noreferrer'
        );
    });

    it.each([
        ['Connector', '..%2F..%2Fnifi-api%2Fcontroller', 'process-group'],
        ['Process Group', 'connector', '../../controller']
    ])(
        'should reject a path-traversal %s identifier without opening a window',
        async (_segment, connectorId, processGroupId) => {
            const { service, open } = await setup();

            expect(() =>
                service.downloadFlow({
                    connectorId,
                    processGroupId,
                    includeReferencedServices: false
                })
            ).toThrow(UnsafeApiPathError);
            expect(open).not.toHaveBeenCalled();
        }
    );

    describe('troubleshooting lifecycle', () => {
        const connector = { id: 'connector-1' } as ConnectorEntity;

        it('should POST the current revision when entering troubleshooting', async () => {
            const { service, httpMock } = await setup();
            const response = { id: 'connector-1' } as ConnectorEntity;

            let result: ConnectorEntity | undefined;
            service.enterTroubleshooting(connector).subscribe((entity) => (result = entity));

            const req = httpMock.expectOne('../nifi-api/connectors/connector-1/troubleshooting');
            expect(req.request.method).toBe('POST');
            expect(req.request.body).toEqual({
                revision: { version: 3, clientId: 'client-1' },
                disconnectedNodeAcknowledged: false
            });
            req.flush(response);
            expect(result).toEqual(response);
        });

        it('should DELETE with revision query parameters when ending troubleshooting', async () => {
            const { service, httpMock } = await setup();

            service.endTroubleshooting(connector).subscribe();

            const req = httpMock.expectOne((r) => r.url === '../nifi-api/connectors/connector-1/troubleshooting');
            expect(req.request.method).toBe('DELETE');
            expect(req.request.params.get('version')).toBe('3');
            expect(req.request.params.get('clientId')).toBe('client-1');
            expect(req.request.params.get('disconnectedNodeAcknowledged')).toBe('false');
            req.flush({});
        });
    });
});
