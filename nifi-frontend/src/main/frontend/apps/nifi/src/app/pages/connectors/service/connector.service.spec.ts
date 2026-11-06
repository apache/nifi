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
import { ConnectorEntity, UnsafeApiPathError } from '@nifi/shared';
import { Observable } from 'rxjs';
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
                { provide: Client, useValue: { getRevision: vi.fn() } },
                { provide: ClusterConnectionService, useValue: { isDisconnectionAcknowledged: vi.fn() } }
            ]
        }).compileComponents();

        return {
            service: TestBed.inject(ConnectorService),
            http: TestBed.inject(HttpTestingController),
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

    it.each([
        ['collection connector', '../connector', 'group', undefined],
        ['collection process group', 'connector', '%2e%2e', undefined],
        ['single service connector', '..%2Fconnector', undefined, 'service'],
        ['single service id', 'connector', undefined, '../service']
    ])('should reject traversal in %s without issuing HTTP', async (_label, connectorId, processGroupId, serviceId) => {
        const { service, http } = await setup();
        let error: unknown;

        const request$: Observable<unknown> = serviceId
            ? service.getControllerService(connectorId, serviceId)
            : service.getConnectorControllerServices(connectorId, processGroupId!);
        request$.subscribe({ error: (responseError: unknown) => (error = responseError) });

        expect(error).toBeInstanceOf(UnsafeApiPathError);
        http.expectNone(() => true);
    });

    it.each<[string, (service: ConnectorService, connector: ConnectorEntity) => Observable<unknown>]>([
        ['updateConnector', (service, connector) => service.updateConnector(connector)],
        [
            'changeConnectorVersion',
            (service, connector) =>
                service.changeConnectorVersion({
                    id: connector.id,
                    uri: connector.uri,
                    payload: {
                        revision: connector.revision,
                        component: { id: connector.id, bundle: connector.component.bundle }
                    }
                })
        ],
        ['deleteConnector', (service, connector) => service.deleteConnector(connector)],
        ['updateConnectorRunStatus', (service, connector) => service.updateConnectorRunStatus(connector, 'RUNNING')],
        [
            'discardConnectorWorkingConfiguration',
            (service, connector) => service.discardConnectorWorkingConfiguration(connector)
        ],
        ['drainConnector', (service, connector) => service.drainConnector(connector)],
        ['cancelConnectorDrain', (service, connector) => service.cancelConnectorDrain(connector)],
        ['searchConnector', (service, connector) => service.searchConnector(connector.id, 'query')],
        ['createPurgeRequest', (service, connector) => service.createPurgeRequest(connector.id)],
        ['getPurgeRequest', (service, connector) => service.getPurgeRequest(connector.id, 'purge-request')],
        ['deletePurgeRequest', (service, connector) => service.deletePurgeRequest(connector.id, 'purge-request')]
    ])('should reject a path-traversal Connector identifier in %s without issuing HTTP', async (_method, call) => {
        const { service, http } = await setup();
        const connector = {
            id: '../../controller',
            uri: '',
            revision: { version: 0 },
            component: { bundle: { group: 'org.test', artifact: 'test', version: '1.0' } }
        } as ConnectorEntity;
        let error: unknown;

        call(service, connector).subscribe({ error: (responseError: unknown) => (error = responseError) });

        expect(error).toBeInstanceOf(UnsafeApiPathError);
        http.expectNone(() => true);
    });

    it.each(['getPurgeRequest', 'deletePurgeRequest'] as const)(
        'should reject a path-traversal purge request identifier in %s without issuing HTTP',
        async (method) => {
            const { service, http } = await setup();
            let error: unknown;

            service[method]('connector', '../purge').subscribe({
                error: (responseError: unknown) => (error = responseError)
            });

            expect(error).toBeInstanceOf(UnsafeApiPathError);
            http.expectNone(() => true);
        }
    );
});
