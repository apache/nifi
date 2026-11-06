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
                { provide: Client, useValue: {} },
                { provide: ClusterConnectionService, useValue: {} }
            ]
        }).compileComponents();

        return {
            service: TestBed.inject(ConnectorService),
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
});
