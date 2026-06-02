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
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { AccessPolicyService } from './access-policy.service';
import { Action, ResourceAction } from '../state/shared';
import { Client } from '../../../service/client.service';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';

describe('AccessPolicyService', () => {
    let service: AccessPolicyService;

    beforeEach(() => {
        TestBed.configureTestingModule({
            providers: [
                AccessPolicyService,
                provideHttpClient(),
                provideHttpClientTesting(),
                {
                    provide: Client,
                    useValue: {
                        getClientId: () => 'client-id',
                        getRevision: (entity: { revision: unknown }) => entity.revision
                    }
                },
                {
                    provide: ClusterConnectionService,
                    useValue: { isDisconnectionAcknowledged: () => false }
                }
            ]
        });

        service = TestBed.inject(AccessPolicyService);
    });

    describe('buildResourcePath', () => {
        function buildPath(resourceAction: ResourceAction): string {
            return service.buildResourcePath(resourceAction);
        }

        it('should transform connectors data policy to /data/connectors', () => {
            expect(
                buildPath({
                    resource: 'connectors',
                    resourceIdentifier: 'data',
                    action: Action.Read
                })
            ).toBe('/data/connectors');
        });

        it('should transform connectors provenance-data policy to /provenance-data/connectors', () => {
            expect(
                buildPath({
                    resource: 'connectors',
                    resourceIdentifier: 'provenance-data',
                    action: Action.Read
                })
            ).toBe('/provenance-data/connectors');
        });

        it('should build global connectors resource path', () => {
            expect(
                buildPath({
                    resource: 'connectors',
                    action: Action.Read
                })
            ).toBe('/connectors');
        });

        it('should build per-instance connector resource path', () => {
            expect(
                buildPath({
                    resource: 'connectors',
                    resourceIdentifier: 'connector-1',
                    action: Action.Write
                })
            ).toBe('/connectors/connector-1');
        });

        it('should pass through non-connector resources unchanged', () => {
            expect(
                buildPath({
                    resource: 'processors',
                    resourceIdentifier: 'processor-1',
                    action: Action.Read
                })
            ).toBe('/processors/processor-1');
        });
    });
});
