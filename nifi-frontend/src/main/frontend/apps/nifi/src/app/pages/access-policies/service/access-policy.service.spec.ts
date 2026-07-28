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

    describe('createAccessPolicy', () => {
        it('should POST with canonical /data/connectors resource', () => {
            const httpMock = TestBed.inject(HttpTestingController);

            service
                .createAccessPolicy({
                    resource: 'connectors',
                    resourceIdentifier: 'data',
                    action: Action.Read
                })
                .subscribe();

            const req = httpMock.expectOne('../nifi-api/policies');
            expect(req.request.method).toBe('POST');
            expect(req.request.body.component.resource).toBe('/data/connectors');
            expect(req.request.body.component.resourceIdentifier).toBeUndefined();
            req.flush({});
            httpMock.verify();
        });

        it('should POST with canonical /provenance-data/connectors resource', () => {
            const httpMock = TestBed.inject(HttpTestingController);

            service
                .createAccessPolicy({
                    resource: 'connectors',
                    resourceIdentifier: 'provenance-data',
                    action: Action.Read
                })
                .subscribe();

            const req = httpMock.expectOne('../nifi-api/policies');
            expect(req.request.method).toBe('POST');
            expect(req.request.body.component.resource).toBe('/provenance-data/connectors');
            expect(req.request.body.component.resourceIdentifier).toBeUndefined();
            req.flush({});
            httpMock.verify();
        });
    });

    describe('getAccessPolicy', () => {
        it('should GET the canonical /data/connectors URL', () => {
            const httpMock = TestBed.inject(HttpTestingController);

            service
                .getAccessPolicy({
                    resource: 'connectors',
                    resourceIdentifier: 'data',
                    action: Action.Read
                })
                .subscribe();

            httpMock.expectOne('../nifi-api/policies/read/data/connectors').flush({});
            httpMock.verify();
        });

        it('should GET the canonical /provenance-data/connectors URL', () => {
            const httpMock = TestBed.inject(HttpTestingController);

            service
                .getAccessPolicy({
                    resource: 'connectors',
                    resourceIdentifier: 'provenance-data',
                    action: Action.Read
                })
                .subscribe();

            httpMock.expectOne('../nifi-api/policies/read/provenance-data/connectors').flush({});
            httpMock.verify();
        });

        it('should encode an untrusted resourceIdentifier segment', () => {
            const httpMock = TestBed.inject(HttpTestingController);

            service
                .getAccessPolicy({
                    resource: 'processors',
                    resourceIdentifier: 'a b',
                    action: Action.Read
                })
                .subscribe();

            httpMock.expectOne('../nifi-api/policies/read/processors/a%20b').flush({});
            httpMock.verify();
        });

        it('should reject a path-traversal resourceIdentifier without issuing a request', () => {
            const httpMock = TestBed.inject(HttpTestingController);

            let error: unknown;
            service
                .getAccessPolicy({
                    resource: 'processors',
                    resourceIdentifier: '../../controller',
                    action: Action.Read
                })
                .subscribe({ error: (e) => (error = e) });

            // the rejection surfaces through the observable error channel (deferred), and no request is issued
            expect(error).toBeInstanceOf(Error);
            httpMock.verify();
        });
    });

    describe('getPolicyComponent', () => {
        it('should GET the component resource URL with encoded segments', () => {
            const httpMock = TestBed.inject(HttpTestingController);

            service
                .getPolicyComponent({
                    resource: 'processors',
                    resourceIdentifier: 'proc 1',
                    action: Action.Write,
                    policy: 'component'
                })
                .subscribe();

            httpMock.expectOne('../nifi-api/processors/proc%201').flush({});
            httpMock.verify();
        });

        it('should reject a path-traversal resourceIdentifier without issuing a request', () => {
            const httpMock = TestBed.inject(HttpTestingController);

            let error: unknown;
            service
                .getPolicyComponent({
                    resource: 'processors',
                    resourceIdentifier: '..%2F..%2Fnifi-api%2Fcontroller%2Fconfig',
                    action: Action.Write,
                    policy: 'component'
                })
                .subscribe({ error: (e) => (error = e) });

            expect(error).toBeInstanceOf(Error);
            httpMock.verify();
        });
    });
});
