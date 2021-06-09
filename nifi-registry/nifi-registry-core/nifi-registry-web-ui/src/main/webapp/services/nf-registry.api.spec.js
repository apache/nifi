/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the 'License'); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { TestBed, inject } from '@angular/core/testing';
import initTestBed from 'nf-registry.testbed-factory';
import NfRegistryApi from 'services/nf-registry.api';
import { HttpTestingController } from '@angular/common/http/testing';
import {
    NfRegistryUsersAdministrationAuthGuard,
    NfRegistryLoginAuthGuard,
    NfRegistryResourcesAuthGuard,
    NfRegistryWorkflowsAdministrationAuthGuard
} from 'services/nf-registry.auth-guard.service';
import { of } from 'rxjs';

var kerbUrl = '../nifi-registry-api/access/token/kerberos';
var oidcUrl = '../nifi-registry-api/access/oidc/exchange';
var tokenVal = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJiYmVuZGVATklGSS5BUEFDSEUuT1JHIiwiaXNzIjoiS2VyYmVyb3NTcG5lZ29JZGVudGl0eVByb3ZpZGVyIiwiYXVkIjoiS2VyYmVyb3NTcG5lZ29JZGVudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYmJlbmRlQE5JRkkuQVBBQ0hFLk9SRyIsImtpZCI6IjQ3NWQwZWEyLTkzZGItNDhiNi05MjcxLTgyOGM3MzQ5ZTFkNiIsImlhdCI6MTUxMjQ4NTY4NywiZXhwIjoxNTEyNTI4ODg3fQ.lkaWPQw1ld7Qqb6-Zu8mAqu6r8mUVHBNP0ZfNpES3rA';

describe('NfRegistry API w/ Angular testing utils', function () {
    let nfRegistryApi;
    let req;
    let reqAgain;

    const providers = [
        NfRegistryUsersAdministrationAuthGuard,
        NfRegistryWorkflowsAdministrationAuthGuard,
        NfRegistryLoginAuthGuard,
        NfRegistryResourcesAuthGuard
    ];

    beforeEach((done) => {
        initTestBed({providers})
            .then(() => {
                nfRegistryApi = TestBed.get(NfRegistryApi);
                done();
            });
    });

    it('should POST to exchange tickets.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.nfStorage, 'setItem').and.callThrough();

        // api call
        nfRegistryApi.ticketExchange().subscribe(function (response) {
            var setItemCall = nfRegistryApi.nfStorage.setItem.calls.first();
            expect(setItemCall.args[1]).toBe('eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJiYmVuZGVATklGSS5BUEFDSEUuT1JHIiwiaXNzIjoiS2VyYmVyb3NTcG5lZ29JZGVudGl0eVByb3ZpZGVyIiwiYXVkIjoiS2VyYmVyb3NTcG5lZ29JZGVudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYmJlbmRlQE5JRkkuQVBBQ0hFLk9SRyIsImtpZCI6IjQ3NWQwZWEyLTkzZGItNDhiNi05MjcxLTgyOGM3MzQ5ZTFkNiIsImlhdCI6MTUxMjQ4NTY4NywiZXhwIjoxNTEyNTI4ODg3fQ.lkaWPQw1ld7Qqb6-Zu8mAqu6r8mUVHBNP0ZfNpES3rA');
            expect(response).toBe('eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJiYmVuZGVATklGSS5BUEFDSEUuT1JHIiwiaXNzIjoiS2VyYmVyb3NTcG5lZ29JZGVudGl0eVByb3ZpZGVyIiwiYXVkIjoiS2VyYmVyb3NTcG5lZ29JZGVudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYmJlbmRlQE5JRkkuQVBBQ0hFLk9SRyIsImtpZCI6IjQ3NWQwZWEyLTkzZGItNDhiNi05MjcxLTgyOGM3MzQ5ZTFkNiIsImlhdCI6MTUxMjQ4NTY4NywiZXhwIjoxNTEyNTI4ODg3fQ.lkaWPQw1ld7Qqb6-Zu8mAqu6r8mUVHBNP0ZfNpES3rA');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/access/token/kerberos');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush('eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJiYmVuZGVATklGSS5BUEFDSEUuT1JHIiwiaXNzIjoiS2VyYmVyb3NTcG5lZ29JZGVudGl0eVByb3ZpZGVyIiwiYXVkIjoiS2VyYmVyb3NTcG5lZ29JZGVudGl0eVByb3ZpZGVyIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiYmJlbmRlQE5JRkkuQVBBQ0hFLk9SRyIsImtpZCI6IjQ3NWQwZWEyLTkzZGItNDhiNi05MjcxLTgyOGM3MzQ5ZTFkNiIsImlhdCI6MTUxMjQ4NTY4NywiZXhwIjoxNTEyNTI4ODg3fQ.lkaWPQw1ld7Qqb6-Zu8mAqu6r8mUVHBNP0ZfNpES3rA');

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should load jwt from local storage.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.nfStorage, 'hasItem').and.callFake(function () {
            return true;
        });
        spyOn(nfRegistryApi.nfStorage, 'getItem').and.callFake(function () {
            return 123;
        });

        // api call
        nfRegistryApi.ticketExchange().subscribe(function (response) {
            expect(response).toBe(123);
        });
    }));

    it('should fail to POST to exchange tickets.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.nfStorage, 'hasItem').and.callFake(function () {
            return false;
        });
        // api call
        nfRegistryApi.ticketExchange().subscribe(function (response) {
            expect(response).toEqual('');
        });

        // the request it made
        req = httpMock.expectOne(kerbUrl);
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush(null, {status: 401, statusText: 'POST exchange tickets mock error'});
        reqAgain = httpMock.expectOne(oidcUrl);
        reqAgain.flush(null, {status: 401, statusText: 'POST exchange tickets mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('ticketExchange should POST to Kerberos, fail, and then use the OIDC endpoint to retrieve a JWT.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.nfStorage, 'setItem').and.callThrough();

        // api call
        nfRegistryApi.ticketExchange().subscribe(function (response) {
            var setItemCall = nfRegistryApi.nfStorage.setItem.calls.first();

            expect(setItemCall.args[1]).toBe(tokenVal);
            expect(response).toBe(tokenVal);
        });

        req = httpMock.expectOne(kerbUrl);
        req.flush(null, {status: 401, statusText: 'POST exchange tickets mock error'});
        reqAgain = httpMock.expectOne(oidcUrl);
        reqAgain.flush(tokenVal);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('ticketExchange should POST to Kerberos and OIDC endpoints and fail to retrieve a JWT.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.nfStorage, 'setItem').and.callThrough();

        // api call
        nfRegistryApi.ticketExchange().subscribe(
            function (response) {
                expect(response).toBe('');
            }
        );

        req = httpMock.expectOne(kerbUrl);
        req.flush(null, {status: 401, statusText: 'POST exchange tickets mock error'});
        reqAgain = httpMock.expectOne(oidcUrl);
        reqAgain.flush(null, {status: 401, statusText: 'POST exchange tickets mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('ticketExchange should POST to Kerberos to retrieve a JWT.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.nfStorage, 'setItem').and.callThrough();

        // api call
        nfRegistryApi.ticketExchange().subscribe(function (response) {
            console.log('ticketExchange() response is: '.concat(response));
            var setItemCall = nfRegistryApi.nfStorage.setItem.calls.first();

            expect(setItemCall.args[1]).toBe(tokenVal);
            expect(response).toBe(tokenVal);
        });

        req = httpMock.expectOne(kerbUrl);
        req.flush(tokenVal);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET to load the current user.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.loadCurrentUser().subscribe(function (response) {
            expect(response.identifier).toBe(123);
            expect(response.identity).toBe('Admin User');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/access');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            'identifier': 123,
            'identity': 'Admin User'
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET droplet snapshot metadata.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getDropletSnapshotMetadata('flow/test').subscribe(function (response) {
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/flow/test/versions');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            snapshotMetadata: [
                {identifier: '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc', version: 999}
            ]
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET droplet snapshot metadata.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.getDropletSnapshotMetadata('flow/test').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/flow/test/versions: 401 GET droplet mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/flow/test/versions: 401 GET droplet mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/flow/test/versions');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/flow/test/versions: 401 GET droplet mock error', {status: 401, statusText: 'GET droplet mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET droplet by type and ID.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getDroplet('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc', 'flows', '2e04b4fb-9513-47bb-aa74-1ae34616bfdc').subscribe(function (response) {
            expect(response.identifier).toEqual('2e04b4fb-9513-47bb-aa74-1ae34616bfdc');
            expect(response.type).toEqual('FLOW');
            expect(response.name).toEqual('Flow #1');
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc/flows/2e04b4fb-9513-47bb-aa74-1ae34616bfdc');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            'identifier': '2e04b4fb-9513-47bb-aa74-1ae34616bfdc',
            'name': 'Flow #1',
            'description': 'This is flow #1',
            'bucketIdentifier': '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'createdTimestamp': 1505931890999,
            'modifiedTimestamp': 1505931890999,
            'type': 'FLOW',
            'snapshotMetadata': null,
            'link': {
                'params': {
                    'rel': 'self'
                },
                'href': 'flows/2e04b4fb-9513-47bb-aa74-1ae34616bfdc'
            }
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET droplet by type and ID.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of(true);
                }
            };
        });

        // api call
        nfRegistryApi.getDroplet('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc', 'flows', '2e04b4fb-9513-47bb-aa74-1ae34616bfdc').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc/flows/2e04b4fb-9513-47bb-aa74-1ae34616bfdc: 401 GET droplet mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Flow Not Found');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc/flows/2e04b4fb-9513-47bb-aa74-1ae34616bfdc: 401 GET droplet mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc/flows/2e04b4fb-9513-47bb-aa74-1ae34616bfdc');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc/flows/2e04b4fb-9513-47bb-aa74-1ae34616bfdc: 401 GET droplet mock error', {status: 401, statusText: 'GET droplet mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET all droplets across all buckets.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getDroplets().subscribe(function (response) {
            expect(response.length).toBe(2);
            expect(response[0].bucketIdentifier).toEqual('9q7f9e54-dc09-4ceb-aa58-9fe581319cdc');
            expect(response[1].bucketIdentifier).toEqual('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
            expect(response[0].name).toEqual('Flow #1');
            expect(response[1].name).toEqual('Flow #2');
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/items');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': '2e04b4fb-9513-47bb-aa74-1ae34616bfdc',
            'name': 'Flow #1',
            'description': 'This is flow #1',
            'bucketIdentifier': '9q7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'createdTimestamp': 1505931890999,
            'modifiedTimestamp': 1505931890999,
            'type': 'FLOW',
            'snapshotMetadata': null,
            'link': {
                'params': {
                    'rel': 'self'
                },
                'href': 'flows/2e04b4fb-9513-47bb-aa74-1ae34616bfdc'
            }
        }, {
            'identifier': '5d04b4fb-9513-47bb-aa74-1ae34616bfdc',
            'name': 'Flow #2',
            'description': 'This is flow #2',
            'bucketIdentifier': '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'createdTimestamp': 1505931890999,
            'modifiedTimestamp': 1505931890999,
            'type': 'FLOW',
            'snapshotMetadata': null,
            'link': {
                'params': {
                    'rel': 'self'
                },
                'href': 'flows/5d04b4fb-9513-47bb-aa74-1ae34616bfdc'
            }
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET all droplets across all buckets.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getDroplets().subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/items: 401 GET droplet mock error');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/items');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/items: 401 GET droplet mock error', {status: 401, statusText: 'GET droplet mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET all droplets across a single bucket.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getDroplets('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc').subscribe(function (response) {
            expect(response.length).toBe(1);
            expect(response[0].bucketIdentifier).toEqual('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
            expect(response[0].name).toEqual('Flow #1');
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/items/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': '2e04b4fb-9513-47bb-aa74-1ae34616bfdc',
            'name': 'Flow #1',
            'description': 'This is flow #1',
            'bucketIdentifier': '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'createdTimestamp': 1505931890999,
            'modifiedTimestamp': 1505931890999,
            'type': 'FLOW',
            'snapshotMetadata': null,
            'link': {
                'params': {
                    'rel': 'self'
                },
                'href': 'flows/2e04b4fb-9513-47bb-aa74-1ae34616bfdc'
            }
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET all droplets across a single bucket.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getDroplets('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/items/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 GET droplet mock error');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/items/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/items/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 GET droplet mock error', {status: 401, statusText: 'GET droplet mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should DELETE a droplet.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.deleteDroplet('flows/1234').subscribe(function (response) {
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/flows/1234');
        expect(req.request.method).toEqual('DELETE');

        // Next, fulfill the request by transmitting a response.
        req.flush({});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to DELETE a droplet.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.deleteDroplet('flows/1234').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/flows/1234: 401 DELETE droplet mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/flows/1234: 401 DELETE droplet mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/flows/1234');
        expect(req.request.method).toEqual('DELETE');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/flows/1234: 401 DELETE droplet mock error', {status: 401, statusText: 'DELETE droplet mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should POST to create a new bucket.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.createBucket('test').subscribe(function (response) {
            expect(response.identifier).toBe('1234');
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            identifier: '1234'
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to POST to create a new bucket.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.createBucket('test').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/buckets: 401 POST bucket mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/buckets: 401 POST bucket mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/buckets: 401 POST bucket mock error', {status: 401, statusText: 'POST bucket mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should DELETE a bucket.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.deleteBucket('1234', 0).subscribe(function (response) {
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets/1234?version=0');
        expect(req.request.method).toEqual('DELETE');

        // Next, fulfill the request by transmitting a response.
        req.flush({});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to DELETE a bucket.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.deleteBucket('1234', 0).subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/buckets/1234?version=0: 401 DELETE bucket mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/buckets/1234?version=0: 401 DELETE bucket mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets/1234?version=0');
        expect(req.request.method).toEqual('DELETE');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/buckets/1234?version=0: 401 DELETE bucket mock error', {status: 401, statusText: 'DELETE bucket mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET bucket by ID.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getBucket('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc').subscribe(function (response) {
            expect(response.identifier).toEqual('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
            expect(response.name).toEqual('Bucket #1');
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            'identifier': '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'name': 'Bucket #1'
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET bucket by ID.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of(true);
                }
            };
        });

        // api call
        nfRegistryApi.getBucket('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 GET bucket mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Bucket Not Found');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 GET bucket mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 GET bucket mock error', {status: 401, statusText: 'GET bucket mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET metadata for all buckets in the registry for which the client is authorized.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getBuckets().subscribe(function (response) {
            expect(response[0].identifier).toEqual('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
            expect(response[0].name).toEqual('Bucket #1');
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'name': 'Bucket #1'
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET metadata for all buckets in the registry for which the client is authorized.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.getBuckets().subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/buckets: 401 GET metadata mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Buckets Not Found');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/buckets: 401 GET metadata mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/buckets: 401 GET metadata mock error', {status: 401, statusText: 'GET metadata mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should PUT to update a bucket name.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.updateBucket({
            'identifier': '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'name': 'Bucket #1',
            'allowBundleRedeploy': true,
            'allowPublicRead': true
        }).subscribe(function (response) {
            expect(response[0].identifier).toEqual('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
            expect(response[0].name).toEqual('Bucket #1');
            expect(response[0].allowBundleRedeploy).toEqual(true);
            expect(response[0].allowPublicRead).toEqual(true);
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        expect(req.request.method).toEqual('PUT');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'name': 'Bucket #1',
            'allowBundleRedeploy': true,
            'allowPublicRead': true
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to PUT to update a bucket name.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.updateBucket({
            'identifier': '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'name': 'Bucket #1',
            'allowBundleRedeploy': false
        }).subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 PUT to update a bucket name mock error');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        expect(req.request.method).toEqual('PUT');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/buckets/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 PUT to update a bucket name mock error', {status: 401, statusText: 'PUT to update a bucket name mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET user by id.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getUser('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc').subscribe(function (response) {
            expect(response[0].identifier).toEqual('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
            expect(response[0].name).toEqual('User #1');
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/users/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'name': 'User #1'
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET user by id.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.getUser('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/tenants/users/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 GET user by id mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('User Not Found');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/tenants/users/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 GET user by id mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/users/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/tenants/users/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 GET user by id mock error', {status: 401, statusText: 'GET user by id mock error', error: 'test'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should POST to add a new user.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.addUser('test').subscribe(function (response) {
            expect(response.identifier).toBe('1234');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/users');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            identifier: '1234'
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to POST to add a new user.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.addUser('test').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/tenants/users: 401 POST add user mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/tenants/users: 401 POST add user mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/users');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/tenants/users: 401 POST add user mock error', {status: 401, statusText: 'POST add user mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should PUT to update a user name.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.updateUser('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc', 'user #1').subscribe(function (response) {
            expect(response[0].identifier).toEqual('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
            expect(response[0].name).toEqual('user #1');
        });
        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/users/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        expect(req.request.method).toEqual('PUT');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            'name': 'user #1'
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to PUT to update a user name.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.updateUser('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc', 'user #1').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/tenants/users/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 PUT to update a user name mock error');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/users/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        expect(req.request.method).toEqual('PUT');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/tenants/users/2f7f9e54-dc09-4ceb-aa58-9fe581319cdc: 401 PUT to update a user name mock error', {status: 401, statusText: 'PUT to update a user name mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET users.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getUsers().subscribe(function (response) {
            expect(response[0].identity).toEqual('User #1');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/users');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': 123,
            'identity': 'User #1'
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail GET users.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.getUsers().subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/tenants/users: 401 GET users mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Users Not Found');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/tenants/users: 401 GET users mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/users');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/tenants/users: 401 GET users mock error', {status: 401, statusText: 'GET users mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should DELETE users.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.deleteUser(123, 0).subscribe(function (response) {
            expect(response.identity).toEqual('User #1');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/users/123?version=0');
        expect(req.request.method).toEqual('DELETE');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            'identifier': 123,
            'identity': 'User #1'
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to DELETE users.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.deleteUser(123, 0).subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/tenants/users/123?version=0: 401 DELETE users mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/tenants/users/123?version=0: 401 DELETE users mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/users/123?version=0');
        expect(req.request.method).toEqual('DELETE');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/tenants/users/123?version=0: 401 DELETE users mock error', {status: 401, statusText: 'DELETE users mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET user groups.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getUserGroups().subscribe(function (response) {
            expect(response[0].identity).toEqual('Group #1');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/user-groups');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': 123,
            'identity': 'Group #1'
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET user groups.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.getUserGroups().subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/tenants/user-groups: 401 GET user groups mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Groups Not Found');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/tenants/user-groups: 401 GET user groups mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/user-groups');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/tenants/user-groups: 401 GET user groups mock error', {status: 401, statusText: 'GET user groups mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET resource policies by resource identifier.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getResourcePoliciesById('read', '/buckets', '123').subscribe(function (response) {
            expect(response[0].identifier).toEqual('123');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/policies/read/buckets/123');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': '123'
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET resource policies by resource identifier.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.getResourcePoliciesById('read', '/buckets', '123').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/policies/read/buckets/123: 401 GET get resource policies by id mock error');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/policies/read/buckets/123');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/policies/read/buckets/123: 401 GET get resource policies by id mock error', {status: 401, statusText: 'GET get resource policies by id mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET policy action resource.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getPolicyActionResource('read', '/buckets').subscribe(function (response) {
            expect(response[0].identifier).toEqual('123');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/policies/read/buckets');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': '123'
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET policy action resource.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.getPolicyActionResource('read', '/buckets').subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/policies/read/buckets: 401 GET policy action resource mock error');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/policies/read/buckets');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/policies/read/buckets: 401 GET policy action resource mock error', {status: 401, statusText: 'GET policy action resource mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should PUT policy action resource.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.putPolicyActionResource('123', 'read', '/buckets', [], []).subscribe(function (response) {
            expect(response[0].identifier).toEqual('123');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/policies/123');
        expect(req.request.method).toEqual('PUT');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': '123'
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to PUT policy action resource.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.putPolicyActionResource('123', 'read', '/buckets', [], []).subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/policies/123: 401 PUT policy action resource mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/policies/123: 401 PUT policy action resource mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/policies/123');
        expect(req.request.method).toEqual('PUT');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/policies/123: 401 PUT policy action resource mock error', {status: 401, statusText: 'PUT policy action resource mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should POST policy action resource.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.postPolicyActionResource('read', '/buckets', [], []).subscribe(function (response) {
            expect(response[0].identifier).toEqual('123');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/policies');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush([{
            'identifier': '123'
        }]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to POST policy action resource.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.postPolicyActionResource('read', '/buckets', [], []).subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/policies: 401 POST policy action resource mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/policies: 401 POST policy action resource mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/policies');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/policies: 401 POST policy action resource mock error', {status: 401, statusText: 'POST policy action resource mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should POST to login.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.postToLogin('username', 'password').subscribe(function (response) {
            expect(response).toEqual('abc');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/access/token/login');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush('abc');

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to POST to login.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.postToLogin('username', 'password').subscribe(function (response) {
            expect(response).toEqual('');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Please contact your System Administrator.');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/access/token/login');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/access/token/login: 401 POST to login mock error', {status: 401, statusText: 'POST to login mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET all access policies.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getPolicies().subscribe(function (response) {
            expect(response[0].identifier).toEqual('123');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/policies');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush([
            {
                'identifier': '123',
            }
        ]);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET all access policies.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getPolicies().subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/policies: 401 GET policies mock error');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/policies');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/policies: 401 GET policies mock error', {status: 401, statusText: 'GET policies mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET a user group.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.getUserGroup(123).subscribe(function (response) {
            expect(response.identity).toEqual('Group #1');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/user-groups/123');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            'identifier': 123,
            'identity': 'Group #1'
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to GET a user group.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.getUserGroup(123).subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/tenants/user-groups/123: 401 GET user groups mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Group Not Found');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/tenants/user-groups/123: 401 GET user groups mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/user-groups/123');
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/tenants/user-groups/123: 401 GET user groups mock error', {status: 401, statusText: 'GET user groups mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should DELETE a user group.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.deleteUserGroup(123, 0).subscribe(function (response) {
            expect(response.identity).toEqual('Group #1');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/user-groups/123?version=0');
        expect(req.request.method).toEqual('DELETE');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            'identifier': 123,
            'identity': 'Group #1'
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to DELETE a user group.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.deleteUserGroup(123, 0).subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/tenants/user-groups/123?version=0: 401 DELETE user groups mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/tenants/user-groups/123?version=0: 401 DELETE user groups mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/user-groups/123?version=0');
        expect(req.request.method).toEqual('DELETE');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/tenants/user-groups/123?version=0: 401 DELETE user groups mock error', {status: 401, statusText: 'DELETE user groups mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should POST to create a user group.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.createNewGroup(123, 'Group #1', [{
            identity: 'User #1',
            identifier: 9999
        }]).subscribe(function (response) {
            expect(response.identifier).toEqual(123);
            expect(response.identity).toEqual('Group #1');
            expect(response.users[0].identity).toEqual('User #1');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/user-groups');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            'identifier': 123,
            'identity': 'Group #1',
            'users': [{identity: 'User #1', identifier: 9999}]
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to POST to create a user group.', inject([HttpTestingController], function (httpMock) {
        // Spy
        spyOn(nfRegistryApi.dialogService, 'openConfirm').and.callFake(function () {
        });

        // api call
        nfRegistryApi.createNewGroup(123, 'Group #1', [{
            identity: 'User #1',
            identifier: 9999
        }]).subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/tenants/user-groups: 401 POST user groups mock error');
            var dialogServiceCall = nfRegistryApi.dialogService.openConfirm.calls.first();
            expect(dialogServiceCall.args[0].title).toBe('Error');
            expect(dialogServiceCall.args[0].message).toBe('Http failure response for ../nifi-registry-api/tenants/user-groups: 401 POST user groups mock error');
            expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
            expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/user-groups');
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/tenants/user-groups: 401 POST user groups mock error', {status: 401, statusText: 'POST user groups mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should PUT to update a user group.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.updateUserGroup(123, 'Group #1', [{
            identity: 'User #1',
            identifier: 9999
        }]).subscribe(function (response) {
            expect(response.identifier).toEqual(123);
            expect(response.identity).toEqual('Group #1');
            expect(response.users[0].identity).toEqual('User #1');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/user-groups/123');
        expect(req.request.method).toEqual('PUT');

        // Next, fulfill the request by transmitting a response.
        req.flush({
            'identifier': 123,
            'identity': 'Group #1',
            'users': [{identity: 'User #1', identifier: 9999}]
        });

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should fail to PUT to update a user group.', inject([HttpTestingController], function (httpMock) {
        // api call
        nfRegistryApi.updateUserGroup('123', 'Group #1', [{
            identity: 'User #1',
            identifier: '9999'
        }]).subscribe(function (response) {
            expect(response.message).toEqual('Http failure response for ../nifi-registry-api/tenants/user-groups/123: 401 PUT to update a user group name mock error');
        });

        // the request it made
        req = httpMock.expectOne('../nifi-registry-api/tenants/user-groups/123');
        expect(req.request.method).toEqual('PUT');

        // Next, fulfill the request by transmitting a response.
        req.flush('Http failure response for ../nifi-registry-api/tenants/user-groups/123: 401 PUT to update a user group name mock error', {status: 401, statusText: 'PUT to update a user group name mock error'});

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should GET to export versioned snapshot.', inject([HttpTestingController], function (httpMock) {
        var url = 'testUrl';
        var versionNumber = 1;
        var reqUrl = '../nifi-registry-api/' + url + '/versions/' + versionNumber + '/export';

        var response = '{'
            + 'body: {'
                + 'flowContents: {'
                    + 'componentType: \'PROCESS_GROUP\','
                    + 'connections: [],'
                    + 'controllerServices: [],'
                    + 'funnels: [],'
                    + 'identifier: \'123\','
                    + 'inputPorts: [],'
                    + 'labels: [],'
                    + 'name: \'Test snapshot\','
                    + 'outputPorts: [],'
                    + 'processGroups: []'
                + '},'
                + 'snapshotMetadata: {'
                    + 'author: \'anonymous\','
                    + 'bucketIdentifier: \'123\','
                    + 'comments: \'Test comments\','
                    + 'flowIdentifier: \'555\','
                    + 'link: {'
                        + 'href: \'buckets/123/flows/555/versions/2\','
                        + 'params: {}'
                    + '},'
                    + 'version: 2'
                + '}'
            + '},'
            + 'headers: {'
                + 'headers: ['
                    + '{\'filename\': [\'Test-flow-version-1\']}'
                + '],'
                + 'normalizedNames: {\'filename\': \'filename\'}'
            + '},'
            + 'ok: true,'
            + 'status: 200,'
            + 'statusText: \'OK\','
            + 'type: 4,'
            + 'url: \'testUrl\''
        + '}';

        var stringResponse = encodeURIComponent(response);

        var anchor = document.createElement('a');

        anchor.href = 'data:application/json;charset=utf-8,' + stringResponse;
        anchor.download = 'Test-flow-version-3.json';
        anchor.style = 'display: none;';

        spyOn(document.body, 'appendChild');
        spyOn(document.body, 'removeChild');

        // api call
        nfRegistryApi.exportDropletVersionedSnapshot(url, versionNumber).subscribe(function (res) {
            expect(res.body).toEqual(response);
            expect(res.status).toEqual(200);
        });

        // the request it made
        req = httpMock.expectOne(reqUrl);
        expect(req.request.method).toEqual('GET');

        // Next, fulfill the request by transmitting a response.
        req.flush(response);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();

        expect(document.body.appendChild).toHaveBeenCalled();
        expect(document.body.removeChild).toHaveBeenCalled();
    }));

    it('should POST to upload versioned flow snapshot.', inject([HttpTestingController], function (httpMock) {
        var url = 'testUrl';
        var reqUrl = '../nifi-registry-api/' + url + '/versions/import';

        var response = {
            flowContents: {
                componentType: 'PROCESS_GROUP',
                connections: [],
                controllerServices: [],
                funnels: [],
                name: 'Test name',
                identifier: '123'
            },
            snapshotMetadata: {
                author: 'anonymous',
                comments: 'This is snapshot #5',
                timestamp: 1619806926583,
                version: 3
            }
        };

        var testFile = new File([], 'filename');

        // api call
        nfRegistryApi.uploadVersionedFlowSnapshot(url, testFile, '').subscribe(function (res) {
            expect(res).toEqual(response);
            expect(res.flowContents.name).toEqual('Test name');
            expect(res.snapshotMetadata.comments).toEqual('This is snapshot #5');
        });

        // the request it made
        req = httpMock.expectOne(reqUrl);
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush(response);

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));

    it('should POST to upload new flow snapshot.', inject([HttpTestingController], function (httpMock) {
        var bucketUri = 'buckets/123';
        var flowUri = 'buckets/123/flows/456';
        var createFlowReqUrl = '../nifi-registry-api/' + bucketUri + '/flows';
        var importFlowReqUrl = '../nifi-registry-api/' + flowUri + '/versions/import';
        var headers = new Headers({'Content-Type': 'application/json'});

        var response = {
            bucketIdentifier: '123',
            bucketName: 'Bucket 1',
            createdTimestamp: 1620168949158,
            description: 'Test description',
            identifier: '456',
            link: {
                href: 'buckets/123/flows/456',
                params: {}
            },
            modifiedTimestamp: 1620175586179,
            name: 'Test Flow name',
            permissions: {canDelete: true, canRead: true, canWrite: true},
            type: 'Flow',
            versionCount: 0
        };

        var testFile = new File([], 'filename.json');

        // api call
        nfRegistryApi.uploadFlow(bucketUri, testFile, headers).subscribe(function (res) {
            expect(res).toEqual(response);
        });

        // the request it made
        req = httpMock.expectOne(createFlowReqUrl);
        expect(req.request.method).toEqual('POST');

        // Next, fulfill the request by transmitting a response.
        req.flush(response);

        // the inner request it made
        req = httpMock.expectOne(importFlowReqUrl);
        expect(req.request.method).toEqual('POST');

        // Finally, assert that there are no outstanding requests.
        httpMock.verify();
    }));
});
