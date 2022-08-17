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

import { TestBed, fakeAsync, tick } from '@angular/core/testing';
import initTestBed from 'nf-registry.testbed-factory';
import { of } from 'rxjs';
import NfRegistryApi from 'services/nf-registry.api';
import NfRegistryService from 'services/nf-registry.service';
import { ActivatedRoute } from '@angular/router';

import NfRegistryBucketGridListViewer from 'components/explorer/grid-list/registry/nf-registry-bucket-grid-list-viewer';

describe('NfRegistryBucketGridListViewer Component', function () {
    let comp;
    let fixture;
    let nfRegistryService;
    let nfRegistryApi;

    beforeEach((done) => {
        const providers = [
            {
                provide: ActivatedRoute,
                useValue: {
                    params: of({bucketId: '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc'})
                }
            }
        ];

        initTestBed({providers})
            .then(() => {
                fixture = TestBed.createComponent(NfRegistryBucketGridListViewer);

                // test instance
                comp = fixture.componentInstance;

                // from the root injector
                nfRegistryService = TestBed.get(NfRegistryService);
                nfRegistryApi = TestBed.get(NfRegistryApi);

                // because the NfRegistryBucketGridListViewer component is a nested route component we need to set up the nfRegistryService service manually
                nfRegistryService.explorerViewType = 'grid-list';

                //Spy
                spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {}).and.returnValue(of({}));
                spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {}).and.returnValue(of({}));
                spyOn(nfRegistryService, 'filterDroplets');

                done();
            });
    });

    it('should have a defined component', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getBuckets').and.callFake(function () {
        }).and.returnValue(of([{
            identifier: '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            name: 'Bucket #1',
            permissions: {'canDelete': true, 'canRead': true, 'canWrite': true}
        }]));
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            name: 'Bucket #1',
            permissions: {'canDelete': true, 'canRead': true, 'canWrite': true}
        }));
        spyOn(nfRegistryApi, 'getDroplets').and.callFake(function () {
        }).and.returnValue(of([{
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
            },
            'permissions': {'canDelete': true, 'canRead': true, 'canWrite': true}
        }]));
        // 1st change detection triggers ngOnInit which makes getBuckets, getBucket, and getDroplets calls
        fixture.detectChanges();
        // wait for async getBuckets, getBucket, and getDroplets calls
        tick();
        // 2nd change detection completes after the getBuckets, getBucket, and getDroplets calls
        fixture.detectChanges();

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.breadCrumbState).toBe('in');
        expect(nfRegistryService.inProgress).toBe(false);
        expect(nfRegistryService.droplet.identity).toBeUndefined();
        expect(nfRegistryService.bucket.name).toEqual('Bucket #1');
        expect(nfRegistryService.buckets[0].name).toEqual('Bucket #1');
        expect(nfRegistryService.buckets.length).toBe(1);
        expect(nfRegistryService.droplets[0].name).toEqual('Flow #1');
        expect(nfRegistryService.droplets.length).toBe(1);
        expect(nfRegistryApi.getBuckets).toHaveBeenCalled();
        expect(nfRegistryService.filterDroplets).toHaveBeenCalled();
        expect(nfRegistryService.filterDroplets.calls.count()).toBe(1);

        const getDropletsCall = nfRegistryApi.getDroplets.calls.first();
        expect(getDropletsCall.args[0]).toBe('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
        const getBucketCall = nfRegistryApi.getBucket.calls.first();
        expect(getBucketCall.args[0]).toBe('2f7f9e54-dc09-4ceb-aa58-9fe581319cdc');
    }));

    it('should FAIL to get buckets, get bucket, and get droplets and redirect to view all buckets', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getBuckets').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(nfRegistryApi, 'getDroplets').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit which makes getBuckets, getBucket, and getDroplets calls
        fixture.detectChanges();
        // wait for async getBuckets, getBucket, and getDroplets calls
        tick();
        // 2nd change detection completes after the getBuckets, getBucket, and getDroplets calls
        fixture.detectChanges();

        //assertions
        const routerCall = comp.router.navigateByUrl.calls.first();
        expect(routerCall.args[0]).toBe('explorer/grid-list');
        expect(comp.router.navigateByUrl.calls.count()).toBe(3);
    }));

    it('should destroy the component', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getBuckets').and.callFake(function () {
        }).and.returnValue(of([{
            identifier: '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            name: 'Bucket #1',
            permissions: {'canDelete': true, 'canRead': true, 'canWrite': true}
        }]));
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
            name: 'Bucket #1',
            permissions: {'canDelete': true, 'canRead': true, 'canWrite': true}
        }));
        spyOn(nfRegistryApi, 'getDroplets').and.callFake(function () {
        }).and.returnValue(of([{
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
            },
            'permissions': {'canDelete': true, 'canRead': true, 'canWrite': true}
        }]));
        // 1st change detection triggers ngOnInit which makes getBuckets, getBucket, and getDroplets calls
        fixture.detectChanges();
        // wait for async getBuckets, getBucket, and getDroplets calls
        tick();
        // 2nd change detection completes after the getBuckets, getBucket, and getDroplets calls
        fixture.detectChanges();

        // The function to test
        comp.ngOnDestroy();

        //assertions
        expect(nfRegistryService.explorerViewType).toBe('');
        expect(nfRegistryService.filteredDroplets.length).toBe(0);
        expect(nfRegistryService.breadCrumbState).toBe('out');
    }));
});
