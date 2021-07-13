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

import NfRegistryGridListViewer from 'components/explorer/grid-list/registry/nf-registry-grid-list-viewer';

describe('NfRegistryGridListViewer Component', function () {
    let comp;
    let fixture;
    let nfRegistryService;
    let nfRegistryApi;

    beforeEach((done) => {
        const providers = [
            {
                provide: ActivatedRoute,
                useValue: {
                    params: of({})
                }
            }
        ];

        initTestBed({providers})
            .then(() => {
                fixture = TestBed.createComponent(NfRegistryGridListViewer);

                // test instance
                comp = fixture.componentInstance;

                // from the root injector
                nfRegistryService = TestBed.get(NfRegistryService);
                nfRegistryApi = TestBed.get(NfRegistryApi);

                // because the NfRegistryGridListViewer component is a nested route component we need to set up the nfRegistryService service manually
                nfRegistryService.perspective = 'explorer';

                // Spy
                spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {}).and.returnValue(of({}));
                spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {}).and.returnValue(of({}));
                spyOn(nfRegistryApi, 'getBuckets').and.callFake(function () {
                }).and.returnValue(of([{
                    identifier: '2f7f9e54-dc09-4ceb-aa58-9fe581319cdc',
                    name: 'Bucket #1',
                    permissions: {'canDelete': true, 'canRead': true, 'canWrite': true}
                }]));
                spyOn(nfRegistryService, 'filterDroplets');

                done();
            });
    });

    it('should have a defined component', fakeAsync(function () {
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
        // 1st change detection triggers ngOnInit which makes getBuckets and getDroplets calls
        fixture.detectChanges();
        // wait for async getBuckets and getDroplets calls
        tick();
        // 2nd change detection completes after the getBuckets and getDroplets calls
        fixture.detectChanges();

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.explorerViewType).toBe('grid-list');
        expect(nfRegistryService.breadCrumbState).toBe('in');
        expect(nfRegistryService.inProgress).toBe(false);
        expect(nfRegistryService.bucket.identity).toBeUndefined();
        expect(nfRegistryService.droplet.identity).toBeUndefined();
        expect(nfRegistryService.buckets[0].name).toEqual('Bucket #1');
        expect(nfRegistryService.buckets.length).toBe(1);
        expect(nfRegistryService.droplets[0].name).toEqual('Flow #1');
        expect(nfRegistryService.droplets.length).toBe(1);
        expect(nfRegistryApi.getDroplets).toHaveBeenCalled();
        expect(nfRegistryApi.getBuckets).toHaveBeenCalled();
        expect(nfRegistryService.filterDroplets).toHaveBeenCalled();
    }));

    it('should destroy the component', fakeAsync(function () {
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
        // 1st change detection triggers ngOnInit which makes getBuckets and getDroplets calls
        fixture.detectChanges();
        // wait for async getBuckets and getDroplets calls
        tick();
        // 2nd change detection completes after the getBuckets and getDroplets calls
        fixture.detectChanges();

        // The function to test
        comp.ngOnDestroy();

        //assertions
        expect(nfRegistryService.explorerViewType).toBe('');
        expect(nfRegistryService.filteredDroplets.length).toBe(0);
        expect(nfRegistryService.breadCrumbState).toBe('out');
    }));
});
