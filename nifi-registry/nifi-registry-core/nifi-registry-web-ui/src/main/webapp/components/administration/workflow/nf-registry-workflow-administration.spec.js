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

import { TestBed, fakeAsync, tick, async } from '@angular/core/testing';
import initTestBed from 'nf-registry.testbed-factory';
import { of } from 'rxjs';
import NfRegistryApi from 'services/nf-registry.api';
import NfRegistryService from 'services/nf-registry.service';
import * as ngPlatformBrowser from '@angular/platform-browser';
import NfRegistryWorkflowAdministration from 'components/administration/workflow/nf-registry-workflow-administration';
import { ActivatedRoute } from '@angular/router';

describe('NfRegistryWorkflowAdministration Component', function () {
    let comp;
    let fixture;
    let de;
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
                fixture = TestBed.createComponent(NfRegistryWorkflowAdministration);

                // test instance
                comp = fixture.componentInstance;

                // from the root injector
                nfRegistryService = TestBed.get(NfRegistryService);
                nfRegistryApi = TestBed.get(NfRegistryApi);
                de = fixture.debugElement.query(ngPlatformBrowser.By.css('#nifi-registry-workflow-administration-perspective-buckets-container'));

                // Spy
                spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {}).and.returnValue(of({}));
                spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {}).and.returnValue(of({}));
                spyOn(nfRegistryApi, 'getBuckets').and.callFake(function () {
                }).and.returnValue(of([{name: 'Bucket #1'}]));
                spyOn(nfRegistryService, 'filterBuckets');

                done();
            });
    });

    it('should have a defined component', async(function () {
        fixture.detectChanges();
        fixture.whenStable().then(function () { // wait for async getBuckets
            fixture.detectChanges();

            //assertions
            expect(comp).toBeDefined();
            expect(de).toBeDefined();
            expect(nfRegistryService.adminPerspective).toBe('workflow');
            expect(nfRegistryService.inProgress).toBe(false);
            expect(nfRegistryService.buckets[0].name).toEqual('Bucket #1');
            expect(nfRegistryService.buckets.length).toBe(1);
            expect(nfRegistryService.filterBuckets).toHaveBeenCalled();
        });
    }));

    it('should open a dialog to create a new bucket', function () {
        spyOn(comp.dialog, 'open');
        fixture.detectChanges();

        // the function to test
        comp.createBucket();

        //assertions
        expect(comp.dialog.open).toHaveBeenCalled();
    });

    it('should destroy the component', fakeAsync(function () {
        fixture.detectChanges();
        // wait for async getBucket call
        tick();
        fixture.detectChanges();

        // The function to test
        comp.ngOnDestroy();

        //assertions
        expect(nfRegistryService.adminPerspective).toBe('');
        expect(nfRegistryService.buckets.length).toBe(0);
        expect(nfRegistryService.filteredBuckets.length).toBe(0);
        expect(nfRegistryService.allBucketsSelected).toBe(false);
    }));
});
