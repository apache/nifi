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
import initTestBed from 'nf-registry.testbed-factory';
import { of } from 'rxjs';
import NfRegistryApi from 'services/nf-registry.api';
import NfRegistryService from 'services/nf-registry.service';

import * as ngPlatformBrowser from '@angular/platform-browser';
import NfRegistryAdministration from 'components/administration/nf-registry-administration';

describe('NfRegistryAdministration Component', function () {
    let comp;
    let fixture;
    let de;
    let nfRegistryService;
    let nfRegistryApi;

    beforeEach((done) => {
        initTestBed()
            .then(() => {
                fixture = TestBed.createComponent(NfRegistryAdministration);

                // test instance
                comp = fixture.componentInstance;

                // from the root injector
                nfRegistryService = TestBed.get(NfRegistryService);
                nfRegistryApi = TestBed.get(NfRegistryApi);
                de = fixture.debugElement.query(ngPlatformBrowser.By.css('#nifi-registry-administration-perspective'));

                // Spy
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
                    }
                }]));

                done();
            });
    });

    it('should have a defined component', function () {
        fixture.detectChanges();

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.perspective).toBe('administration');
        expect(nfRegistryService.breadCrumbState).toBe('in');
        expect(de).toBeDefined();
    });

    it('should destroy the component', function () {
        fixture.detectChanges();

        // The function to test
        comp.ngOnDestroy();

        //assertions
        expect(nfRegistryService.perspective).toBe('');
        expect(nfRegistryService.breadCrumbState).toBe('out');
    });
});
