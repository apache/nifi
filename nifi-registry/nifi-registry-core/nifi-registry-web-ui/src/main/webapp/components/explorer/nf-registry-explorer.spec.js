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
import NfRegistryService from 'services/nf-registry.service';

import NfRegistryExplorer from 'components/explorer/nf-registry-explorer';

describe('NfRegistryExplorer Component', function () {
    let comp;
    let fixture;
    let nfRegistryService;

    beforeEach((done) => {
        initTestBed()
            .then(() => {
                fixture = TestBed.createComponent(NfRegistryExplorer);

                // test instance
                comp = fixture.componentInstance;

                // from the root injector
                nfRegistryService = TestBed.get(NfRegistryService);

                done();
            });
    });

    it('should have a defined component', function () {
        fixture.detectChanges();

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.perspective).toBe('explorer');
    });

    it('should destroy the component', function () {
        fixture.detectChanges();

        // The function to test
        comp.ngOnDestroy();

        //assertions
        expect(nfRegistryService.perspective).toBe('');
        expect(nfRegistryService.buckets.length).toBe(0);
        expect(nfRegistryService.droplet.identifier).toBeUndefined();
        expect(nfRegistryService.bucket.identifier).toBeUndefined();
    });
});
