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

import NfRegistryApi from 'services/nf-registry.api';
import NfRegistryService from 'services/nf-registry.service';
import NfRegistryCreateBucket from 'components/administration/workflow/dialogs/create-bucket/nf-registry-create-bucket';
import { of } from 'rxjs';

describe('NfRegistryCreateBucket Component isolated unit tests', function () {
    var comp;
    var nfRegistryService;
    var nfRegistryApi;

    beforeEach(function () {
        nfRegistryService = new NfRegistryService();
        nfRegistryApi = new NfRegistryApi();
        comp = new NfRegistryCreateBucket(nfRegistryApi, {
            openCoaster: function () {
            }
        },
        nfRegistryService, {
            close: function () {
            }
        });

        // Spy
        spyOn(nfRegistryApi, 'createBucket').and.callFake(function () {
        }).and.returnValue(of({name: 'NewBucket'}));
        spyOn(nfRegistryService, 'filterBuckets');
        spyOn(comp.dialogRef, 'close');
    });

    it('should create a new bucket and close the dialog', function () {
        // The function to test
        comp.createBucket({value: 'NewBucket'}, {checked: false});

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.buckets.length).toBe(1);
        expect(nfRegistryService.buckets[0].name).toBe('NewBucket');
        expect(nfRegistryService.filterBuckets).toHaveBeenCalled();
        expect(comp.dialogRef.close).toHaveBeenCalled();
    });

    it('should create a new bucket and keep the dialog open', function () {
        // setup the component
        comp.keepDialogOpen = true;

        // The function to test
        comp.createBucket({value: 'NewBucket'}, {checked: false});

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.buckets.length).toBe(1);
        expect(nfRegistryService.buckets[0].name).toBe('NewBucket');
        expect(nfRegistryService.filterBuckets).toHaveBeenCalled();
        expect(comp.dialogRef.close.calls.count()).toEqual(0);
    });

    it('should close the dialog', function () {
        // The function to test
        comp.cancel();

        //assertions
        expect(comp.dialogRef.close).toHaveBeenCalled();
    });
});
