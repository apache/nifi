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

import NfRegistryApi from 'services/nf-registry.api';
import NfRegistryService from 'services/nf-registry.service';
import { of } from 'rxjs';
import NfRegistryCreateNewGroup from 'components/administration/users/dialogs/create-new-group/nf-registry-create-new-group';

describe('NfRegistryCreateNewGroup Component isolated unit tests', function () {
    var comp;
    var nfRegistryService;
    var nfRegistryApi;

    beforeEach(function () {
        nfRegistryService = new NfRegistryService();
        nfRegistryApi = new NfRegistryApi();
        comp = new NfRegistryCreateNewGroup(nfRegistryApi, {
            openCoaster: function () {
            }
        },
        nfRegistryService,
        {
            close: function () {
            }
        });

        // Spy
        spyOn(nfRegistryApi, 'createNewGroup').and.callFake(function () {
        }).and.returnValue(of([{
            'identifier': '2e04b4fb-9513-47bb-aa74-1ae34616bfdc',
            'identity': 'New Group #1'
        }]));
        spyOn(nfRegistryService, 'filterUsersAndGroups');
        spyOn(comp.dialogRef, 'close');
    });

    it('should make a call to the api to create a new group and close the dialog.', function () {
        // the function to test
        comp.createNewGroup({value: 'New Group #1'});

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.groups.length).toBe(1);
        expect(nfRegistryService.allUsersAndGroupsSelected).toBe(false);
        expect(nfRegistryService.filterUsersAndGroups).toHaveBeenCalled();
        expect(comp.dialogRef.close).toHaveBeenCalled();
    });

    it('should make a call to the api to create a new group and keep the dialog open.', function () {
        // setup the component
        comp.keepDialogOpen = true;

        // the function to test
        comp.createNewGroup({value: 'New Group #1'});

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.groups.length).toBe(1);
        expect(nfRegistryService.allUsersAndGroupsSelected).toBe(false);
        expect(nfRegistryService.filterUsersAndGroups).toHaveBeenCalled();
        expect(comp.dialogRef.close.calls.count()).toEqual(0);
    });

    it('should cancel the creation of a new group', function () {
        // the function to test
        comp.cancel();

        //assertions
        expect(comp.dialogRef.close).toHaveBeenCalled();
    });
});
