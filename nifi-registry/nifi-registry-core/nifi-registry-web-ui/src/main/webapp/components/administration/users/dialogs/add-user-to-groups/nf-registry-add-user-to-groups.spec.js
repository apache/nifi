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
import NfRegistryAddUserToGroups
    from 'components/administration/users/dialogs/add-user-to-groups/nf-registry-add-user-to-groups';
import { of } from 'rxjs';
import { TdDataTableService } from '@covalent/core/data-table';
import { FdsDialogService, FdsSnackBarService } from '@nifi-fds/core';

describe('NfRegistryAddUserToGroups Component isolated unit tests', function () {
    var comp;
    var nfRegistryService;
    var nfRegistryApi;
    var dialogService;
    var snackBarService;
    var dataTableService;

    beforeEach(function () {
        nfRegistryService = new NfRegistryService();
        // setup the nfRegistryService
        nfRegistryService.user = {identifier: 3, identity: 'User 3', userGroups: []};
        nfRegistryService.groups = [{identifier: 1, identity: 'Group 1', configurable: true, checked: true, users: []}];

        nfRegistryApi = new NfRegistryApi();
        dialogService = new FdsDialogService();
        snackBarService = new FdsSnackBarService();
        dataTableService = new TdDataTableService();
        comp = new NfRegistryAddUserToGroups(nfRegistryApi, dataTableService, nfRegistryService, {
            close: function () {
            }
        }, dialogService, snackBarService, {user: nfRegistryService.user});

        // Spy
        spyOn(nfRegistryApi, 'getUserGroups').and.callFake(function () {
        }).and.returnValue(of([{identifier: 1, identity: 'Group 1', configurable: true, checked: true, users: []}]));
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({identifier: 1, identity: 'Group 1'}));
        spyOn(nfRegistryApi, 'updateUserGroup').and.callFake(function () {
        }).and.returnValue(of({identifier: 1, identity: 'Group 1'}));
        spyOn(comp.dialogRef, 'close');
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(comp, 'filterGroups').and.callThrough();

        // initialize the component
        comp.ngOnInit();

        //assertions
        expect(comp.filterGroups).toHaveBeenCalled();
        expect(comp.filteredUserGroups[0].identity).toEqual('Group 1');
        expect(comp.filteredUserGroups.length).toBe(1);
        expect(comp).toBeDefined();
    });

    it('should make a call to the api to add user to selected groups', function () {
        // select a group
        comp.filteredUserGroups[0].checked = true;

        // the function to test
        comp.addToSelectedGroups();

        //assertions
        expect(comp.dialogRef.close).toHaveBeenCalled();
        expect(comp.snackBarService.openCoaster).toHaveBeenCalled();
    });

    it('should determine if all groups are selected', function () {
        // select a group
        comp.filteredUserGroups[0].checked = true;

        // the function to test
        comp.determineAllUserGroupsSelectedState();

        //assertions
        expect(comp.allGroupsSelected).toBe(true);
        expect(comp.isAddToSelectedGroupsDisabled).toBe(false);
    });

    it('should determine if all groups are not selected', function () {
        // select a group
        comp.filteredUserGroups[0].checked = false;

        // the function to test
        comp.determineAllUserGroupsSelectedState();

        //assertions
        expect(comp.allGroupsSelected).toBe(false);
        expect(comp.isAddToSelectedGroupsDisabled).toBe(true);
    });

    it('should select all groups.', function () {
        // The function to test
        comp.selectAllUserGroups();

        //assertions
        expect(comp.filteredUserGroups[0].checked).toBe(true);
        expect(comp.isAddToSelectedGroupsDisabled).toBe(false);
        expect(comp.allGroupsSelected).toBe(true);
    });

    it('should deselect all groups.', function () {
        // select a group
        comp.filteredUserGroups[0].checked = true;

        // The function to test
        comp.deselectAllUserGroups();

        //assertions
        expect(comp.filteredUserGroups[0].checked).toBe(false);
        expect(comp.isAddToSelectedGroupsDisabled).toBe(true);
        expect(comp.allGroupsSelected).toBe(false);
    });

    it('should toggle all groups `checked` properties to true.', function () {
        //Spy
        spyOn(comp, 'selectAllUserGroups').and.callFake(function () {
        });

        comp.allGroupsSelected = true;

        // The function to test
        comp.toggleUserGroupsSelectAll();

        //assertions
        expect(comp.selectAllUserGroups).toHaveBeenCalled();
    });

    it('should toggle all groups `checked` properties to false.', function () {
        //Spy
        spyOn(comp, 'deselectAllUserGroups').and.callFake(function () {
        });

        comp.allGroupsSelected = false;

        // The function to test
        comp.toggleUserGroupsSelectAll();

        //assertions
        expect(comp.deselectAllUserGroups).toHaveBeenCalled();
    });

    it('should sort `groups` by `column`', function () {
        // object to be updated by the test
        var column = {name: 'name', label: 'Group Name', sortable: true};

        // The function to test
        comp.sortUserGroups(column);

        //assertions
        var filterGroupsCall = comp.filterGroups.calls.mostRecent();
        expect(filterGroupsCall.args[0]).toBe('name');
        expect(filterGroupsCall.args[1]).toBe('ASC');
    });

    it('should cancel the addition of the user to any group', function () {
        // the function to test
        comp.cancel();

        //assertions
        expect(comp.dialogRef.close).toHaveBeenCalled();
    });
});
