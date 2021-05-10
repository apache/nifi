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
import { TdDataTableService } from '@covalent/core/data-table';
import { FdsDialogService, FdsSnackBarService } from '@nifi-fds/core';
import NfRegistryAddUsersToGroup from 'components/administration/users/dialogs/add-users-to-group/nf-registry-add-users-to-group';

describe('NfRegistryAddUsersToGroup Component isolated unit tests', function () {
    var comp;
    var nfRegistryService;
    var nfRegistryApi;
    var dialogService;
    var snackBarService;
    var dataTableService;

    beforeEach(function () {
        nfRegistryService = new NfRegistryService();
        // setup the nfRegistryService
        nfRegistryService.group = {identifier: 1, identity: 'Group 1', users: []};
        nfRegistryService.users = [{identifier: 2, identity: 'User 1', checked: true}];

        nfRegistryApi = new NfRegistryApi();
        dialogService = new FdsDialogService();
        snackBarService = new FdsSnackBarService();
        dataTableService = new TdDataTableService();
        comp = new NfRegistryAddUsersToGroup(nfRegistryApi, dataTableService, nfRegistryService, {
            close: function () {
            }
        }, dialogService, snackBarService, {group: nfRegistryService.group});

        // Spy
        spyOn(nfRegistryApi, 'getUsers').and.callFake(function () {
        }).and.returnValue(of([{identifier: 2, identity: 'User 1', checked: true}]));
        spyOn(nfRegistryApi, 'updateUserGroup').and.callFake(function () {
        }).and.returnValue(of({identifier: 1, identity: 'Group 1'}));
        spyOn(comp.dialogRef, 'close');
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(comp, 'filterUsers').and.callThrough();

        // initialize the component
        comp.ngOnInit();

        //assertions
        expect(comp.filterUsers).toHaveBeenCalled();
        expect(comp.filteredUsers[0].identity).toEqual('User 1');
        expect(comp.filteredUsers.length).toBe(1);
        expect(comp).toBeDefined();
    });

    it('should make a call to the api to add selected users to the group', function () {
        // select a group
        comp.filteredUsers[0].checked = true;

        // the function to test
        comp.addSelectedUsersToGroup();

        //assertions
        expect(comp.dialogRef.close).toHaveBeenCalled();
        expect(comp.snackBarService.openCoaster).toHaveBeenCalled();
    });

    it('should determine if all users are selected', function () {
        // select a group
        comp.filteredUsers[0].checked = true;

        // the function to test
        comp.determineAllUsersSelectedState();

        //assertions
        expect(comp.allUsersSelected).toBe(true);
        expect(comp.isAddSelectedUsersToGroupDisabled).toBe(false);
    });

    it('should determine all user groups are not selected', function () {
        // select a group
        comp.filteredUsers[0].checked = false;

        // the function to test
        comp.determineAllUsersSelectedState();

        //assertions
        expect(comp.allUsersSelected).toBe(false);
        expect(comp.isAddSelectedUsersToGroupDisabled).toBe(true);
    });

    it('should select all groups.', function () {
        // The function to test
        comp.selectAllUsers();

        //assertions
        expect(comp.filteredUsers[0].checked).toBe(true);
        expect(comp.isAddSelectedUsersToGroupDisabled).toBe(false);
        expect(comp.allUsersSelected).toBe(true);
    });

    it('should deselect all groups.', function () {
        // select a group
        comp.filteredUsers[0].checked = true;

        // The function to test
        comp.deselectAllUsers();

        //assertions
        expect(comp.filteredUsers[0].checked).toBe(false);
        expect(comp.isAddSelectedUsersToGroupDisabled).toBe(true);
        expect(comp.allUsersSelected).toBe(false);
    });

    it('should toggle all groups `checked` properties to true.', function () {
        //Spy
        spyOn(comp, 'selectAllUsers').and.callFake(function () {
        });

        comp.allUsersSelected = true;

        // The function to test
        comp.toggleUsersSelectAll();

        //assertions
        expect(comp.selectAllUsers).toHaveBeenCalled();
    });

    it('should toggle all groups `checked` properties to false.', function () {
        //Spy
        spyOn(comp, 'deselectAllUsers').and.callFake(function () {
        });

        comp.allUsersSelected = false;

        // The function to test
        comp.toggleUsersSelectAll();

        //assertions
        expect(comp.deselectAllUsers).toHaveBeenCalled();
    });

    it('should sort `groups` by `column`', function () {
        // object to be updated by the test
        var column = {name: 'name', label: 'Group Name', sortable: true};

        // The function to test
        comp.sortUsers(column);

        //assertions
        var filterUsersCall = comp.filterUsers.calls.mostRecent();
        expect(filterUsersCall.args[0]).toBe('name');
        expect(filterUsersCall.args[1]).toBe('ASC');
    });

    it('should cancel the creation of a new user', function () {
        // the function to test
        comp.cancel();

        //assertions
        expect(comp.dialogRef.close).toHaveBeenCalled();
    });
});
