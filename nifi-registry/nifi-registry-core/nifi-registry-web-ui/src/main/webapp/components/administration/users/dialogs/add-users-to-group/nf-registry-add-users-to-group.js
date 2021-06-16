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

import { TdDataTableService } from '@covalent/core/data-table';
import NfRegistryApi from 'services/nf-registry.api';
import { Component } from '@angular/core';
import { FdsDialogService, FdsSnackBarService } from '@nifi-fds/core';
import NfRegistryService from 'services/nf-registry.service';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';

/**
 * NfRegistryAddUsersToGroup constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param tdDataTableService    The covalent data table service module.
 * @param nfRegistryService     The nf-registry.service module.
 * @param matDialogRef          The angular material dialog ref.
 * @param fdsSnackBarService    The FDS snack bar service module.
 * @param fdsDialogService      The FDS dialog service.
 * @param data                  The data passed into this component.
 * @constructor
 */
function NfRegistryAddUsersToGroup(nfRegistryApi, tdDataTableService, nfRegistryService, matDialogRef, fdsDialogService, fdsSnackBarService, data) {
    //  Services
    this.dataTableService = tdDataTableService;
    this.snackBarService = fdsSnackBarService;
    this.dialogService = fdsDialogService;
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogRef = matDialogRef;
    this.data = data;

    // local state
    //make an independent copy of the users for sorting and selecting within the scope of this component
    this.users = [];
    this.filteredUsers = [];
    this.isAddSelectedUsersToGroupDisabled = true;
    this.usersSearchTerms = [];
    this.allUsersSelected = false;
    this.usersColumns = [
        {
            name: 'identity',
            label: 'Display Name',
            sortable: true,
            tooltip: 'Group name.',
            width: 100
        }
    ];
}

NfRegistryAddUsersToGroup.prototype = {
    constructor: NfRegistryAddUsersToGroup,

    /**
     * Initialize the component.
     */
    ngOnInit: function () {
        var self = this;

        // retrieve the fresh list of users
        self.nfRegistryApi.getUsers().subscribe(function (response) {
            if (!response.status || response.status === 200) {
                self.users = response;

                self.data.group.users.forEach(function (groupUser) {
                    self.users = self.users.filter(function (user) {
                        return (user.identifier !== groupUser.identifier);
                    });
                });

                self.filterUsers();
                self.deselectAllUsers();
                self.determineAllUsersSelectedState();
            } else {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: response.error,
                    acceptButton: 'Close',
                    acceptButtonColor: 'fds-warn'
                });
            }
        });
    },

    /**
     * Filter users.
     *
     * @param {string} [sortBy]       The column name to sort `usersColumns` by.
     * @param {string} [sortOrder]    The order. Either 'ASC' or 'DES'
     */
    filterUsers: function (sortBy, sortOrder) {
        // if `sortOrder` is `undefined` then use 'ASC'
        if (sortOrder === undefined) {
            sortOrder = 'ASC';
        }
        // if `sortBy` is `undefined` then find the first sortable column in `dropletColumns`
        if (sortBy === undefined) {
            var arrayLength = this.usersColumns.length;
            for (var i = 0; i < arrayLength; i++) {
                if (this.usersColumns[i].sortable === true) {
                    sortBy = this.usersColumns[i].name;
                    //only one column can be actively sorted so we reset all to inactive
                    this.usersColumns.forEach(function (c) {
                        c.active = false;
                    });
                    //and set this column as the actively sorted column
                    this.usersColumns[i].active = true;
                    this.usersColumns[i].sortOrder = sortOrder;
                    break;
                }
            }
        }

        var newUsersData = this.users;

        for (var i = 0; i < this.usersSearchTerms.length; i++) {
            newUsersData = this.dataTableService.filterData(newUsersData, this.usersSearchTerms[i], true);
        }

        newUsersData = this.dataTableService.sortData(newUsersData, sortBy, sortOrder);
        this.filteredUsers = newUsersData;
    },

    /**
     * Sort `filteredUsers` by `column`.
     *
     * @param column    The column to sort by.
     */
    sortUsers: function (column) {
        if (column.sortable) {
            var sortBy = column.name;
            var sortOrder = (column.sortOrder === 'ASC') ? 'DESC' : 'ASC';
            column.sortOrder = sortOrder;
            this.filterUsers(sortBy, sortOrder);
        }
    },

    /**
     * Checks the `allUsersSelected` property state and either selects
     * or deselects each of the `filteredUsers`.
     */
    toggleUsersSelectAll: function () {
        if (this.allUsersSelected) {
            this.selectAllUsers();
        } else {
            this.deselectAllUsers();
        }
    },

    /**
     * Sets the `checked` property of each of the `filteredUsers` to true
     * and sets the `isAddSelectedUsersToGroupDisabled` and the `allUsersSelected`
     * properties accordingly.
     */
    selectAllUsers: function () {
        this.filteredUsers.forEach(function (c) {
            c.checked = true;
        });
        this.isAddSelectedUsersToGroupDisabled = false;
        this.allUsersSelected = true;
    },

    /**
     * Sets the `checked` property of each group to false
     * and sets the `isAddSelectedUsersToGroupDisabled` and the `allUsersSelected`
     * properties accordingly.
     */
    deselectAllUsers: function () {
        this.filteredUsers.forEach(function (c) {
            c.checked = false;
        });
        this.isAddSelectedUsersToGroupDisabled = true;
        this.allUsersSelected = false;
    },

    /**
     * Checks of each of the `filteredUsers`'s checked property state
     * and sets the `allBucketsSelected` and `isAddSelectedUsersToGroupDisabled`
     * property accordingly.
     */
    determineAllUsersSelectedState: function () {
        var selected = 0;
        var allSelected = true;
        this.filteredUsers.forEach(function (c) {
            if (c.checked) {
                selected++;
            }
            if (c.checked === undefined || c.checked === false) {
                allSelected = false;
            }
        });

        if (selected > 0) {
            this.isAddSelectedUsersToGroupDisabled = false;
        } else {
            this.isAddSelectedUsersToGroupDisabled = true;
        }

        this.allUsersSelected = allSelected;
    },

    /**
     * Adds each of the selected users to this group.
     */
    addSelectedUsersToGroup: function () {
        var self = this;
        this.filteredUsers.filter(function (filteredUser) {
            return filteredUser.checked;
        }).forEach(function (filteredUser) {
            self.data.group.users.push(filteredUser);
        });
        this.nfRegistryApi.updateUserGroup(self.data.group.identifier, self.data.group.identity, self.data.group.users, self.data.group.revision).subscribe(function (response) {
            self.dialogRef.close();
            if (!response.status || response.status === 200) {
                self.snackBarService.openCoaster({
                    title: 'Success',
                    message: 'Selected users have been added to the ' + self.data.group.identity + ' group.',
                    verticalPosition: 'bottom',
                    horizontalPosition: 'right',
                    icon: 'fa fa-check-circle-o',
                    color: '#1EB475',
                    duration: 3000
                });
            } else {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: response.error,
                    acceptButton: 'Close',
                    acceptButtonColor: 'fds-warn'
                });
            }
        });
    },

    /**
     * Cancel adding selected users to groups and close the dialog.
     */
    cancel: function () {
        this.dialogRef.close();
    }
};

NfRegistryAddUsersToGroup.annotations = [
    new Component({
        templateUrl: './nf-registry-add-users-to-group.html'
    })
];

NfRegistryAddUsersToGroup.parameters = [
    NfRegistryApi,
    TdDataTableService,
    NfRegistryService,
    MatDialogRef,
    FdsDialogService,
    FdsSnackBarService,
    MAT_DIALOG_DATA
];

export default NfRegistryAddUsersToGroup;
