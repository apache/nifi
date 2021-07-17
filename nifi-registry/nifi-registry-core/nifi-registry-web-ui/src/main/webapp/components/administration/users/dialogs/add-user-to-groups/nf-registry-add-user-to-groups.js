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
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';

/**
 * NfRegistryAddUserToGroups constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param tdDataTableService    The covalent data table service module.
 * @param nfRegistryService     The nf-registry.service module.
 * @param matDialogRef          The angular material dialog ref.
 * @param fdsDialogService      The FDS dialog service.
 * @param fdsSnackBarService    The FDS snack bar service module.
 * @param data                  The data passed into this component.
 * @constructor
 */
function NfRegistryAddUserToGroups(nfRegistryApi, tdDataTableService, nfRegistryService, matDialogRef, fdsDialogService, fdsSnackBarService, data) {
    //Services
    this.dataTableService = tdDataTableService;
    this.snackBarService = fdsSnackBarService;
    this.dialogService = fdsDialogService;
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogRef = matDialogRef;
    this.data = data;

    // local state
    this.groups = [];
    this.filteredUserGroups = [];
    this.isAddToSelectedGroupsDisabled = true;
    this.userGroupsSearchTerms = [];
    this.allGroupsSelected = false;
    this.userGroupsColumns = [
        {
            name: 'identity',
            label: 'Display Name',
            sortable: true,
            tooltip: 'Group name.',
            width: 100
        }
    ];
}

NfRegistryAddUserToGroups.prototype = {
    constructor: NfRegistryAddUserToGroups,

    /**
     * Initialize the component.
     */
    ngOnInit: function () {
        var self = this;

        // retrieve the fresh list of groups
        self.nfRegistryApi.getUserGroups().subscribe(function (response) {
            if (!response.status || response.status === 200) {
                self.groups = response;

                // filter out any groups that
                // 1) that are not configurable
                self.groups = self.groups.filter(function (group) {
                    return !!(group.configurable);
                });
                // 2) the user already belongs to
                self.data.user.userGroups.forEach(function (userGroup) {
                    self.groups = self.groups.filter(function (group) {
                        return (group.identifier !== userGroup.identifier);
                    });
                });

                self.filterGroups();
                self.deselectAllUserGroups();
                self.determineAllUserGroupsSelectedState();
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
     * Filter groups.
     *
     * @param {string} [sortBy]       The column name to sort `userGroupsColumns` by.
     * @param {string} [sortOrder]    The order. Either 'ASC' or 'DES'
     */
    filterGroups: function (sortBy, sortOrder) {
        // if `sortOrder` is `undefined` then use 'ASC'
        if (sortOrder === undefined) {
            sortOrder = 'ASC';
        }
        // if `sortBy` is `undefined` then find the first sortable column in `dropletColumns`
        if (sortBy === undefined) {
            var arrayLength = this.userGroupsColumns.length;
            for (var i = 0; i < arrayLength; i++) {
                if (this.userGroupsColumns[i].sortable === true) {
                    sortBy = this.userGroupsColumns[i].name;
                    //only one column can be actively sorted so we reset all to inactive
                    this.userGroupsColumns.forEach(function (c) {
                        c.active = false;
                    });
                    //and set this column as the actively sorted column
                    this.userGroupsColumns[i].active = true;
                    this.userGroupsColumns[i].sortOrder = sortOrder;
                    break;
                }
            }
        }

        var newUserGroupsData = this.groups;

        for (var i = 0; i < this.userGroupsSearchTerms.length; i++) {
            newUserGroupsData = this.dataTableService.filterData(newUserGroupsData, this.userGroupsSearchTerms[i], true);
        }

        newUserGroupsData = this.dataTableService.sortData(newUserGroupsData, sortBy, sortOrder);
        this.filteredUserGroups = newUserGroupsData;
    },

    /**
     * Sort `filteredUserGroups` by `column`.
     *
     * @param column    The column to sort by.
     */
    sortUserGroups: function (column) {
        if (column.sortable) {
            var sortBy = column.name;
            var sortOrder = (column.sortOrder === 'ASC') ? 'DESC' : 'ASC';
            column.sortOrder = sortOrder;
            this.filterGroups(sortBy, sortOrder);
        }
    },

    /**
     * Checks the `allGroupsSelected` property state and either selects
     * or deselects each of the `filteredUserGroups`.
     */
    toggleUserGroupsSelectAll: function () {
        if (this.allGroupsSelected) {
            this.selectAllUserGroups();
        } else {
            this.deselectAllUserGroups();
        }
    },

    /**
     * Sets the `checked` property of each of the `filteredUserGroups` to true
     * and sets the `isAddToSelectedGroupsDisabled` and the `allGroupsSelected`
     * properties accordingly.
     */
    selectAllUserGroups: function () {
        this.filteredUserGroups.forEach(function (c) {
            c.checked = true;
        });
        this.isAddToSelectedGroupsDisabled = false;
        this.allGroupsSelected = true;
    },

    /**
     * Sets the `checked` property of each group to false
     * and sets the `isAddToSelectedGroupsDisabled` and the `allGroupsSelected`
     * properties accordingly.
     */
    deselectAllUserGroups: function () {
        this.filteredUserGroups.forEach(function (c) {
            c.checked = false;
        });
        this.isAddToSelectedGroupsDisabled = true;
        this.allGroupsSelected = false;
    },

    /**
     * Checks of each of the `filteredUserGroups`'s checked property state
     * and sets the `allBucketsSelected` and `isAddToSelectedGroupsDisabled`
     * property accordingly.
     */
    determineAllUserGroupsSelectedState: function () {
        var selected = 0;
        var allSelected = true;
        this.filteredUserGroups.forEach(function (c) {
            if (c.checked) {
                selected++;
            }
            if (c.checked === undefined || c.checked === false) {
                allSelected = false;
            }
        });

        if (selected > 0) {
            this.isAddToSelectedGroupsDisabled = false;
        } else {
            this.isAddToSelectedGroupsDisabled = true;
        }

        this.allGroupsSelected = allSelected;
    },

    /**
     * Adds users to each of the selected groups.
     */
    addToSelectedGroups: function () {
        var self = this;
        var selectedGroups = this.filteredUserGroups.filter(function (filteredUserGroup) {
            return filteredUserGroup.checked;
        });
        selectedGroups.forEach(function (selectedGroup) {
            selectedGroup.users.push(self.data.user);
            self.nfRegistryApi.updateUserGroup(selectedGroup.identifier, selectedGroup.identity, selectedGroup.users, selectedGroup.revision).subscribe(function (response) {
                self.dialogRef.close();
                if (!response.status || response.status === 200) {
                    self.snackBarService.openCoaster({
                        title: 'Success',
                        message: 'User has been added to the ' + response.identity + ' group.',
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
        });
    },

    /**
     * Cancel adding selected users to groups and close the dialog.
     */
    cancel: function () {
        this.dialogRef.close();
    }
};

NfRegistryAddUserToGroups.annotations = [
    new Component({
        templateUrl: './nf-registry-add-user-to-groups.html'
    })
];

NfRegistryAddUserToGroups.parameters = [
    NfRegistryApi,
    TdDataTableService,
    NfRegistryService,
    MatDialogRef,
    FdsDialogService,
    FdsSnackBarService,
    MAT_DIALOG_DATA
];

export default NfRegistryAddUserToGroups;
