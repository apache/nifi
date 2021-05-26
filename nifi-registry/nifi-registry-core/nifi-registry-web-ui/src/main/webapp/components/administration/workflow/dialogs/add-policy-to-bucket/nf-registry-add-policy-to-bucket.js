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

import { Component, ViewChild } from '@angular/core';
import NfRegistryService from 'services/nf-registry.service';
import NfRegistryApi from 'services/nf-registry.api';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { ActivatedRoute } from '@angular/router';
import { TdDataTableService } from '@covalent/core/data-table';
import { FdsSnackBarService } from '@nifi-fds/core';
import { switchMap } from 'rxjs/operators';
import { forkJoin } from 'rxjs';

/**
 * NfRegistryAddPolicyToBucket constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param tdDataTableService    The covalent data table service module.
 * @param fdsSnackBarService    The FDS snack bar service module.
 * @param nfRegistryService     The nf-registry.service module.
 * @param activatedRoute        The angular route module.
 * @param matDialogRef          The angular material dialog ref.
 * @param data                  The data passed into this component.
 * @constructor
 */
function NfRegistryAddPolicyToBucket(nfRegistryApi, tdDataTableService, fdsSnackBarService, nfRegistryService, activatedRoute, matDialogRef, data) {
    // local state
    this.users = [];
    this.groups = [];
    this.userOrGroup = {};
    this.filteredGroups = [];
    this.filteredUsers = [];
    this.usersSearchTerms = [];
    this.userGroupsSearchTerms = [];
    // Services
    this.dataTableService = tdDataTableService;
    this.snackBarService = fdsSnackBarService;
    this.nfRegistryService = nfRegistryService;
    this.route = activatedRoute;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogRef = matDialogRef;
    this.data = data;
}

NfRegistryAddPolicyToBucket.prototype = {
    constructor: NfRegistryAddPolicyToBucket,

    /**
     * Initialize the component.
     */
    ngOnInit: function () {
        var self = this;
        this.route.params
            .pipe(
                switchMap(function (params) {
                    return forkJoin(
                        self.nfRegistryApi.getUsers(),
                        self.nfRegistryApi.getUserGroups()
                    );
                })
            )
            .subscribe(function (response) {
                self.users = response[0];
                self.users = self.users.filter(function (user) {
                    user.checked = false;
                    return (self.data.users.indexOf(user.identity) < 0);
                });
                self.groups = response[1];
                self.groups = self.groups.filter(function (group) {
                    group.checked = false;
                    return (self.data.groups.indexOf(group.identity) < 0);
                });

                self.filterUsersAndGroups();
            });
    },

    /**
     * Filter users and groups.
     *
     * @param {string} [sortBy]       The column name to sort `userGroupsColumns` by.
     * @param {string} [sortOrder]    The order. Either 'ASC' or 'DES'
     */
    filterUsersAndGroups: function (sortBy, sortOrder) {
        // if `sortOrder` is `undefined` then use 'ASC'
        if (sortOrder === undefined) {
            sortOrder = 'ASC';
        }
        // if `sortBy` is `undefined` then find the first sortable column in `dropletColumns`
        if (sortBy === undefined) {
            var arrayLength = this.nfRegistryService.userGroupsColumns.length;
            for (var i = 0; i < arrayLength; i++) {
                if (this.nfRegistryService.userGroupsColumns[i].sortable === true) {
                    sortBy = this.nfRegistryService.userGroupsColumns[i].name;
                    //only one column can be actively sorted so we reset all to inactive
                    this.nfRegistryService.userGroupsColumns.forEach(function (c) {
                        c.active = false;
                    });
                    //and set this column as the actively sorted column
                    this.nfRegistryService.userGroupsColumns[i].active = true;
                    this.nfRegistryService.userGroupsColumns[i].sortOrder = sortOrder;
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

        var newUsersData = this.users;

        for (var i = 0; i < this.usersSearchTerms.length; i++) {
            newUsersData = this.dataTableService.filterData(newUsersData, this.usersSearchTerms[i], true);
        }

        newUsersData = this.dataTableService.sortData(newUsersData, sortBy, sortOrder);
        this.filteredUsers = newUsersData;
    },

    /**
     * Sort users and groups.
     *
     * @param column    The column to sort by.
     */
    sortUserAndGroups: function (column) {
        if (column.sortable) {
            var sortBy = column.name;
            var sortOrder = (column.sortOrder === 'ASC') ? 'DESC' : 'ASC';
            column.sortOrder = sortOrder;
            this.filterUsersAndGroups(sortBy, sortOrder);
        }
    },

    /**
     * Create a new policy.
     */
    applyPolicy: function () {
        var self = this;
        var action = '';
        var resource = '/buckets';
        var permissions = [];
        if (this.readCheckbox.checked) {
            action = 'read';
            permissions.push(action);
            this.nfRegistryApi.getResourcePoliciesById(action, resource, this.nfRegistryService.bucket.identifier).subscribe(function (policy) {
                action = 'read';
                if (policy.status && policy.status === 404) {
                    // resource does NOT exist, let's create it
                    var users = [];
                    var groups = [];

                    if (self.userOrGroup.type === 'user') {
                        users.push(self.userOrGroup);
                    } else {
                        groups.push(self.userOrGroup);
                    }

                    self.nfRegistryApi.postPolicyActionResource(action, resource + '/' + self.nfRegistryService.bucket.identifier, users, groups).subscribe(
                        function (response) {
                            // policy created!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                                self.snackBarService.openCoaster({
                                    title: 'Success',
                                    message: 'Policy created.',
                                    verticalPosition: 'bottom',
                                    horizontalPosition: 'right',
                                    icon: 'fa fa-check-circle-o',
                                    color: '#1EB475',
                                    duration: 3000
                                });
                            });
                        }
                    );
                } else {
                    // resource exists, let's update it
                    if (self.userOrGroup.type === 'user') {
                        policy.users.push(self.userOrGroup);
                    } else {
                        policy.userGroups.push(self.userOrGroup);
                    }
                    self.nfRegistryApi.putPolicyActionResource(policy.identifier, policy.action,
                        policy.resource, policy.users, policy.userGroups, policy.revision).subscribe(
                        function (response) {
                            // policy updated!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                                self.snackBarService.openCoaster({
                                    title: 'Success',
                                    message: 'Policy created.',
                                    verticalPosition: 'bottom',
                                    horizontalPosition: 'right',
                                    icon: 'fa fa-check-circle-o',
                                    color: '#1EB475',
                                    duration: 3000
                                });
                            });
                        }
                    );
                }
            });
        }
        if (this.writeCheckbox.checked) {
            action = 'write';
            permissions.push(action);
            this.nfRegistryApi.getResourcePoliciesById(action, resource, this.nfRegistryService.bucket.identifier).subscribe(function (policy) {
                action = 'write';
                if (policy.status && policy.status === 404) {
                    // resource does NOT exist, let's create it
                    var users = [];
                    var groups = [];

                    if (self.userOrGroup.type === 'user') {
                        users.push(self.userOrGroup);
                    } else {
                        groups.push(self.userOrGroup);
                    }

                    self.nfRegistryApi.postPolicyActionResource(action, resource + '/' + self.nfRegistryService.bucket.identifier, users, groups).subscribe(
                        function (response) {
                            // policy created!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                                self.snackBarService.openCoaster({
                                    title: 'Success',
                                    message: 'Policy created.',
                                    verticalPosition: 'bottom',
                                    horizontalPosition: 'right',
                                    icon: 'fa fa-check-circle-o',
                                    color: '#1EB475',
                                    duration: 3000
                                });
                            });
                        }
                    );
                } else {
                    // resource exists, let's update it
                    if (self.userOrGroup.type === 'user') {
                        policy.users.push(self.userOrGroup);
                    } else {
                        policy.userGroups.push(self.userOrGroup);
                    }
                    self.nfRegistryApi.putPolicyActionResource(policy.identifier, policy.action,
                        policy.resource, policy.users, policy.userGroups, policy.revision).subscribe(
                        function (response) {
                            // policy updated!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                                self.snackBarService.openCoaster({
                                    title: 'Success',
                                    message: 'Policy created.',
                                    verticalPosition: 'bottom',
                                    horizontalPosition: 'right',
                                    icon: 'fa fa-check-circle-o',
                                    color: '#1EB475',
                                    duration: 3000
                                });
                            });
                        }
                    );
                }
            });
        }
        if (this.deleteCheckbox.checked) {
            action = 'delete';
            permissions.push(action);
            this.nfRegistryApi.getResourcePoliciesById(action, resource, this.nfRegistryService.bucket.identifier).subscribe(function (policy) {
                action = 'delete';
                if (policy.status && policy.status === 404) {
                    // resource does NOT exist, let's create it
                    var users = [];
                    var groups = [];

                    if (self.userOrGroup.type === 'user') {
                        users.push(self.userOrGroup);
                    } else {
                        groups.push(self.userOrGroup);
                    }

                    self.nfRegistryApi.postPolicyActionResource(action, resource + '/' + self.nfRegistryService.bucket.identifier, users, groups).subscribe(
                        function (response) {
                            // policy created!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                                self.snackBarService.openCoaster({
                                    title: 'Success',
                                    message: 'Policy created.',
                                    verticalPosition: 'bottom',
                                    horizontalPosition: 'right',
                                    icon: 'fa fa-check-circle-o',
                                    color: '#1EB475',
                                    duration: 3000
                                });
                            });
                        }
                    );
                } else {
                    // resource exists, let's update it
                    if (self.userOrGroup.type === 'user') {
                        policy.users.push(self.userOrGroup);
                    } else {
                        policy.userGroups.push(self.userOrGroup);
                    }
                    self.nfRegistryApi.putPolicyActionResource(policy.identifier, policy.action,
                        policy.resource, policy.users, policy.userGroups, policy.revision).subscribe(
                        function (response) {
                            // policy updated!!!...now update the view
                            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier).subscribe(function (response) {
                                self.nfRegistryService.bucket = response;
                                self.snackBarService.openCoaster({
                                    title: 'Success',
                                    message: 'Policy created.',
                                    verticalPosition: 'bottom',
                                    horizontalPosition: 'right',
                                    icon: 'fa fa-check-circle-o',
                                    color: '#1EB475',
                                    duration: 3000
                                });
                            });
                        }
                    );
                }
            });
        }
        this.dialogRef.close({userOrGroup: self.userOrGroup, permissions: permissions});
    },

    /**
     * Toggle all permission checkboxes.
     *
     * @param $event
     */
    toggleAllPermissions: function ($event) {
        if ($event.checked) {
            this.readCheckbox.checked = true;
            this.writeCheckbox.checked = true;
            this.deleteCheckbox.checked = true;
        } else {
            this.readCheckbox.checked = false;
            this.writeCheckbox.checked = false;
            this.deleteCheckbox.checked = false;
        }
    },

    /**
     * Cancel creation of a new policy and close dialog.
     */
    cancel: function () {
        this.dialogRef.close();
    },

    /**
     * Select a user or group.
     *
     * @param userOrGroup   The selected user or group.
     * @param type
     */
    select: function (userOrGroup, type) {
        //deselect all
        this.filteredUsers = this.filteredUsers.filter(function (user) {
            user.checked = false;
            return true;
        });
        this.filteredUserGroups = this.filteredUserGroups.filter(function (group) {
            group.checked = false;
            return true;
        });
        userOrGroup.checked = true;
        this.userOrGroup = userOrGroup;
        this.userOrGroup.type = type;
    },

    /**
     * Change event handler for user or group checkboxes
     * @param $event
     */
    change: function ($event) {
        $event.source.checked = true;
    }
};

NfRegistryAddPolicyToBucket.annotations = [
    new Component({
        templateUrl: './nf-registry-add-policy-to-bucket.html',
        queries: {
            readCheckbox: new ViewChild('readCheckbox'),
            writeCheckbox: new ViewChild('writeCheckbox'),
            deleteCheckbox: new ViewChild('deleteCheckbox')
        }
    })
];

NfRegistryAddPolicyToBucket.parameters = [
    NfRegistryApi,
    TdDataTableService,
    FdsSnackBarService,
    NfRegistryService,
    ActivatedRoute,
    MatDialogRef,
    MAT_DIALOG_DATA
];

export default NfRegistryAddPolicyToBucket;
