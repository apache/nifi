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
import { FdsDialogService, FdsSnackBarService } from '@nifi-fds/core';
import { Component } from '@angular/core';
import NfRegistryService from 'services/nf-registry.service';
import { ActivatedRoute, Router } from '@angular/router';
import NfRegistryApi from 'services/nf-registry.api';
import { MatDialog } from '@angular/material';
import NfRegistryAddPolicyToBucket from 'components/administration/workflow/dialogs/add-policy-to-bucket/nf-registry-add-policy-to-bucket';
import NfRegistryEditBucketPolicy from 'components/administration/workflow/dialogs/edit-bucket-policy/nf-registry-edit-bucket-policy';
import { switchMap } from 'rxjs/operators';
import { forkJoin } from 'rxjs';

/**
 * NfRegistryManageBucket constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param nfRegistryService     The nf-registry.service module.
 * @param tdDataTableService    The covalent data table service module.
 * @param fdsDialogService      The FDS dialog service.
 * @param fdsSnackBarService    The FDS snack bar service module.
 * @param activatedRoute        The angular route module.
 * @param router                The angular router module.
 * @param matDialog             The angular material dialog module.
 * @constructor
 */
function NfRegistryManageBucket(nfRegistryApi, nfRegistryService, tdDataTableService, fdsDialogService, fdsSnackBarService, activatedRoute, router, matDialog) {
    // local state
    this.sortBy = null;
    this.sortOrder = null;
    this.bucketPoliciesColumns = [
        {
            name: 'identity',
            label: 'Display Name',
            sortable: true,
            tooltip: 'User/Group name.',
            width: 40,
            active: true
        },
        {
            name: 'permissions',
            label: 'Permissions',
            sortable: false,
            tooltip: 'User/Group permissions for this bucket.',
            width: 40
        }
    ];
    this.userPermsSearchTerms = [];
    this.bucketname = '';
    this.allowBundleRedeploy = false;
    this.allowPublicRead = false;
    this.bucketPolicies = [];
    this.userPerms = {};
    this.groupPerms = {};
    this.filteredGroupPermsData = [];
    this.filteredUserPermsData = [];
    this.userIdentitiesWithPolicies = [];
    this.groupIdentitiesWithPolicies = [];

    // Services
    this.nfRegistryService = nfRegistryService;
    this.route = activatedRoute;
    this.router = router;
    this.dialog = matDialog;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogService = fdsDialogService;
    this.snackBarService = fdsSnackBarService;
    this.dataTableService = tdDataTableService;
    this.protocol = location.protocol;
}

NfRegistryManageBucket.prototype = {
    constructor: NfRegistryManageBucket,

    /**
     * Initialize the component.
     */
    ngOnInit: function () {
        var self = this;
        this.$subscription = this.route.params
            .pipe(
                switchMap(function (params) {
                    return forkJoin(
                        self.nfRegistryApi.getBucket(params['bucketId']),
                        self.nfRegistryApi.getPolicies()
                    );
                })
            )
            .subscribe(function (response) {
                if (!response[0].status || response[0].status === 200) {
                    self.nfRegistryService.sidenav.open();
                    var bucket = response[0];
                    self.nfRegistryService.bucket = bucket;
                    self.bucketname = bucket.name;
                    self.allowBundleRedeploy = bucket.allowBundleRedeploy;
                    self.allowPublicRead = bucket.allowPublicRead;
                    if (!self.nfRegistryService.currentUser.anonymous) {
                        if (!response[1].status || response[1].status === 200) {
                            var policies = response[1];
                            policies.forEach(function (policy) {
                                if (policy.resource.indexOf('/buckets/' + self.nfRegistryService.bucket.identifier) >= 0) {
                                    self.bucketPolicies.push(policy);
                                    policy.users.forEach(function (user) {
                                        var userActionsForBucket = self.userPerms[user.identity] || [];
                                        userActionsForBucket.push(policy.action);
                                        self.userPerms[user.identity] = userActionsForBucket;
                                    });
                                    policy.userGroups.forEach(function (group) {
                                        var groupActionsForBucket = self.groupPerms[group.identity] || [];
                                        groupActionsForBucket.push(policy.action);
                                        self.groupPerms[group.identity] = groupActionsForBucket;
                                    });
                                }
                            });
                            self.sortBuckets(self.bucketPoliciesColumns.find(bucketPoliciesColumn => bucketPoliciesColumn.active === true));
                        }
                    }
                } else if (response[0].status === 404) {
                    self.router.navigateByUrl('administration/workflow');
                }
            });
    },

    /**
     * Destroy the component.
     */
    ngOnDestroy: function () {
        this.nfRegistryService.sidenav.close();
        this.$subscription.unsubscribe();
    },

    /**
     * Navigate to administer the buckets of the current registry.
     */
    closeSideNav: function () {
        this.router.navigateByUrl('administration/workflow');
    },

    /**
     * Opens a modal dialog UX enabling the creation of policies.
     */
    addPolicy: function () {
        var self = this;
        this.dialog.open(NfRegistryAddPolicyToBucket, {
            data: {
                users: this.userIdentitiesWithPolicies,
                groups: this.groupIdentitiesWithPolicies,
                disableClose: true
            }
        }).afterClosed().subscribe(function (dialogResult) {
            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier)
                .subscribe(function (response) {
                    self.nfRegistryService.bucket = response;
                    self.bucketname = response.name;
                    self.allowBundleRedeploy = response.allowBundleRedeploy;
                    self.allowPublicRead = response.allowPublicRead;

                    if (dialogResult) {
                        if (dialogResult.userOrGroup.type === 'user') {
                            self.userPerms[dialogResult.userOrGroup.identity] = dialogResult.permissions;
                        } else {
                            self.groupPerms[dialogResult.userOrGroup.identity] = dialogResult.permissions;
                        }
                        self.snackBarService.openCoaster({
                            title: 'Success',
                            message: 'The policy has been created for this user/group.',
                            verticalPosition: 'bottom',
                            horizontalPosition: 'right',
                            icon: 'fa fa-check-circle-o',
                            color: '#1EB475',
                            duration: 3000
                        });
                    }
                    self.filterPolicies(this.sortBy, this.sortOrder);
                });
        });
    },

    /**
     * Opens a modal dialog UX enabling the editing of a policy.
     */
    editPolicy: function (userOrGroup) {
        var self = this;
        this.dialog.open(NfRegistryEditBucketPolicy, {
            data: {
                userOrGroup: userOrGroup,
                disableClose: true
            },
            width: '400px'
        }).afterClosed().subscribe(function (dialogResult) {
            self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier)
                .subscribe(function (response) {
                    self.nfRegistryService.bucket = response;
                    self.bucketname = response.name;
                    self.allowBundleRedeploy = response.allowBundleRedeploy;
                    self.allowPublicRead = response.allowPublicRead;

                    if (dialogResult) {
                        if (dialogResult.userOrGroup.type === 'user') {
                            self.userPerms[dialogResult.userOrGroup.identity] = dialogResult.permissions;
                        } else {
                            self.groupPerms[dialogResult.userOrGroup.identity] = dialogResult.permissions;
                        }
                        self.snackBarService.openCoaster({
                            title: 'Success',
                            message: 'The policy has been updated for this user/group.',
                            verticalPosition: 'bottom',
                            horizontalPosition: 'right',
                            icon: 'fa fa-check-circle-o',
                            color: '#1EB475',
                            duration: 3000
                        });
                    }
                    self.filterPolicies(this.sortBy, this.sortOrder);
                });
        });
    },

    /**
     * Filter policies.
     *
     * @param {string} [sortBy]       The column name to sort `bucketPoliciesColumns` by.
     * @param {string} [sortOrder]    The order. Either 'ASC' or 'DES'
     */
    filterPolicies: function (sortBy, sortOrder) {
        // if `sortOrder` is `undefined` then use 'ASC'
        if (sortOrder === undefined) {
            if (this.sortOrder === undefined) {
                sortOrder = 'ASC';
            } else {
                sortOrder = this.sortOrder;
            }
        }
        // if `sortBy` is `undefined` then find the first sortable column in `bucketPoliciesColumns`
        if (sortBy === undefined) {
            if (this.sortBy === undefined) {
                var arrayLength = this.bucketPoliciesColumns.length;
                for (var i = 0; i < arrayLength; i++) {
                    if (this.bucketPoliciesColumns[i].sortable === true) {
                        sortBy = this.bucketPoliciesColumns[i].name;
                        //only one column can be actively sorted so we reset all to inactive
                        this.bucketPoliciesColumns.forEach(function (c) {
                            c.active = false;
                        });
                        //and set this column as the actively sorted column
                        this.bucketPoliciesColumns[i].active = true;
                        this.bucketPoliciesColumns[i].sortOrder = sortOrder;
                        break;
                    }
                }
            } else {
                sortBy = this.sortBy;
            }
        }

        var newUserPermsData = [];
        this.userIdentitiesWithPolicies = [];
        // eslint-disable-next-line no-restricted-syntax
        for (var identity in this.userPerms) {
            if (this.userPerms.hasOwnProperty(identity)) {
                this.userIdentitiesWithPolicies.push(identity);
                newUserPermsData.push({identity: identity, permissions: this.userPerms[identity].join(', ')});
            }
        }

        for (var i = 0; i < this.userPermsSearchTerms.length; i++) {
            newUserPermsData = this.filterData(newUserPermsData, this.userPermsSearchTerms[i], true);
        }

        newUserPermsData = this.dataTableService.sortData(newUserPermsData, sortBy, sortOrder);
        this.filteredUserPermsData = newUserPermsData;

        var newGroupPermsData = [];
        this.groupIdentitiesWithPolicies = [];
        // eslint-disable-next-line no-restricted-syntax
        for (var identity in this.groupPerms) {
            if (this.groupPerms.hasOwnProperty(identity)) {
                this.groupIdentitiesWithPolicies.push(identity);
                newGroupPermsData.push({identity: identity, permissions: this.groupPerms[identity].join(', ')});
            }
        }

        for (var i = 0; i < this.userPermsSearchTerms.length; i++) {
            newGroupPermsData = this.filterData(newGroupPermsData, this.userPermsSearchTerms[i], true);
        }

        newGroupPermsData = this.dataTableService.sortData(newGroupPermsData, sortBy, sortOrder);
        this.filteredGroupPermsData = newGroupPermsData;
    },

    /**
     * Sort `groups` by `column`.
     *
     * @param column    The column to sort by.
     */
    sortBuckets: function (column) {
        if (column.sortable) {
            this.sortBy = column.name;
            this.sortOrder = (column.sortOrder === 'ASC') ? 'DESC' : 'ASC';
            column.sortOrder = this.sortOrder;

            this.filterPolicies(this.sortBy, this.sortOrder);

            //only one column can be actively sorted so we reset all to inactive
            this.bucketPoliciesColumns.forEach(function (c) {
                c.active = false;
            });
            //and set this column as the actively sorted column
            column.active = true;
        }
    },

    /**
     * Remove policy from bucket.
     *
     * @param userOrGroup       The user or group object
     */
    removePolicyFromBucket: function (userOrGroup) {
        var self = this;
        this.dialogService.openConfirm({
            title: 'Delete Policy',
            message: 'All permissions granted by this policy will be removed for this user/group.',
            cancelButton: 'Cancel',
            acceptButton: 'Delete',
            acceptButtonColor: 'fds-warn'
        }).afterClosed().subscribe(
            function (accept) {
                if (accept) {
                    userOrGroup.permissions.split(', ').forEach(function (action) {
                        self.nfRegistryApi.getPolicyActionResource(action, '/buckets/' + self.nfRegistryService.bucket.identifier).subscribe(function (policy) {
                            if (policy.status && policy.status === 404) {
                                // resource does NOT exist
                            } else {
                                // resource exists, let's filter out the current group and update it
                                policy.users = policy.users.filter(function (user) {
                                    return (user.identity !== userOrGroup.identity);
                                });
                                policy.userGroups = policy.userGroups.filter(function (group) {
                                    return (group.identity !== userOrGroup.identity);
                                });
                                self.nfRegistryApi.putPolicyActionResource(policy.identifier, policy.action,
                                    policy.resource, policy.users, policy.userGroups, policy.revision).subscribe(
                                    function (response) {
                                        // policy removed!!!...now update the view
                                        self.nfRegistryApi.getPolicies().subscribe(function (response) {
                                            self.userPerms = {};
                                            self.groupPerms = {};
                                            self.filteredGroupPermsData = [];
                                            self.filteredUserPermsData = [];
                                            self.userIdentitiesWithPolicies = [];
                                            self.groupIdentitiesWithPolicies = [];
                                            var policies = response;
                                            policies.forEach(function (policy) {
                                                if (policy.resource.indexOf('/buckets/' + self.nfRegistryService.bucket.identifier) >= 0) {
                                                    self.bucketPolicies.push(policy);
                                                    policy.users.forEach(function (user) {
                                                        var userActionsForBucket = self.userPerms[user.identity] || [];
                                                        userActionsForBucket.push(policy.action);
                                                        self.userPerms[user.identity] = userActionsForBucket;
                                                    });
                                                    policy.userGroups.forEach(function (group) {
                                                        var groupActionsForBucket = self.groupPerms[group.identity] || [];
                                                        groupActionsForBucket.push(policy.action);
                                                        self.groupPerms[group.identity] = groupActionsForBucket;
                                                    });
                                                }
                                            });
                                            self.filterPolicies(this.sortBy, this.sortOrder);
                                            self.snackBarService.openCoaster({
                                                title: 'Success',
                                                message: 'All permissions granted by this policy have be removed for this user/group.',
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
                    });
                }
            }
        );
    },

    /**
     * Update bucket name.
     *
     * @param username
     */
    updateBucketName: function (bucketname) {
        var self = this;
        this.nfRegistryApi.updateBucket({
            'identifier': this.nfRegistryService.bucket.identifier,
            'name': bucketname,
            'revision': this.nfRegistryService.bucket.revision
        }).subscribe(function (response) {
            if (!response.status || response.status === 200) {
                self.nfRegistryService.bucket = response;
                // update the bucket identity and revision in the buckets table
                self.nfRegistryService.buckets.filter(function (bucket) {
                    return self.nfRegistryService.bucket.identifier === bucket.identifier;
                }).forEach(function (bucket) {
                    bucket.name = response.name;
                    bucket.revision = response.revision;
                });
                self.snackBarService.openCoaster({
                    title: 'Success',
                    message: 'This bucket name has been updated.',
                    verticalPosition: 'bottom',
                    horizontalPosition: 'right',
                    icon: 'fa fa-check-circle-o',
                    color: '#1EB475',
                    duration: 3000
                });
            } else if (response.status === 409) {
                self.bucketname = self.nfRegistryService.bucket.name;
                self.allowBundleRedeploy = self.nfRegistryService.bucket.allowBundleRedeploy;
                self.allowPublicRead = self.nfRegistryService.bucket.allowPublicRead;

                self.dialogService.openConfirm({
                    title: 'Error',
                    message: 'This bucket already exists. Please enter a different identity/bucket name.',
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
            } else if (response.status === 404) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: response.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
            } else {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: response.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                }).afterClosed().subscribe(function (accept) {
                    self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier)
                        .subscribe(function (response) {
                            if (!response.status || response.status === 200) {
                                self.nfRegistryService.bucket = response;
                                self.bucketname = self.nfRegistryService.bucket.name;
                                self.allowBundleRedeploy = self.nfRegistryService.bucket.allowBundleRedeploy;
                                self.allowPublicRead = self.nfRegistryService.bucket.allowPublicRead;
                            } else if (response.status === 404) {
                                self.router.navigateByUrl('administration/workflow');
                            }
                        });
                });
            }
        });
    },

    /**
     * Update allowBundleRedeploy flag.
     *
     * @param the checkbox change event
     */
    toggleBucketBundleRedeploy: function (event) {
        var self = this;
        this.nfRegistryApi.updateBucket({
            'identifier': this.nfRegistryService.bucket.identifier,
            'allowBundleRedeploy': event.checked,
            'revision': this.nfRegistryService.bucket.revision
        }).subscribe(function (response) {
            if (!response.status || response.status === 200) {
                self.nfRegistryService.bucket = response;
                // update the bucket revision in the buckets table
                self.nfRegistryService.buckets.filter(function (bucket) {
                    return self.nfRegistryService.bucket.identifier === bucket.identifier;
                }).forEach(function (bucket) {
                    bucket.revision = response.revision;
                });
                self.snackBarService.openCoaster({
                    title: 'Success',
                    message: 'Bundle settings have been updated.',
                    verticalPosition: 'bottom',
                    horizontalPosition: 'right',
                    icon: 'fa fa-check-circle-o',
                    color: '#1EB475',
                    duration: 3000
                });
            } else if (response.status === 404) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: response.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
            } else {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: response.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                }).afterClosed().subscribe(function (accept) {
                    self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier)
                        .subscribe(function (response) {
                            if (!response.status || response.status === 200) {
                                self.nfRegistryService.bucket = response;
                                self.bucketname = self.nfRegistryService.bucket.name;
                                self.allowBundleRedeploy = self.nfRegistryService.bucket.allowBundleRedeploy;
                                self.allowPublicRead = self.nfRegistryService.bucket.allowPublicRead;
                            } else if (response.status === 404) {
                                self.router.navigateByUrl('administration/workflow');
                            }
                        });
                });
            }
        });
    },

    /**
     * Update allowPublicRead flag.
     *
     * @param the checkbox change event
     */
    toggleBucketPublicRead: function (event) {
        var self = this;
        this.nfRegistryApi.updateBucket({
            'identifier': this.nfRegistryService.bucket.identifier,
            'allowPublicRead': event.checked,
            'revision': this.nfRegistryService.bucket.revision
        }).subscribe(function (response) {
            if (!response.status || response.status === 200) {
                self.nfRegistryService.bucket = response;
                // update the bucket revision in the buckets table
                self.nfRegistryService.buckets.filter(function (bucket) {
                    return self.nfRegistryService.bucket.identifier === bucket.identifier;
                }).forEach(function (bucket) {
                    bucket.revision = response.revision;
                });
                self.snackBarService.openCoaster({
                    title: 'Success',
                    message: 'Bucket settings have been updated.',
                    verticalPosition: 'bottom',
                    horizontalPosition: 'right',
                    icon: 'fa fa-check-circle-o',
                    color: '#1EB475',
                    duration: 3000
                });
            } else if (response.status === 404) {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: response.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                });
            } else {
                self.dialogService.openConfirm({
                    title: 'Error',
                    message: response.error,
                    acceptButton: 'Ok',
                    acceptButtonColor: 'fds-warn'
                }).afterClosed().subscribe(function (accept) {
                    self.nfRegistryApi.getBucket(self.nfRegistryService.bucket.identifier)
                        .subscribe(function (response) {
                            if (!response.status || response.status === 200) {
                                self.nfRegistryService.bucket = response;
                                self.bucketname = self.nfRegistryService.bucket.name;
                                self.allowBundleRedeploy = self.nfRegistryService.bucket.allowBundleRedeploy;
                                self.allowPublicRead = self.nfRegistryService.bucket.allowPublicRead;
                            } else if (response.status === 404) {
                                self.router.navigateByUrl('administration/workflow');
                            }
                        });
                });
            }
        });
    },

    /**
     * Determine if bucket policies can be edited.
     * @returns {boolean}
     */
    canEditBucketPolicies: function () {
        return this.nfRegistryService.currentUser.resourcePermissions.policies.canWrite
                && this.nfRegistryService.registry.config.supportsConfigurableAuthorizer;
    }
};

NfRegistryManageBucket.annotations = [
    new Component({
        templateUrl: './nf-registry-manage-bucket.html'
    })
];

NfRegistryManageBucket.parameters = [
    NfRegistryApi,
    NfRegistryService,
    TdDataTableService,
    FdsDialogService,
    FdsSnackBarService,
    ActivatedRoute,
    Router,
    MatDialog
];

export default NfRegistryManageBucket;
