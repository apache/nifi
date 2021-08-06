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
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material';
import { FdsDialogService, FdsSnackBarService } from '@nifi-fds/core';
import NfRegistryApi from 'services/nf-registry.api.js';
import NfStorage from 'services/nf-storage.service.js';
import NfRegistryExportVersionedFlow from '../components/explorer/grid-list/dialogs/export-versioned-flow/nf-registry-export-versioned-flow';
import NfRegistryImportVersionedFlow from '../components/explorer/grid-list/dialogs/import-versioned-flow/nf-registry-import-versioned-flow';
import NfRegistryImportNewFlow from '../components/explorer/grid-list/dialogs/import-new-flow/nf-registry-import-new-flow';

/**
 * NfRegistryService constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param nfStorage             A wrapper for the browser's local storage.
 * @param tdDataTableService    The covalent data table service module.
 * @param router                The angular router module.
 * @param fdsDialogService      The FDS dialog service.
 * @param fdsSnackBarService    The FDS snack bar service module.
 * @param matDialog             The angular material dialog module.
 * @constructor
 */
function NfRegistryService(nfRegistryApi, nfStorage, tdDataTableService, router, fdsDialogService, fdsSnackBarService, matDialog) {
    var self = this;
    this.registry = {
        name: 'NiFi Registry',
        // Config is updated later by calling the /config API.
        config: {}
    };

    this.documentation = {
        link: '../nifi-registry-docs/documentation'
    };
    this.redirectUrl = 'explorer/grid-list';

    // Services
    this.router = router;
    this.api = nfRegistryApi;
    this.nfStorage = nfStorage;
    this.dialogService = fdsDialogService;
    this.snackBarService = fdsSnackBarService;
    this.dataTableService = tdDataTableService;
    this.matDialog = matDialog;

    // data table column definitions
    this.userColumns = [
        {
            name: 'identity',
            label: 'Display Name',
            sortable: true,
            tooltip: 'User name.',
            width: 100
        }
    ];
    this.userGroupsColumns = [
        {
            name: 'identity',
            label: 'Display Name',
            sortable: true,
            tooltip: 'Group name.',
            width: 100
        }
    ];
    this.dropletColumns = [
        {
            name: 'name',
            label: 'Name',
            sortable: true,
            active: true
        },
        {
            name: 'modifiedTimestamp',
            label: 'Updated',
            sortable: true
        },
        {
            name: 'type',
            label: 'Type',
            sortable: true
        }
    ];
    this.bucketColumns = [
        {
            name: 'name',
            label: 'Bucket Name',
            sortable: true,
            tooltip: 'Sort Buckets by name.'
        }
    ];

    // data table available row action definitions
    this.disableMultiBucketDeleteAction = false;
    this.bucketActions = [
        {
            name: 'manage',
            icon: 'fa fa-pencil',
            tooltip: 'Manage Bucket',
            type: 'sidenav',
            disabled: function (row) {
                return false;
            }
        }, {
            name: 'Delete',
            icon: 'fa fa-trash',
            tooltip: 'Delete Bucket',
            disabled: function (row) {
                return (!row.permissions.canDelete);
            }
        }
    ];
    this.bucketPoliciesActions = [
        {
            name: 'manage',
            icon: 'fa fa-pencil',
            tooltip: 'Manage Policy',
            type: 'dialog'
        }, {
            name: 'Delete',
            icon: 'fa fa-trash',
            tooltip: 'Delete Policy'
        }
    ];
    this.dropletActions = [
        {
            name: 'Import new version',
            icon: 'fa fa-upload',
            tooltip: 'Import new flow version',
            disabled: function (droplet) {
                return !droplet.permissions.canWrite;
            }
        },
        {
            name: 'Export version',
            icon: 'fa fa-download',
            tooltip: 'Export flow version',
            disabled: function (droplet) {
                return !droplet.permissions.canRead;
            }
        },
        {
            name: 'Delete flow',
            icon: 'fa fa-trash',
            tooltip: 'Delete',
            disabled: function (droplet) {
                return !droplet.permissions.canDelete;
            }
        }
    ];
    this.disableMultiDeleteAction = false;
    this.usersActions = [
        {
            name: 'manage',
            icon: 'fa fa-pencil',
            type: 'sidenav',
            tooltip: 'Manage User',
            disabled: function (row) {
                return false;
            }
        }, {
            name: 'delete',
            icon: 'fa fa-trash',
            tooltip: 'Delete User',
            disabled: function (row) {
                return (!self.currentUser.resourcePermissions.tenants.canWrite || !row.configurable);
            }
        }
    ];
    this.userGroupsActions = [
        {
            name: 'manage',
            icon: 'fa fa-pencil',
            tooltip: 'Manage User Group Policies',
            type: 'sidenav',
            disabled: function (row) {
                return false;
            }
        }, {
            name: 'delete',
            icon: 'fa fa-trash',
            tooltip: 'Delete User Group',
            disabled: function (row) {
                return (!self.currentUser.resourcePermissions.tenants.canWrite || !row.configurable);
            }
        }
    ];

    // model for buckets privileges
    this.BUCKETS_PRIVS = {
        '/buckets': ['read', 'write', 'delete']
    };

    // model for tenants privileges
    this.TENANTS_PRIVS = {
        '/tenants': ['read', 'write', 'delete']
    };

    // model for policies privileges
    this.POLICIES_PRIVS = {
        '/policies': ['read', 'write', 'delete']
    };

    // model for proxy privileges
    this.PROXY_PRIVS = {
        '/proxy': ['read', 'write', 'delete']
    };

    //<editor-fold desc="application state objects">

    // General
    this.alerts = [];
    this.inProgress = false;
    this.perspective = '';
    this.breadCrumbState = 'out';
    this.explorerViewType = '';
    this.currentUser = {
        loginSupported: false,
        oidcloginSupported: false,
        resourcePermissions: {
            anyTopLevelResource: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            buckets: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            tenants: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            policies: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            proxy: {
                canRead: false,
                canWrite: false,
                canDelete: false
            }
        }
    };
    this.bucket = {};
    this.buckets = [];
    this.droplet = {};
    this.droplets = [];
    this.user = {
        resourcePermissions: {
            anyTopLevelResource: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            buckets: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            tenants: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            policies: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            proxy: {
                canRead: false,
                canWrite: false,
                canDelete: false
            }
        }
    };
    this.users = [];
    this.group = {
        resourcePermissions: {
            anyTopLevelResource: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            buckets: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            tenants: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            policies: {
                canRead: false,
                canWrite: false,
                canDelete: false
            },
            proxy: {
                canRead: false,
                canWrite: false,
                canDelete: false
            }
        }
    };
    this.groups = [];

    // Droplets
    this.filteredDroplets = [];
    this.activeDropletColumn = this.dropletColumns[0];
    this.autoCompleteDroplets = [];
    this.dropletsSearchTerms = [];

    // Buckets
    this.filteredBuckets = [];
    this.allBucketsSelected = false;
    this.autoCompleteBuckets = [];
    this.bucketsSearchTerms = [];
    this.isMultiBucketActionsDisabled = true;

    // Users and Groups
    this.filteredUsers = [];
    this.filteredUserGroups = [];
    this.allUsersAndGroupsSelected = false;
    this.autoCompleteUsersAndGroups = [];
    this.usersSearchTerms = [];

    //</editor-fold>
}

NfRegistryService.prototype = {
    constructor: NfRegistryService,

    /**
     * Set the `breadCrumbState` for the breadcrumb animations.
     *
     * @param {string} state    The state. Valid values are 'in' or 'out'.
     */
    setBreadcrumbState: function (state) {
        this.breadCrumbState = state;
    },

    /**
     * Gets the droplet grid-list explorer component's active sorting column display label.
     *
     * @returns {string}
     */
    getSortByLabel: function () {
        var sortByColumn;
        var arrayLength = this.dropletColumns.length;
        for (var i = 0; i < arrayLength; i++) {
            if (this.dropletColumns[i].active === true) {
                sortByColumn = this.dropletColumns[i];
                break;
            }
        }

        if (sortByColumn) {
            var label = '';
            switch (sortByColumn.label) {
            case 'Updated':
                label = (sortByColumn.sortOrder === 'ASC') ? 'Oldest (update)' : 'Newest (update)';
                break;
            case 'Name':
                label = (sortByColumn.sortOrder === 'ASC') ? 'Name (a - z)' : 'Name (z - a)';
                break;
            case 'Type':
                label = (sortByColumn.sortOrder === 'ASC') ? 'Type (a - z)' : 'Type (z - a)';
                break;
            default:
                break;
            }
            return label;
        }
    },

    /**
     * Generates the droplet grid-list explorer component's sorting menu options.
     *
     * @param col           One of the available `dropletColumns`.
     * @returns {string}
     */
    generateSortMenuLabels: function (col) {
        var label = '';
        switch (col.label) {
        case 'Updated':
            label = (col.sortOrder !== 'ASC') ? 'Oldest (update)' : 'Newest (update)';
            break;
        case 'Name':
            label = (col.sortOrder !== 'ASC') ? 'Name (a - z)' : 'Name (z - a)';
            break;
        case 'Type':
            label = (col.sortOrder !== 'ASC') ? 'Type (a - z)' : 'Type (z - a)';
            break;
        default:
            break;
        }
        return label;
    },

    /**
     * Delete the latest flow snapshot.
     *
     * @param droplet       The droplet object.
     */
    deleteDroplet: function (droplet) {
        var self = this;
        this.dialogService.openConfirm({
            title: 'Delete Flow',
            message: 'All versions of this ' + droplet.type.toLowerCase() + ' will be deleted.',
            cancelButton: 'Cancel',
            acceptButton: 'Delete',
            acceptButtonColor: 'fds-warn'
        }).afterClosed().subscribe(
            function (accept) {
                if (accept) {
                    var deleteUrl = droplet.link.href;
                    if (droplet.type === 'Flow') {
                        deleteUrl = deleteUrl + '?version=' + droplet.revision.version;
                    }
                    self.api.deleteDroplet(deleteUrl).subscribe(function (response) {
                        if (!response.status || response.status === 200) {
                            self.droplets = self.droplets.filter(function (d) {
                                return (d.identifier !== droplet.identifier);
                            });
                            self.snackBarService.openCoaster({
                                title: 'Success',
                                message: 'All versions of this ' + droplet.type.toLowerCase() + ' have been deleted.',
                                verticalPosition: 'bottom',
                                horizontalPosition: 'right',
                                icon: 'fa fa-check-circle-o',
                                color: '#1EB475',
                                duration: 3000
                            });
                            self.droplet = {};
                            self.filterDroplets();
                        }
                    });
                }
            }
        );
    },

    /**
     * Opens the export version dialog.
     *
     * @param droplet       The droplet object.
     */
    openExportVersionedFlowDialog: function (droplet) {
        this.matDialog.open(NfRegistryExportVersionedFlow, {
            disableClose: true,
            width: '400px',
            data: {
                droplet: droplet
            }
        });
    },

    /**
     * Opens the import new flow dialog.
     *
     * @param buckets       The buckets object.
     * @param activeBucket  The active bucket object.
     */
    openImportNewFlowDialog: function (buckets, activeBucket) {
        var self = this;
        this.matDialog.open(NfRegistryImportNewFlow, {
            disableClose: true,
            width: '550px',
            data: {
                buckets: buckets,
                activeBucket: activeBucket
            }
        }).afterClosed().subscribe(function (flowUri) {
            if (flowUri != null) {
                self.router.navigateByUrl('explorer/grid-list/' + flowUri);
            }
        });
    },

    /**
     * Opens the import new version dialog.
     *
     * @param droplet       The droplet object.
     */
    openImportVersionedFlowDialog: function (droplet) {
        var self = this;

        this.matDialog.open(NfRegistryImportVersionedFlow, {
            disableClose: true,
            width: '550px',
            data: {
                droplet: droplet
            }
        }).afterClosed().subscribe(function () {
            self.getDropletSnapshotMetadata(droplet);
        });
    },

    /**
     * Execute the given droplet action.
     *
     * @param action        The action object.
     * @param droplet       The droplet object the `action` will act upon.
     */
    executeDropletAction: function (action, droplet) {
        switch (action.name.toLowerCase()) {
        case 'import new version':
            // Opens the import versioned flow dialog
            this.openImportVersionedFlowDialog(droplet);
            break;
        case 'export version':
            // Opens the export flow version dialog
            this.openExportVersionedFlowDialog(droplet);
            break;
        case 'delete flow':
            // Deletes the entire data flow
            this.deleteDroplet(droplet);
            break;
        default: // do nothing
        }
    },

    /**
     * Retrieves the snapshot metadata for the given droplet.
     *
     * @param droplet       The droplet.
     */
    getDropletSnapshotMetadata: function (droplet) {
        this.api.getDropletSnapshotMetadata(droplet.link.href, true).subscribe(function (snapshotMetadata) {
            droplet.snapshotMetadata = snapshotMetadata;
        });
    },

    /**
     * Sort `filteredDroplets` by `column`.
     *
     * @param column    The column to sort by.
     */
    sortDroplets: function (column) {
        if (column.sortable === true) {
            // toggle column sort order
            var sortOrder = (column.sortOrder === 'ASC') ? 'DESC' : 'ASC';
            column.sortOrder = sortOrder;

            this.filterDroplets(column.name, sortOrder);
            //only one column can be actively sorted so we reset all to inactive
            this.dropletColumns.forEach(function (c) {
                c.active = false;
            });
            //and set this column as the actively sorted column
            column.active = true;
            this.activeDropletColumn = column;
        }
    },

    /**
     * Filter droplets.
     *
     * @param {string} [sortBy]       The column name to sort `dropletColumns` by.
     * @param {string} [sortOrder]    The order. Either 'ASC' or 'DES'
     */
    filterDroplets: function (sortBy, sortOrder) {
        // if `sortOrder` is `undefined` then use 'ASC'
        if (sortOrder === undefined) {
            sortOrder = 'ASC';
        }
        // if `sortBy` is `undefined` then find the first sortable column in `dropletColumns`
        if (sortBy === undefined) {
            var arrayLength = this.dropletColumns.length;
            for (var i = 0; i < arrayLength; i++) {
                if (this.dropletColumns[i].sortable === true) {
                    sortBy = this.dropletColumns[i].name;
                    //only one column can be actively sorted so we reset all to inactive
                    this.dropletColumns.forEach(function (c) {
                        c.active = false;
                    });
                    //and set this column as the actively sorted column
                    this.dropletColumns[i].active = true;
                    this.dropletColumns[i].sortOrder = sortOrder;
                    break;
                }
            }
        }

        var newData;

        // if we are viewing a single droplet
        if (this.droplet.identifier) {
            newData = [this.droplet];
        } else {
            newData = this.droplets;
        }

        for (var i = 0; i < this.dropletsSearchTerms.length; i++) {
            newData = this.dataTableService.filterData(newData, this.dropletsSearchTerms[i], true);
        }

        newData = this.dataTableService.sortData(newData, sortBy, sortOrder);
        this.filteredDroplets = newData;
        this.getAutoCompleteDroplets();
    },

    /**
     * Generates the `autoCompleteDroplets` options for the droplet filter.
     */
    getAutoCompleteDroplets: function () {
        var self = this;
        this.autoCompleteDroplets = [];
        this.dropletColumns.forEach(function (c) {
            return self.filteredDroplets.forEach(function (r) {
                return (r[c.name.toLowerCase()]) ? self.autoCompleteDroplets.push(r[c.name.toLowerCase()].toString()) : '';
            });
        });
    },

    /**
     * Execute the given bucket action.
     *
     * @param action        The action object.
     * @param bucket        The bucket object the `action` will act upon.
     */
    executeBucketAction: function (action, bucket) {
        var self = this;
        switch (action.name.toLowerCase()) {
        case 'delete':
            this.dialogService.openConfirm({
                title: 'Delete Bucket',
                message: 'All items stored in this bucket will be deleted as well.',
                cancelButton: 'Cancel',
                acceptButton: 'Delete',
                acceptButtonColor: 'fds-warn'
            }).afterClosed().subscribe(
                function (accept) {
                    if (accept) {
                        self.api.deleteBucket(bucket.identifier, bucket.revision.version).subscribe(function (response) {
                            if (!response.status || response.status === 200) {
                                self.buckets = self.buckets.filter(function (b) {
                                    return b.identifier !== bucket.identifier;
                                });
                                self.snackBarService.openCoaster({
                                    title: 'Success',
                                    message: 'All versions of all items in this bucket, as well as the bucket, have been deleted.',
                                    verticalPosition: 'bottom',
                                    horizontalPosition: 'right',
                                    icon: 'fa fa-check-circle-o',
                                    color: '#1EB475',
                                    duration: 3000
                                });
                                self.bucket = {};
                                self.filterBuckets();
                                self.determineAllBucketsSelectedState();
                            }
                        });
                    }
                }
            );
            break;
        case 'manage':
            this.router.navigateByUrl('administration/workflow(' + action.type + ':' + action.name + '/bucket/' + bucket.identifier + ')');
            break;
        default:
            break;
        }
    },

    /**
     * Filter buckets and sets the `isMultiBucketActionsDisabled` property accordingly.
     *
     * @param {string} sortBy       The column name to sort `bucketColumns` by.
     * @param {string} sortOrder    The order. Either 'ASC' or 'DES'
     */
    filterBuckets: function (sortBy, sortOrder) {
        // if `sortOrder` is `undefined` then use 'ASC'
        if (sortOrder === undefined) {
            sortOrder = 'ASC';
        }

        // if `sortBy` is `undefined` then find the first sortable column in this.bucketColumns
        if (sortBy === undefined) {
            var arrayLength = this.bucketColumns.length;
            for (var i = 0; i < arrayLength; i++) {
                if (this.bucketColumns[i].sortable === true) {
                    sortBy = this.bucketColumns[i].name;
                    //only one column can be actively sorted so we reset all to inactive
                    this.bucketColumns.forEach(function (c) {
                        c.active = false;
                    });
                    //and set this column as the actively sorted column
                    this.bucketColumns[i].active = true;
                    this.bucketColumns[i].sortOrder = sortOrder;
                    break;
                }
            }
        }

        var newData = this.buckets;

        for (var i = 0; i < this.bucketsSearchTerms.length; i++) {
            newData = this.dataTableService.filterData(newData, this.bucketsSearchTerms[i], true);
        }

        newData = this.dataTableService.sortData(newData, sortBy, sortOrder);
        this.filteredBuckets = newData;

        var selected = 0;
        this.filteredBuckets.forEach(function (filteredBucket) {
            if (filteredBucket.checked) {
                selected++;
            }
        });

        this.isMultiBucketActionsDisabled = !((selected > 0));

        this.getAutoCompleteBuckets();
    },

    /**
     * Gets the buckets the user has permissions to write.
     *
     * @param buckets       The buckets object.
     */
    filterWritableBuckets: function (buckets) {
        var writableBuckets = [];

        buckets.forEach(function (b) {
            if (b.permissions.canWrite) {
                writableBuckets.push(b);
            }
        });
        return writableBuckets;
    },

    /**
     * Generates the `autoCompleteBuckets` options for the bucket filter.
     */
    getAutoCompleteBuckets: function () {
        var self = this;
        this.autoCompleteBuckets = [];
        this.bucketColumns.forEach(function (c) {
            return self.filteredBuckets.forEach(function (r) {
                return (r[c.name.toLowerCase()]) ? self.autoCompleteBuckets.push(r[c.name.toLowerCase()].toString()) : '';
            });
        });
    },

    /**
     * Sort `filteredBuckets` by `column`.
     *
     * @param column    The column to sort by.
     */
    sortBuckets: function (column) {
        if (column.sortable === true) {
            // toggle column sort order
            var sortOrder = (column.sortOrder === 'ASC') ? 'DESC' : 'ASC';
            column.sortOrder = sortOrder;

            this.filterBuckets(column.name, sortOrder);
            //only one column can be actively sorted so we reset all to inactive
            this.bucketColumns.forEach(function (c) {
                c.active = false;
            });
            //and set this column as the actively sorted column
            column.active = true;
        }
    },

    /**
     * Returns true if each bucket in the `filteredBuckets` are selected and sets the `isMultiBucketActionsDisabled`
     * property accordingly.
     *
     * @returns {boolean}
     */
    allFilteredBucketsSelected: function () {
        var selected = 0;
        var allSelected = true;
        var disableMultiBucketDeleteAction = false;
        this.filteredBuckets.forEach(function (bucket) {
            if (bucket.checked) {
                selected++;
            }
            if (bucket.checked === undefined || bucket.checked === false) {
                allSelected = false;
            }
            if (bucket.permissions.canDelete === false) {
                disableMultiBucketDeleteAction = true;
            }
        });

        this.disableMultiBucketDeleteAction = disableMultiBucketDeleteAction;
        this.isMultiBucketActionsDisabled = !((selected > 0));
        return allSelected;
    },

    /**
     * Checks each of the `filteredBuckets`'s `checked` property state and sets the `allBucketsSelected`
     * property accordingly.
     */
    determineAllBucketsSelectedState: function () {
        if (this.allFilteredBucketsSelected()) {
            this.allBucketsSelected = true;
        } else {
            this.allBucketsSelected = false;
        }
    },

    /**
     * Checks the `allBucketsSelected` property state and either selects
     * or deselects all filtered buckets.
     */
    toggleBucketsSelectAll: function () {
        if (this.allBucketsSelected) {
            this.selectAllBuckets();
        } else {
            this.deselectAllBuckets();
        }
    },

    /**
     * Sets the `checked` property of each filtered bucket to true and sets
     * the `isMultiBucketActionsDisabled` property accordingly.
     */
    selectAllBuckets: function () {
        this.filteredBuckets.forEach(function (c) {
            c.checked = true;
        });
        this.isMultiBucketActionsDisabled = false;
    },

    /**
     * Sets the `checked` property of each filtered bucket to false and sets
     * the `isMultiBucketActionsDisabled` property accordingly.
     */
    deselectAllBuckets: function () {
        this.filteredBuckets.forEach(function (c) {
            c.checked = false;
        });
        this.isMultiBucketActionsDisabled = true;
    },

    /**
     * Removes a `searchTerm` from the `bucketsSearchTerms` and filters the `buckets`.
     *
     * @param {string} searchTerm The search term to remove.
     */
    bucketsSearchRemove: function (searchTerm) {
        //only remove the first occurrence of the search term
        var index = this.bucketsSearchTerms.indexOf(searchTerm);
        if (index !== -1) {
            this.bucketsSearchTerms.splice(index, 1);
        }
        this.filterBuckets();
        this.determineAllBucketsSelectedState();
    },

    /**
     * Adds a `searchTerm` from the `bucketsSearchTerms` and filters the `buckets`.
     *
     * @param {string} searchTerm The search term to add.
     */
    bucketsSearchAdd: function (searchTerm) {
        this.bucketsSearchTerms.push(searchTerm);
        this.filterBuckets();
        this.determineAllBucketsSelectedState();
    },

    /**
     * Deletes all versions of all flows of each selected bucket
     */
    deleteSelectedBuckets: function () {
        var self = this;
        this.dialogService.openConfirm({
            title: 'Delete Buckets',
            message: 'All versions of all flows of each selected bucket will be deleted.',
            cancelButton: 'Cancel',
            acceptButton: 'Delete',
            acceptButtonColor: 'fds-warn'
        }).afterClosed().subscribe(
            function (accept) {
                if (accept) {
                    self.filteredBuckets.forEach(function (filteredBucket) {
                        if (filteredBucket.checked) {
                            self.api.deleteBucket(filteredBucket.identifier, filteredBucket.revision.version).subscribe(function (response) {
                                if (!response.status || response.status === 200) {
                                    self.buckets = self.buckets.filter(function (bucket) {
                                        return bucket.identifier !== filteredBucket.identifier;
                                    });
                                    self.snackBarService.openCoaster({
                                        title: 'Success',
                                        message: 'All versions of all items in ' + filteredBucket.name + ' have been deleted.',
                                        verticalPosition: 'bottom',
                                        horizontalPosition: 'right',
                                        icon: 'fa fa-check-circle-o',
                                        color: '#1EB475',
                                        duration: 3000
                                    });
                                    self.filterBuckets();
                                }
                            });
                        }
                    });
                    self.determineAllBucketsSelectedState();
                }
            }
        );
    },

    /**
     * Sort `users` and `groups` by `column`.
     *
     * @param column    The column to sort by.
     */
    sortUsersAndGroups: function (column) {
        if (column.sortable) {
            var sortBy = column.name;
            var sortOrder = (column.sortOrder === 'ASC') ? 'DESC' : 'ASC';
            column.sortOrder = sortOrder;

            this.filterUsersAndGroups(sortBy, sortOrder);

            //only one column can be actively sorted so we reset all to inactive
            this.userColumns.forEach(function (c) {
                c.active = false;
            });
            //and set this column as the actively sorted column
            column.active = true;
        }
    },

    /**
     * Adds a `searchTerm` to the `usersSearchTerms` and filters the `users` amd `groups`.
     *
     * @param {string} searchTerm   The search term to add.
     */
    usersSearchRemove: function (searchTerm) {
        //only remove the first occurrence of the search term
        var index = this.usersSearchTerms.indexOf(searchTerm);
        if (index !== -1) {
            this.usersSearchTerms.splice(index, 1);
        }
        this.filterUsersAndGroups();
        this.determineAllUsersAndGroupsSelectedState();
    },

    /**
     * Removes a `searchTerm` from the `usersSearchTerms` and filters the `users` amd `groups`.
     *
     * @param {string} searchTerm   The search term to remove.
     */
    usersSearchAdd: function (searchTerm) {
        this.usersSearchTerms.push(searchTerm);
        this.filterUsersAndGroups();
        this.determineAllUsersAndGroupsSelectedState();
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
            var arrayLength = this.userColumns.length;
            for (var i = 0; i < arrayLength; i++) {
                if (this.userColumns[i].sortable === true) {
                    sortBy = this.userColumns[i].name;
                    //only one column can be actively sorted so we reset all to inactive
                    this.userColumns.forEach(function (c) {
                        c.active = false;
                    });
                    //and set this column as the actively sorted column
                    this.userColumns[i].active = true;
                    this.userColumns[i].sortOrder = sortOrder;
                    break;
                }
            }
        }

        var newUsersData = this.users;
        var newUserGroupsData = this.groups;

        for (var i = 0; i < this.usersSearchTerms.length; i++) {
            newUsersData = this.dataTableService.filterData(newUsersData, this.usersSearchTerms[i], true);
        }

        newUsersData = this.dataTableService.sortData(newUsersData, sortBy, sortOrder);
        this.filteredUsers = newUsersData;

        for (var i = 0; i < this.usersSearchTerms.length; i++) {
            newUserGroupsData = this.dataTableService.filterData(newUserGroupsData, this.usersSearchTerms[i], true);
        }

        newUserGroupsData = this.dataTableService.sortData(newUserGroupsData, sortBy, sortOrder);
        this.filteredUserGroups = newUserGroupsData;

        this.getAutoCompleteUserAndGroups();
    },

    /**
     * Checks each of the `filteredUsers` and each of the `filteredUserGroups` `checked` property state and sets
     * the `allUsersAndGroupsSelected` property accordingly.
     */
    determineAllUsersAndGroupsSelectedState: function () {
        var allSelected = true;
        var disableMultiDeleteAction = false;
        this.filteredUserGroups.forEach(function (group) {
            if (group.checked === undefined || group.checked === false) {
                allSelected = false;
            }
            if (group.checked && group.configurable === false) {
                disableMultiDeleteAction = true;
            }
        });

        this.filteredUsers.forEach(function (user) {
            if (user.checked === undefined || user.checked === false) {
                allSelected = false;
            }
            if (user.checked && user.configurable === false) {
                disableMultiDeleteAction = true;
            }
        });
        this.disableMultiDeleteAction = disableMultiDeleteAction;
        this.allUsersAndGroupsSelected = allSelected;
    },

    /**
     * Gets the currently selected groups.
     *
     * @returns {Array.<T>}     The selected groups.
     */
    getSelectedGroups: function () {
        return this.filteredUserGroups.filter(function (filteredUserGroup) {
            return filteredUserGroup.checked;
        });
    },

    /**
     * Gets the currently selected users.
     *
     * @returns {Array.<T>}     The selected users.
     */
    getSelectedUsers: function () {
        return this.filteredUsers.filter(function (filteredUser) {
            return filteredUser.checked;
        });
    },

    /**
     * Checks the `allUsersAndGroupsSelected` property state and either selects
     * or deselects all `filteredUsers` and each `filteredUserGroups`.
     */
    toggleUsersSelectAll: function () {
        if (this.allUsersAndGroupsSelected) {
            this.selectAllUsersAndGroups();
        } else {
            this.deselectAllUsersAndGroups();
        }
    },

    /**
     * Sets the `checked` property of each `filteredUsers` and each `filteredUserGroups` to true and sets
     * the `allUsersAndGroupsSelected` properties accordingly.
     */
    selectAllUsersAndGroups: function () {
        this.filteredUsers.forEach(function (c) {
            c.checked = true;
        });
        this.filteredUserGroups.forEach(function (c) {
            c.checked = true;
        });
        this.determineAllUsersAndGroupsSelectedState();
    },

    /**
     * Sets the `checked` property of each `filteredUsers` and each `filteredUserGroups` to false and sets
     * the `allUsersAndGroupsSelected` properties accordingly.
     */
    deselectAllUsersAndGroups: function () {
        this.filteredUsers.forEach(function (c) {
            c.checked = false;
        });
        this.filteredUserGroups.forEach(function (c) {
            c.checked = false;
        });
        this.determineAllUsersAndGroupsSelectedState();
    },

    /**
     * Generates the `autoCompleteUsersAndGroups` options for the users and groups data table filter.
     */
    getAutoCompleteUserAndGroups: function () {
        var self = this;
        this.autoCompleteUsersAndGroups = [];
        this.userColumns.forEach(function (c) {
            var usersAndGroups = self.filteredUsers.concat(self.filteredUserGroups);
            usersAndGroups.forEach(function (r) {
                // eslint-disable-next-line no-unused-expressions
                (r[c.name.toLowerCase()]) ? self.autoCompleteUsersAndGroups.push(r[c.name.toLowerCase()].toString()) : '';
            });
        });
    },

    /**
     * Execute the given user action.
     *
     * @param action        The action object.
     * @param user          The user object the `action` will act upon.
     */
    executeUserAction: function (action, user) {
        var self = this;
        this.user = user;
        switch (action.name.toLowerCase()) {
        case 'delete':
            return this.dialogService.openConfirm({
                title: 'Delete User',
                message: 'This user will lose all access to the registry.',
                cancelButton: 'Cancel',
                acceptButton: 'Delete',
                acceptButtonColor: 'fds-warn'
            }).afterClosed().subscribe(
                function (accept) {
                    if (accept) {
                        self.api.deleteUser(user.identifier, user.revision.version).subscribe(function (response) {
                            if (!response.status || response.status === 200) {
                                self.users = self.users.filter(function (u) {
                                    return u.identifier !== user.identifier;
                                });
                                self.snackBarService.openCoaster({
                                    title: 'Success',
                                    message: 'User: ' + user.identity + ' has been deleted.',
                                    verticalPosition: 'bottom',
                                    horizontalPosition: 'right',
                                    icon: 'fa fa-check-circle-o',
                                    color: '#1EB475',
                                    duration: 3000
                                });
                                self.filterUsersAndGroups();
                                self.determineAllUsersAndGroupsSelectedState();
                            }
                        });
                    }
                }
            );
        case 'manage':
            this.router.navigateByUrl('administration/users(' + action.type + ':' + action.name + '/user/' + user.identifier + ')');
            break;
        default:
            break;
        }
    },

    /**
     * Execute the given group action.
     *
     * @param action        The action object.
     * @param group          The group object the `action` will act upon.
     */
    executeGroupAction: function (action, group) {
        var self = this;
        this.group = group;
        switch (action.name.toLowerCase()) {
        case 'delete':
            this.dialogService.openConfirm({
                title: 'Delete Group',
                message: 'All policies granted to this group will be deleted as well.',
                cancelButton: 'Cancel',
                acceptButton: 'Delete',
                acceptButtonColor: 'fds-warn'
            }).afterClosed().subscribe(
                function (accept) {
                    if (accept) {
                        self.api.deleteUserGroup(group.identifier, group.revision.version).subscribe(function (response) {
                            if (!response.status || response.status === 200) {
                                self.groups = self.groups.filter(function (u) {
                                    return u.identifier !== group.identifier;
                                });
                                self.snackBarService.openCoaster({
                                    title: 'Success',
                                    message: 'Group: ' + group.identity + ' has been deleted.',
                                    verticalPosition: 'bottom',
                                    horizontalPosition: 'right',
                                    icon: 'fa fa-check-circle-o',
                                    color: '#1EB475',
                                    duration: 3000
                                });
                                self.filterUsersAndGroups();
                                self.determineAllUsersAndGroupsSelectedState();
                            }
                        });
                    }
                }
            );
            break;
        case 'manage':
            this.router.navigateByUrl('administration/users(' + action.type + ':' + action.name + '/group/' + group.identifier + ')');
            break;
        default:
            break;
        }
    },

    /**
     * Deletes all selected `filteredUserGroups` and `filteredUsers` and sets the `allUsersAndGroupsSelected`
     * property accordingly.
     */
    deleteSelectedUsersAndGroups: function () {
        var self = this;
        this.dialogService.openConfirm({
            title: 'Delete Users/Groups',
            message: 'The selected users will lose all access to the registry and all policies granted to the selected groups will be deleted.',
            cancelButton: 'Cancel',
            acceptButton: 'Delete',
            acceptButtonColor: 'fds-warn'
        }).afterClosed().subscribe(
            function (accept) {
                if (accept) {
                    self.filteredUserGroups.forEach(function (filteredUserGroup) {
                        if (filteredUserGroup.checked) {
                            self.api.deleteUserGroup(filteredUserGroup.identifier, filteredUserGroup.revision.version).subscribe(function (response) {
                                if (!response.status || response.status === 200) {
                                    self.groups = self.groups.filter(function (u) {
                                        return u.identifier !== filteredUserGroup.identifier;
                                    });
                                    self.snackBarService.openCoaster({
                                        title: 'Success',
                                        message: 'User group: ' + filteredUserGroup.identity + ' has been deleted.',
                                        verticalPosition: 'bottom',
                                        horizontalPosition: 'right',
                                        icon: 'fa fa-check-circle-o',
                                        color: '#1EB475',
                                        duration: 3000
                                    });
                                    self.filterUsersAndGroups();
                                }
                            });
                        }
                    });
                    self.filteredUsers.forEach(function (filteredUser) {
                        if (filteredUser.checked) {
                            self.api.deleteUser(filteredUser.identifier, filteredUser.revision.version).subscribe(function (response) {
                                if (!response.status || response.status === 200) {
                                    self.users = self.users.filter(function (u) {
                                        return u.identifier !== filteredUser.identifier;
                                    });
                                    self.snackBarService.openCoaster({
                                        title: 'Success',
                                        message: 'User: ' + filteredUser.identity + ' has been deleted.',
                                        verticalPosition: 'bottom',
                                        horizontalPosition: 'right',
                                        icon: 'fa fa-check-circle-o',
                                        color: '#1EB475',
                                        duration: 3000
                                    });
                                    self.filterUsersAndGroups();
                                }
                            });
                        }
                    });
                    self.determineAllUsersAndGroupsSelectedState();
                }
            }
        );
    },


    /**
     * Utility method that performs the custom search capability for data tables.
     *
     * @param data          The data to search.
     * @param searchTerm    The term we are looking for.
     * @param ignoreCase    Ignore case.
     * @returns {*}
     */
    experimental_filterData: function (data, searchTerm, ignoreCase) {
        var field = '';
        if (searchTerm.indexOf(':') > -1) {
            field = searchTerm.split(':')[0].trim();
            searchTerm = searchTerm.split(':')[1].trim();
        }
        var filter = searchTerm ? (ignoreCase ? searchTerm.toLowerCase() : searchTerm) : '';

        if (filter) {
            data = data.filter(function (item) {
                var res = Object.keys(item).find(function (key) {
                    if (key !== field && field !== '') {
                        return false;
                    }
                    var preItemValue = ('' + item[key]);
                    var itemValue = ignoreCase ? preItemValue.toLowerCase() : preItemValue;
                    return itemValue.indexOf(filter) > -1;
                });
                return !(typeof res === 'undefined');
            });
        }
        return data;
    }

    //</editor-fold>
};

NfRegistryService.parameters = [
    NfRegistryApi,
    NfStorage,
    TdDataTableService,
    Router,
    FdsDialogService,
    FdsSnackBarService,
    MatDialog
];

export default NfRegistryService;
