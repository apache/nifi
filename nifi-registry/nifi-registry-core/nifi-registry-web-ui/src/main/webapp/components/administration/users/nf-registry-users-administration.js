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

import { FdsDialogService } from '@nifi-fds/core';
import { Component } from '@angular/core';
import NfRegistryService from 'services/nf-registry.service';
import { ActivatedRoute, Router } from '@angular/router';
import NfRegistryApi from 'services/nf-registry.api';
import { MatDialog } from '@angular/material';
import nfRegistryAnimations from 'nf-registry.animations';
import NfStorage from 'services/nf-storage.service';
import NfRegistryCreateNewGroup from 'components/administration/users/dialogs/create-new-group/nf-registry-create-new-group';
import NfRegistryAddUser from 'components/administration/users/dialogs/add-user/nf-registry-add-user';
import { switchMap } from 'rxjs/operators';
import { forkJoin } from 'rxjs';

/**
 * NfRegistryUsersAdministration constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param nfStorage             A wrapper for the browser's local storage.
 * @param nfRegistryService     The nf-registry.service module.
 * @param activatedRoute        The angular activated route module.
 * @param fdsDialogService      The FDS dialog service.
 * @param matDialog             The angular material dialog module.
 * @param router                The angular router module.
 * @constructor
 */
function NfRegistryUsersAdministration(nfRegistryApi, nfStorage, nfRegistryService, activatedRoute, fdsDialogService, matDialog, router) {
    // Services
    this.route = activatedRoute;
    this.nfStorage = nfStorage;
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogService = fdsDialogService;
    this.dialog = matDialog;
    this.router = router;
}

NfRegistryUsersAdministration.prototype = {
    constructor: NfRegistryUsersAdministration,

    /**
     * Initialize the component.
     */
    ngOnInit: function () {
        var self = this;
        this.nfRegistryService.inProgress = true;
        this.$subscription = this.route.params
            .pipe(
                switchMap(function (params) {
                    self.nfRegistryService.adminPerspective = 'users';
                    return forkJoin(
                        self.nfRegistryApi.getUsers(),
                        self.nfRegistryApi.getUserGroups()
                    );
                })
            )
            .subscribe(function (response) {
                if (!response[0].status || response[0].status === 200) {
                    var users = response[0];
                    self.nfRegistryService.users = users;
                } else if (response[0].status === 404) {
                    self.router.navigateByUrl('administration/users');
                } else if (response[0].status === 409) {
                    self.router.navigateByUrl('administration/workflow');
                }
                if (!response[1].status || response[1].status === 200) {
                    var groups = response[1];
                    self.nfRegistryService.groups = groups;
                } else if (response[1].status === 404) {
                    self.router.navigateByUrl('administration/users');
                } else if (response[1].status === 409) {
                    self.router.navigateByUrl('administration/workflow');
                }
                self.nfRegistryService.filterUsersAndGroups();
                self.nfRegistryService.inProgress = false;
            });
    },

    /**
     * Destroy the component.
     */
    ngOnDestroy: function () {
        this.nfRegistryService.adminPerspective = '';
        this.nfRegistryService.users = [];
        this.nfRegistryService.filteredUsers = [];
        this.nfRegistryService.groups = [];
        this.nfRegistryService.filteredUserGroups = [];
        this.nfRegistryService.allUsersAndGroupsSelected = false;
        this.$subscription.unsubscribe();
    },

    /**
     * Opens the create new bucket dialog.
     */
    addUser: function () {
        this.dialog.open(NfRegistryAddUser, {
            disableClose: true,
            width: '400px'
        });
    },

    /**
     * Opens the create new group dialog.
     */
    createNewGroup: function () {
        this.dialog.open(NfRegistryCreateNewGroup, {
            disableClose: true,
            width: '400px'
        });
    },

    /**
     * Determine if users can be edited.
     * @returns {boolean}
     */
    canEditUsers: function () {
        return this.nfRegistryService.currentUser.resourcePermissions.tenants.canWrite
                && this.nfRegistryService.registry.config.supportsConfigurableUsersAndGroups;
    }
};

NfRegistryUsersAdministration.annotations = [
    new Component({
        templateUrl: './nf-registry-users-administration.html',
        animations: [nfRegistryAnimations.slideInLeftAnimation],
        host: {
            '[@routeAnimation]': 'routeAnimation'
        }
    })
];

NfRegistryUsersAdministration.parameters = [
    NfRegistryApi,
    NfStorage,
    NfRegistryService,
    ActivatedRoute,
    Router,
    FdsDialogService,
    MatDialog
];

export default NfRegistryUsersAdministration;
