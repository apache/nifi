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
import { MatDialogRef } from '@angular/material';
import { FdsSnackBarService } from '@nifi-fds/core';

/**
 * NfRegistryAddUser constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param nfRegistryService     The nf-registry.service module.
 * @param fdsSnackBarService    The FDS snack bar service module.
 * @param matDialogRef          The angular material dialog ref.
 * @constructor
 */
function NfRegistryAddUser(nfRegistryApi, nfRegistryService, fdsSnackBarService, matDialogRef) {
    // Services
    this.snackBarService = fdsSnackBarService;
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogRef = matDialogRef;
    // local state
    this.keepDialogOpen = false;
}

NfRegistryAddUser.prototype = {
    constructor: NfRegistryAddUser,

    /**
     * Create a new user.
     *
     * @param addUserInput     The addUserInput element.
     */
    addUser: function (addUserInput) {
        var self = this;
        this.nfRegistryApi.addUser(addUserInput.value).subscribe(function (user) {
            if (!user.error) {
                self.nfRegistryService.users.push(user);
                self.nfRegistryService.allUsersAndGroupsSelected = false;
                self.nfRegistryService.filterUsersAndGroups();
                if (self.keepDialogOpen !== true) {
                    self.dialogRef.close();
                }
                self.snackBarService.openCoaster({
                    title: 'Success',
                    message: 'User has been added.',
                    verticalPosition: 'bottom',
                    horizontalPosition: 'right',
                    icon: 'fa fa-check-circle-o',
                    color: '#1EB475',
                    duration: 3000
                });
            } else {
                self.dialogRef.close();
            }
        });
    },

    /**
     * Cancel creation of a new bucket and close dialog.
     */
    cancel: function () {
        this.dialogRef.close();
    },

    /**
     * Focus the new user input.
     */
    ngAfterViewChecked: function () {
        this.newUserInput.nativeElement.focus();
    }
};

NfRegistryAddUser.annotations = [
    new Component({
        templateUrl: './nf-registry-add-user.html',
        queries: {
            newUserInput: new ViewChild('newUserInput')
        }
    })
];

NfRegistryAddUser.parameters = [
    NfRegistryApi,
    NfRegistryService,
    FdsSnackBarService,
    MatDialogRef
];

export default NfRegistryAddUser;
