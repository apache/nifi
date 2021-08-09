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

import NfRegistryApi from 'services/nf-registry.api';
import { Component, ViewChild } from '@angular/core';
import { FdsSnackBarService } from '@nifi-fds/core';
import NfRegistryService from 'services/nf-registry.service';
import { MatDialogRef } from '@angular/material';

/**
 * NfRegistryCreateNewGroup constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param fdsSnackBarService    The FDS snack bar service module.
 * @param nfRegistryService     The nf-registry.service module.
 * @param matDialogRef          The angular material dialog ref.
 * @constructor
 */
function NfRegistryCreateNewGroup(nfRegistryApi, fdsSnackBarService, nfRegistryService, matDialogRef) {
    // Services
    this.snackBarService = fdsSnackBarService;
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogRef = matDialogRef;
    // local state
    this.keepDialogOpen = false;
}

NfRegistryCreateNewGroup.prototype = {
    constructor: NfRegistryCreateNewGroup,

    /**
     * Create a new group.
     *
     * @param createNewGroupInput     The createNewGroupInput element.
     */
    createNewGroup: function (createNewGroupInput) {
        var self = this;
        // create new group with any selected users added to the new group
        this.nfRegistryApi.createNewGroup(null, createNewGroupInput.value, this.nfRegistryService.getSelectedUsers()).subscribe(function (group) {
            if (!group.error) {
                self.nfRegistryService.groups.push(group);
                self.nfRegistryService.filterUsersAndGroups();
                self.nfRegistryService.allUsersAndGroupsSelected = false;
                if (self.keepDialogOpen !== true) {
                    self.dialogRef.close();
                }
                self.snackBarService.openCoaster({
                    title: 'Success',
                    message: 'Group has been added.',
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
     * Focus the new group input.
     */
    ngAfterViewChecked: function () {
        this.createNewGroupInput.nativeElement.focus();
    }
};

NfRegistryCreateNewGroup.annotations = [
    new Component({
        templateUrl: './nf-registry-create-new-group.html',
        queries: {
            createNewGroupInput: new ViewChild('createNewGroupInput')
        }
    })
];

NfRegistryCreateNewGroup.parameters = [
    NfRegistryApi,
    FdsSnackBarService,
    NfRegistryService,
    MatDialogRef
];

export default NfRegistryCreateNewGroup;
