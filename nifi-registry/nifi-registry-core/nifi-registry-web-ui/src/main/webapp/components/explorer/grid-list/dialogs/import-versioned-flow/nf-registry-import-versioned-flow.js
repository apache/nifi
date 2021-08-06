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

import { Component } from '@angular/core';
import NfRegistryApi from 'services/nf-registry.api';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';
import { FdsSnackBarService } from '@nifi-fds/core';

/**
 * NfRegistryImportVersionedFlow constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param fdsSnackBarService    The FDS snack bar service module.
 * @param matDialogRef          The angular material dialog ref.
 * @param data                  The data passed into this component.
 * @constructor
 */
function NfRegistryImportVersionedFlow(nfRegistryApi, fdsSnackBarService, matDialogRef, data) {
    // Services
    this.snackBarService = fdsSnackBarService;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogRef = matDialogRef;
    // local state
    this.keepDialogOpen = false;
    this.droplet = data.droplet;
    this.fileToUpload = null;
    this.fileName = null;
    this.comments = '';
    this.hoverValidity = '';
    this.extensions = 'application/json';
    this.multiple = false;
}

NfRegistryImportVersionedFlow.prototype = {
    constructor: NfRegistryImportVersionedFlow,

    fileDragHandler: function (event, extensions) {
        event.preventDefault();
        event.stopPropagation();

        this.extensions = extensions;

        var { items } = event.dataTransfer;
        this.hoverValidity = this.isFileInvalid(items)
            ? 'invalid'
            : 'valid';
    },

    fileDragEndHandler: function () {
        this.hoverValidity = '';
    },

    fileDropHandler: function (event) {
        event.preventDefault();
        event.stopPropagation();

        var { files } = event.dataTransfer;

        if (!this.isFileInvalid(Array.from(files))) {
            this.handleFileInput(files);
        }

        this.hoverValidity = '';
    },
    /**
     * Handle the file input on change.
     */
    handleFileInput: function (files) {
        // get the file
        this.fileToUpload = files[0];

        // get the filename
        var fileName = this.fileToUpload.name;

        // trim off the file extension
        this.fileName = fileName.replace(/\..*/, '');
    },

    /**
     * Open the file selector.
     */
    selectFile: function () {
        document.getElementById('upload-versioned-flow-file-field').click();
    },

    /**
     * Upload new versioned flow snapshot.
     */
    importNewVersion: function () {
        var self = this;
        var comments = this.comments;

        this.nfRegistryApi.uploadVersionedFlowSnapshot(this.droplet.link.href, this.fileToUpload, comments).subscribe(function (response) {
            if (!response.status || response.status === 201) {
                self.snackBarService.openCoaster({
                    title: 'Success',
                    message: 'Successfully imported version ' + response.snapshotMetadata.version + ' of ' + response.flow.name + '.',
                    verticalPosition: 'bottom',
                    horizontalPosition: 'right',
                    icon: 'fa fa-check-circle-o',
                    color: '#1EB475',
                    duration: 3000
                });

                if (self.keepDialogOpen !== true) {
                    self.dialogRef.close();
                }
            } else {
                self.dialogRef.close();
            }
        });
    },

    isFileInvalid: function (items) {
        return (items.length > 1) || (this.extensions !== '' && items[0].type === '') || (this.extensions.indexOf(items[0].type) === -1);
    },

    /**
     * Cancel uploading a new version and close dialog.
     */
    cancel: function () {
        this.dialogRef.close();
    }
};

NfRegistryImportVersionedFlow.annotations = [
    new Component({
        templateUrl: './nf-registry-import-versioned-flow.html'
    })
];

NfRegistryImportVersionedFlow.parameters = [
    NfRegistryApi,
    FdsSnackBarService,
    MatDialogRef,
    MAT_DIALOG_DATA
];

export default NfRegistryImportVersionedFlow;
