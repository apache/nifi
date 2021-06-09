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
 * NfRegistryImportNewFlow constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param fdsSnackBarService    The FDS snack bar service module.
 * @param matDialogRef          The angular material dialog ref.
 * @param data                  The data passed into this component.
 * @constructor
 */
function NfRegistryImportNewFlow(nfRegistryApi, fdsSnackBarService, matDialogRef, data) {
    // Services
    this.snackBarService = fdsSnackBarService;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogRef = matDialogRef;
    // local state
    this.keepDialogOpen = false;
    this.buckets = data.buckets;
    this.activeBucket = data.activeBucket.identifier;
    this.writableBuckets = [];
    this.fileToUpload = null;
    this.fileName = null;
    this.name = '';
    this.description = '';
    this.selectedBucket = {};
    this.hoverValidity = '';
    this.extensions = 'application/json';
    this.multiple = false;
}

NfRegistryImportNewFlow.prototype = {
    constructor: NfRegistryImportNewFlow,

    ngOnInit: function () {
        this.writableBuckets = this.filterWritableBuckets(this.buckets);

        // if there's only 1 writable bucket, always set as the initial value in the bucket dropdown
        // if opening the dialog from the explorer/grid-list, there is no active bucket
        if (this.activeBucket === undefined) {
            if (this.writableBuckets.length === 1) {
                // set the active bucket
                this.activeBucket = this.writableBuckets[0].identifier;
            }
        }
    },

    filterWritableBuckets: function (buckets) {
        var self = this;
        self.writableBuckets = this.writableBuckets;

        buckets.forEach(function (b) {
            if (b.permissions.canWrite) {
                self.writableBuckets.push(b);
            }
        });
        return self.writableBuckets;
    },

    fileDragHandler: function (event, extensions) {
        event.preventDefault();
        event.stopPropagation();

        this.extensions = extensions;

        var {items} = event.dataTransfer;
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
        document.getElementById('upload-flow-file-field').click();
    },

    /**
     * Upload new data flow snapshot.
     */
    importNewFlow: function () {
        var self = this;
        self.name = this.name;
        self.description = this.description;
        self.activeBucket = this.activeBucket;

        self.selectedBucket = this.writableBuckets.find(function (b) {
            return b.identifier === self.activeBucket;
        });

        this.nfRegistryApi.uploadFlow(self.selectedBucket.link.href, self.fileToUpload, self.name, self.description).subscribe(function (response) {
            if (!response.status || response.status === 201) {
                self.snackBarService.openCoaster({
                    title: 'Success',
                    message: 'Successfully imported ' + response.flow.name + ' to the ' + response.bucket.name + ' bucket.',
                    verticalPosition: 'bottom',
                    horizontalPosition: 'right',
                    icon: 'fa fa-check-circle-o',
                    color: '#1EB475',
                    duration: 3000
                });

                if (self.keepDialogOpen !== true) {
                    var uploadedFlowHref = response.flow.link.href;
                    self.dialogRef.close(uploadedFlowHref);
                }
            } else {
                self.dialogRef.close();
            }
        });
    },

    isFileInvalid: function (items) {
        return ((items.length > 1) || (this.extensions !== '' && (items[0].type === '')) || ((this.extensions.indexOf(items[0].type) === -1)));
    },

    /**
     * Cancel uploading a new version and close dialog.
     */
    cancel: function () {
        this.dialogRef.close();
    }
};

NfRegistryImportNewFlow.annotations = [
    new Component({
        templateUrl: './nf-registry-import-new-flow.html'
    })
];

NfRegistryImportNewFlow.parameters = [
    NfRegistryApi,
    FdsSnackBarService,
    MatDialogRef,
    MAT_DIALOG_DATA
];

export default NfRegistryImportNewFlow;
