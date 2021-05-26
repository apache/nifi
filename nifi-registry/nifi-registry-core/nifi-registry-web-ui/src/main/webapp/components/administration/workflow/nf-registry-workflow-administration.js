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
import NfRegistryService from 'services/nf-registry.service';
import NfRegistryApi from 'services/nf-registry.api';
import NfStorage from 'services/nf-storage.service';
import NfRegistryCreateBucket from 'components/administration/workflow/dialogs/create-bucket/nf-registry-create-bucket';
import nfRegistryAnimations from 'nf-registry.animations';
import { ActivatedRoute } from '@angular/router';
import { MatDialog } from '@angular/material';
import { switchMap } from 'rxjs/operators';

/**
 * NfRegistryWorkflowAdministration constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param nfStorage             A wrapper for the browser's local storage.
 * @param nfRegistryService     The nf-registry.service module.
 * @param activatedRoute        The angular activated route module.
 * @param matDialog             The angular material dialog module.
 * @constructor
 */
function NfRegistryWorkflowAdministration(nfRegistryApi, nfStorage, nfRegistryService, activatedRoute, matDialog) {
    // Services
    this.route = activatedRoute;
    this.nfStorage = nfStorage;
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.dialog = matDialog;
}

NfRegistryWorkflowAdministration.prototype = {
    constructor: NfRegistryWorkflowAdministration,

    /**
     * Initialize the component.
     */
    ngOnInit: function () {
        var self = this;
        this.nfRegistryService.inProgress = true;
        this.$subscription = this.route.params
            .pipe(
                switchMap(function (params) {
                    self.nfRegistryService.adminPerspective = 'workflow';
                    return self.nfRegistryApi.getBuckets();
                })
            )
            .subscribe(function (buckets) {
                self.nfRegistryService.buckets = buckets;
                self.nfRegistryService.filterBuckets();
                self.nfRegistryService.inProgress = false;
            });
    },

    /**
     * Destroy the component.
     */
    ngOnDestroy: function () {
        this.nfRegistryService.adminPerspective = '';
        this.nfRegistryService.buckets = [];
        this.nfRegistryService.filteredBuckets = [];
        this.nfRegistryService.allBucketsSelected = false;
        this.$subscription.unsubscribe();
    },

    /**
     * Opens the create new bucket dialog.
     */
    createBucket: function () {
        this.dialog.open(NfRegistryCreateBucket, {
            disableClose: true,
            width: '400px'
        });
    }
};

NfRegistryWorkflowAdministration.annotations = [
    new Component({
        templateUrl: './nf-registry-workflow-administration.html',
        animations: [nfRegistryAnimations.slideInLeftAnimation],
        host: {
            '[@routeAnimation]': 'routeAnimation'
        }
    })
];

NfRegistryWorkflowAdministration.parameters = [
    NfRegistryApi,
    NfStorage,
    NfRegistryService,
    ActivatedRoute,
    MatDialog
];

export default NfRegistryWorkflowAdministration;
