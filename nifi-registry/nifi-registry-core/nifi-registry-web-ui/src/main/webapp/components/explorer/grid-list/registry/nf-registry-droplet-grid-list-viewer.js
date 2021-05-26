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
import { ActivatedRoute, Router } from '@angular/router';
import nfRegistryAnimations from 'nf-registry.animations';
import { switchMap } from 'rxjs/operators';
import { forkJoin } from 'rxjs';

/**
 * NfRegistryDropletGridListViewer constructor.
 *
 * @param nfRegistryApi         The api service.
 * @param nfStorage             A wrapper for the browser's local storage.
 * @param nfRegistryService     The nf-registry.service module.
 * @param activatedRoute        The angular activated route module.
 * @param router                The angular router module.
 * @constructor
 */
function NfRegistryDropletGridListViewer(nfRegistryApi, nfStorage, nfRegistryService, activatedRoute, router) {
    this.route = activatedRoute;
    this.router = router;
    this.nfStorage = nfStorage;
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
}

NfRegistryDropletGridListViewer.prototype = {
    constructor: NfRegistryDropletGridListViewer,

    /**
     * Initialize the component.
     */
    ngOnInit: function () {
        var self = this;
        this.nfRegistryService.inProgress = true;
        this.nfRegistryService.explorerViewType = 'grid-list';

        // subscribe to the route params
        this.$subscription = this.route.params
            .pipe(
                switchMap(function (params) {
                    return forkJoin(
                        self.nfRegistryApi.getDroplet(params['bucketId'], params['dropletType'], params['dropletId']),
                        self.nfRegistryApi.getBucket(params['bucketId']),
                        self.nfRegistryApi.getBuckets(),
                        self.nfRegistryApi.getDroplets(params['bucketId'])
                    );
                })
            )
            .subscribe(function (response) {
                if (!response[0].status || response[0].status === 200) {
                    var droplet = response[0];
                    self.nfRegistryService.droplet = droplet;
                } else if (response[0].status === 404) {
                    if (!response[1].status || response[1].status === 200) {
                        var bucket = response[1];
                        self.nfRegistryService.bucket = bucket;
                        self.router.navigateByUrl('explorer/grid-list/buckets/' + bucket.identifier);
                    } else if (response[1].status === 404) {
                        self.router.navigateByUrl('explorer/grid-list');
                    }
                }
                if (!response[1].status || response[1].status === 200) {
                    var bucket = response[1];
                    self.nfRegistryService.bucket = bucket;
                } else if (response[1].status === 404) {
                    self.router.navigateByUrl('explorer/grid-list');
                }
                if (!response[2].status || response[2].status === 200) {
                    var buckets = response[2];
                    self.nfRegistryService.buckets = buckets;
                }
                if (!response[3].status || response[3].status === 200) {
                    var droplets = response[3];
                    self.nfRegistryService.droplets = droplets;
                }
                self.nfRegistryService.filterDroplets();
                self.nfRegistryService.setBreadcrumbState('in');
                self.nfRegistryService.inProgress = false;
            });
    },

    /**
     * Destroy the component.
     */
    ngOnDestroy: function () {
        this.nfRegistryService.explorerViewType = '';
        this.nfRegistryService.setBreadcrumbState('out');
        this.nfRegistryService.filteredDroplets = [];
        this.$subscription.unsubscribe();
    }
};

NfRegistryDropletGridListViewer.annotations = [
    new Component({
        templateUrl: './nf-registry-grid-list-viewer.html',
        animations: [nfRegistryAnimations.flyInOutAnimation]
    })
];

NfRegistryDropletGridListViewer.parameters = [
    NfRegistryApi,
    NfStorage,
    NfRegistryService,
    ActivatedRoute,
    Router
];

export default NfRegistryDropletGridListViewer;
