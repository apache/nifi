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
import nfRegistryAnimations from 'nf-registry.animations';
import { FdsDialogService } from '@nifi-fds/core';
import { Router } from '@angular/router';

/**
 * NfLoginComponent constructor.
 *
 * @param nfRegistryService     The nf-registry.service module.
 * @param fdsDialogService      The FDS dialog service.
 * @param router                The angular router module.
 */
function NfPageNotFoundComponent(nfRegistryService, fdsDialogService, router) {
    // Services
    this.nfRegistryService = nfRegistryService;
    this.dialogService = fdsDialogService;
    this.router = router;
}

NfPageNotFoundComponent.prototype = {
    constructor: NfPageNotFoundComponent,

    /**
     * Initialize the component
     */
    ngOnInit: function () {
        var self = this;
        this.nfRegistryService.perspective = 'not-found';
        this.dialogService.openConfirm({
            title: 'Page Not Found',
            acceptButton: 'Home',
            acceptButtonColor: 'fds-warn'
        }).afterClosed().subscribe(
            function (accept) {
                if (accept) {
                    self.router.navigateByUrl(self.nfRegistryService.redirectUrl);
                }
            }
        );
    }
};

NfPageNotFoundComponent.annotations = [
    new Component({
        templateUrl: './nf-registry-page-not-found.html',
        animations: [nfRegistryAnimations.slideInLeftAnimation],
        host: {
            '[@routeAnimation]': 'routeAnimation'
        }
    })
];

NfPageNotFoundComponent.parameters = [
    NfRegistryService,
    FdsDialogService,
    Router
];

export default NfPageNotFoundComponent;
