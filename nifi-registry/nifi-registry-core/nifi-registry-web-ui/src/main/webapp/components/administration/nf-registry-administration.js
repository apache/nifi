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
import { Router } from '@angular/router';

/**
 * NfRegistryAdministration constructor.
 *
 * @param nfRegistryService     The nf-registry.service module.
 * @param router                The angular router module.
 * @constructor
 */
function NfRegistryAdministration(nfRegistryService, router) {
    //Services
    this.router = router;
    this.nfRegistryService = nfRegistryService;
}

NfRegistryAdministration.prototype = {
    constructor: NfRegistryAdministration,

    /**
     * Initialize the component.
     */
    ngOnInit: function () {
        this.nfRegistryService.perspective = 'administration';
        this.nfRegistryService.setBreadcrumbState('in');
    },

    /**
     * Destroy the component.
     */
    ngOnDestroy: function () {
        this.nfRegistryService.perspective = '';
        this.nfRegistryService.setBreadcrumbState('out');
    },

    /**
     * Navigates to admin perspective.
     *
     * @param $event
     */
    navigateToAdminPerspective: function ($event) {
        this.router.navigateByUrl('administration/' + $event.value);
    },

    /**
     * Generate the user tab tooltip.
     *
     * @returns {*}
     */
    getUserTooltip: function () {
        if (this.nfRegistryService.currentUser.anonymous) {
            return 'Please configure NiFi Registry security to enable.';
        }

        if (!this.nfRegistryService.currentUser.resourcePermissions.tenants.canRead) {
            return 'You do not have permission. Please contact your System Administrator.';
        }
        return 'Manage NiFi Registry users and groups.';
    }
};

NfRegistryAdministration.annotations = [
    new Component({
        templateUrl: './nf-registry-administration.html',
        animations: [nfRegistryAnimations.slideInLeftAnimation],
        host: {
            '[@routeAnimation]': 'routeAnimation'
        }
    })
];

NfRegistryAdministration.parameters = [
    NfRegistryService,
    Router
];

export default NfRegistryAdministration;
