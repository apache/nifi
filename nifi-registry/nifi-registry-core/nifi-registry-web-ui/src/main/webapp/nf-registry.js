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

import { Component, ChangeDetectorRef, ViewChild } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import NfRegistryService from 'services/nf-registry.service';
import NfStorage from 'services/nf-storage.service';
import nfRegistryAnimations from 'nf-registry.animations';
import NfRegistryApi from 'services/nf-registry.api';
import { Router } from '@angular/router';

/**
 * NfRegistry constructor.
 *
 * @param http                  The angular http module.
 * @param nfStorage             A wrapper for the browser's local storage.
 * @param nfRegistryService     The registry service.
 * @param nfRegistryApi     The api service.
 * @param changeDetectorRef     The change detector ref.
 * @param router                The angular router module.
 * @constructor
 */
function NfRegistry(http, nfStorage, nfRegistryService, nfRegistryApi, changeDetectorRef, router) {
    this.http = http;
    this.nfStorage = nfStorage;
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.cd = changeDetectorRef;
    this.router = router;
}

NfRegistry.prototype = {
    constructor: NfRegistry,

    /**
     * Initialize the component
     */
    ngOnInit: function () {
        var self = this;
        this.nfRegistryService.sidenav = this.sidenav; //ngCore.ViewChild

        this.nfRegistryApi.getRegistryConfig().subscribe(function (registryConfig) {
            self.nfRegistryService.registry.config = registryConfig;
        });
    },

    /**
     * Since the child views are updating the nfRegistryService values that are used to display
     * the breadcrumbs in this component's view we need to manually detect changes at the correct
     * point in the lifecycle.
     */
    ngAfterViewChecked: function () {
        this.cd.detectChanges();
    },

    /**
     * Invalidate old tokens and route to login page
     */
    logout: function () {
        var self = this;
        self.nfRegistryApi.deleteToLogout('../nifi-registry/logout').subscribe(
            function () {
                // next call
            },
            function () {
                // error callback
            },
            function () {
                // complete callback... clean up and navigate on complete only
                self.nfStorage.removeItem('jwt');
                delete self.nfRegistryService.currentUser.identity;
                delete self.nfRegistryService.currentUser.anonymous;
                self.router.navigateByUrl('login');
            }
        );
    },

    /**
     * Navigate to login route.
     */
    login: function () {
        var self = this;
        if (self.nfRegistryService.currentUser.oidcloginSupported === true) {
            window.location.href = location.origin + '/nifi-registry/login';
        } else {
            self.router.navigateByUrl('login');
        }
    }
};

NfRegistry.annotations = [
    new Component({
        selector: 'nf-registry-app',
        templateUrl: './nf-registry.html',
        queries: {
            sidenav: new ViewChild('sidenav')
        },
        animations: [nfRegistryAnimations.flyInOutAnimation]
    })
];

NfRegistry.parameters = [
    HttpClient,
    NfStorage,
    NfRegistryService,
    NfRegistryApi,
    ChangeDetectorRef,
    Router
];

export default NfRegistry;
