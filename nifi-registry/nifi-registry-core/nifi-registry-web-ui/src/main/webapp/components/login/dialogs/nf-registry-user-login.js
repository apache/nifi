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
import { MatDialogRef } from '@angular/material';
import { NfRegistryLoginAuthGuard } from 'services/nf-registry.auth-guard.service';
import { Router } from '@angular/router';

/**
 * NfRegistryUserLogin constructor.
 *
 * @param nfRegistryApi                     The api service.
 * @param nfRegistryService                 The nf-registry.service module.
 * @param matDialogRef                      The angular material dialog ref.
 * @param nfRegistryLoginAuthGuard          The login auth guard.
 * @param router                    The angular router module.
 * @constructor
 */
function NfRegistryUserLogin(nfRegistryApi, nfRegistryService, matDialogRef, nfRegistryLoginAuthGuard, router) {
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.dialogRef = matDialogRef;
    this.nfRegistryLoginAuthGuard = nfRegistryLoginAuthGuard;
    this.router = router;
}

NfRegistryUserLogin.prototype = {
    constructor: NfRegistryUserLogin,

    /**
     * Submit login form.
     *
     * @param username  The user name.
     * @param password  The password.
     */
    login: function (username, password) {
        if (username.value.length === 0 || password.value.length === 0) {
            return;
        }
        var self = this;
        this.nfRegistryApi.postToLogin(username.value, password.value).subscribe(function (response) {
            if (response || response.status === 200) {
                //successful login update registry config
                self.nfRegistryApi.getRegistryConfig().subscribe(function (registryConfig) {
                    self.nfRegistryService.registry.config = registryConfig;
                    self.nfRegistryService.currentUser.anonymous = false;
                    self.dialogRef.close();
                    self.nfRegistryLoginAuthGuard.checkLogin(self.nfRegistryService.redirectUrl);
                });
            }
        });
    },

    cancel: function () {
        var self = this;
        self.dialogRef.close();
        self.router.navigateByUrl('/nifi-registry');
    }
};

NfRegistryUserLogin.annotations = [
    new Component({
        templateUrl: './nf-registry-user-login.html'
    })
];

NfRegistryUserLogin.parameters = [
    NfRegistryApi,
    NfRegistryService,
    MatDialogRef,
    NfRegistryLoginAuthGuard,
    Router
];

export default NfRegistryUserLogin;
