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

import NfRegistryService from 'services/nf-registry.service';
import NfStorage from 'services/nf-storage.service';
import { Router } from '@angular/router';
import { FdsDialogService } from '@nifi-fds/core';
import NfRegistryApi from 'services/nf-registry.api';

/**
 * NfRegistryUsersAdministrationAuthGuard constructor.
 *
 * @param nfRegistryService         The nfRegistryService module.
 * @param nfRegistryApi             The nfRegistryApi module.
 * @param nfStorage                 The NfStorage module.
 * @param router                    The angular router module.
 * @param fdsDialogService          The FDS dialog service.
 * @constructor
 */
function NfRegistryUsersAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, fdsDialogService) {
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.nfStorage = nfStorage;
    this.router = router;
    this.dialogService = fdsDialogService;
}

NfRegistryUsersAdministrationAuthGuard.prototype = {
    constructor: NfRegistryUsersAdministrationAuthGuard,

    /**
     * Can activate guard.
     * @returns {*}
     */
    canActivate: function (route, state) {
        var url = state.url;

        return this.checkLogin(url);
    },

    checkLogin: function (url) {
        var self = this;
        return new Promise((resolve) => {
            if (this.nfRegistryService.currentUser.resourcePermissions.tenants.canRead) {
                resolve(true);
                return true;
            }

            // Store the attempted URL for redirecting
            this.nfRegistryService.redirectUrl = url;

            // attempt Kerberos or OIDC authentication
            this.nfRegistryApi.ticketExchange().subscribe(function (jwt) {
                self.nfRegistryApi.loadCurrentUser().subscribe(function (currentUser) {
                    // there is no anonymous access and we don't know this user - open the login page which handles login/registration/etc
                    if (currentUser.error) {
                        if (currentUser.error.status === 401) {
                            self.nfStorage.removeItem('jwt');
                            self.router.navigateByUrl('login');
                            resolve(false);
                        }
                    } else {
                        self.nfRegistryService.currentUser = currentUser;
                        if (currentUser.anonymous === false) {
                            // render the logout button if there is a token locally
                            if (self.nfStorage.getItem('jwt') !== null) {
                                self.nfRegistryService.currentUser.canLogout = true;
                            }

                            // redirect to explorer perspective if not admin
                            if (!currentUser.resourcePermissions.anyTopLevelResource.canRead) {
                                self.dialogService.openConfirm({
                                    title: 'Access denied',
                                    message: 'Please contact your system administrator.',
                                    acceptButton: 'Ok',
                                    acceptButtonColor: 'fds-warn'
                                });
                                self.router.navigateByUrl('explorer');
                                resolve(false);
                            } else if (currentUser.resourcePermissions.tenants.canRead) {
                                resolve(true);
                            } else {
                                self.dialogService.openConfirm({
                                    title: 'Access denied',
                                    message: 'Please contact your system administrator.',
                                    acceptButton: 'Ok',
                                    acceptButtonColor: 'fds-warn'
                                });
                                self.router.navigateByUrl('explorer');
                                resolve(false);
                            }
                        } else if (location.protocol === 'http:') {
                            // user is anonymous and we are NOT secure, redirect to workflow perspective
                            self.dialogService.openConfirm({
                                title: 'Not Applicable',
                                message: 'User administration is not configured for this registry.',
                                acceptButton: 'Ok',
                                acceptButtonColor: 'fds-warn'
                            });
                            self.router.navigateByUrl('administration/workflow');
                            resolve(false);
                        } else {
                            if (self.nfRegistryService.currentUser.resourcePermissions.tenants.canRead) {
                                resolve(true);
                                return true;
                            }

                            // user is anonymous and we are secure so don't allow the url, navigate to the main page
                            self.dialogService.openConfirm({
                                title: 'Access denied',
                                message: 'Please contact your system administrator.',
                                acceptButton: 'Ok',
                                acceptButtonColor: 'fds-warn'
                            });
                            self.router.navigateByUrl('explorer');
                            resolve(false);
                        }
                    }
                });
            });
        });
    }
};

NfRegistryUsersAdministrationAuthGuard.parameters = [
    NfRegistryService,
    NfRegistryApi,
    NfStorage,
    Router,
    FdsDialogService
];

/**
 * NfRegistryWorkflowsAdministrationAuthGuard constructor.
 *
 * @param nfRegistryService         The nfRegistryService module.
 * @param nfRegistryApi             The nfRegistryApi module.
 * @param nfStorage                 The NfStorage module.
 * @param router                    The angular router module.
 * @param fdsDialogService          The FDS dialog service.
 * @constructor
 */
function NfRegistryWorkflowsAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, fdsDialogService) {
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.nfStorage = nfStorage;
    this.router = router;
    this.dialogService = fdsDialogService;
}

NfRegistryWorkflowsAdministrationAuthGuard.prototype = {
    constructor: NfRegistryWorkflowsAdministrationAuthGuard,

    /**
     * Can activate guard.
     * @returns {*}
     */
    canActivate: function (route, state) {
        var url = state.url;

        return this.checkLogin(url);
    },

    checkLogin: function (url) {
        var self = this;
        return new Promise((resolve) => {
            if (this.nfRegistryService.currentUser.resourcePermissions.buckets.canRead) {
                resolve(true);
                return;
            }

            // Store the attempted URL for redirecting
            this.nfRegistryService.redirectUrl = url;

            // attempt Kerberos or OIDC authentication
            this.nfRegistryApi.ticketExchange().subscribe(function (jwt) {
                self.nfRegistryApi.loadCurrentUser().subscribe(function (currentUser) {
                    // there is no anonymous access and we don't know this user - open the login page which handles login/registration/etc
                    if (currentUser.error) {
                        if (currentUser.error.status === 401) {
                            self.nfStorage.removeItem('jwt');
                            self.router.navigateByUrl('login');
                            resolve(false);
                        }
                    } else {
                        self.nfRegistryService.currentUser = currentUser;
                        if (currentUser.anonymous === false) {
                            // render the logout button if there is a token locally
                            if (self.nfStorage.getItem('jwt') !== null) {
                                self.nfRegistryService.currentUser.canLogout = true;
                            }

                            // redirect to explorer perspective if not admin
                            if (!currentUser.resourcePermissions.anyTopLevelResource.canRead) {
                                self.dialogService.openConfirm({
                                    title: 'Access denied',
                                    message: 'Please contact your system administrator.',
                                    acceptButton: 'Ok',
                                    acceptButtonColor: 'fds-warn'
                                });
                                self.router.navigateByUrl('explorer');
                                resolve(false);
                            } else if (currentUser.resourcePermissions.buckets.canRead) {
                                resolve(true);
                            } else {
                                self.dialogService.openConfirm({
                                    title: 'Access denied',
                                    message: 'Please contact your system administrator.',
                                    acceptButton: 'Ok',
                                    acceptButtonColor: 'fds-warn'
                                });
                                self.router.navigateByUrl('explorer');
                                resolve(false);
                            }
                        } else if (location.protocol === 'http:') {
                            // user is anonymous and we are NOT secure so allow the url
                            resolve(true);
                        } else {
                            if (self.nfRegistryService.currentUser.resourcePermissions.buckets.canRead) {
                                resolve(true);
                                return;
                            }

                            // user is anonymous and we are secure so don't allow the url, navigate to the main page
                            self.dialogService.openConfirm({
                                title: 'Access denied',
                                message: 'Please contact your system administrator.',
                                acceptButton: 'Ok',
                                acceptButtonColor: 'fds-warn'
                            });
                            self.router.navigateByUrl('explorer');
                            resolve(false);
                        }
                    }
                });
            });
        });
    }
};

NfRegistryWorkflowsAdministrationAuthGuard.parameters = [
    NfRegistryService,
    NfRegistryApi,
    NfStorage,
    Router,
    FdsDialogService
];

/**
 * NfRegistryLoginAuthGuard constructor.
 *
 * @param nfRegistryService         The nfRegistryService module.
 * @param nfRegistryApi             The nfRegistryApi module.
 * @param nfStorage                 The NfStorage module.
 * @param router                    The angular router module.
 * @constructor
 */
function NfRegistryLoginAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router) {
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.nfStorage = nfStorage;
    this.router = router;
}

NfRegistryLoginAuthGuard.prototype = {
    constructor: NfRegistryLoginAuthGuard,

    /**
     * Can activate guard.
     * @returns {*}
     */
    canActivate: function (route, state) {
        var url = state.url;

        return this.checkLogin(url);
    },

    checkLogin: function (url) {
        var self = this;
        return new Promise((resolve) => {
            if (this.nfRegistryService.currentUser.anonymous) {
                resolve(true);
                return;
            }
            // attempt Kerberos or OIDC authentication
            this.nfRegistryApi.ticketExchange().subscribe(function (jwt) {
                self.nfRegistryApi.loadCurrentUser().subscribe(function (currentUser) {
                    self.nfRegistryService.currentUser = currentUser;
                    if (currentUser.anonymous === false) {
                        // render the logout button if there is a token locally
                        if (self.nfStorage.getItem('jwt') !== null) {
                            self.nfRegistryService.currentUser.canLogout = true;
                        }
                        self.nfRegistryService.currentUser.canActivateResourcesAuthGuard = true;
                        resolve(false);
                        self.router.navigateByUrl(self.nfRegistryService.redirectUrl);
                    } else if (self.nfRegistryService.currentUser.anonymous && !self.nfRegistryService.currentUser.loginSupported) {
                        resolve(false);
                        self.router.navigateByUrl('/nifi-registry');
                    } else {
                        self.nfRegistryService.currentUser.anonymous = true;
                        resolve(true);
                    }
                });
            });
        });
    }
};

NfRegistryLoginAuthGuard.parameters = [
    NfRegistryService,
    NfRegistryApi,
    NfStorage,
    Router
];

/**
 * NfRegistryResourcesAuthGuard constructor.
 *
 * @param nfRegistryService         The nfRegistryService module.
 * @param nfRegistryApi             The nfRegistryApi module.
 * @param nfStorage                 The NfStorage module.
 * @param router                    The angular router module.
 * @constructor
 */
function NfRegistryResourcesAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router) {
    this.nfRegistryService = nfRegistryService;
    this.nfRegistryApi = nfRegistryApi;
    this.nfStorage = nfStorage;
    this.router = router;
}

NfRegistryResourcesAuthGuard.prototype = {
    constructor: NfRegistryResourcesAuthGuard,

    /**
     * Can activate guard.
     * @returns {*}
     */
    canActivate: function (route, state) {
        var url = state.url;
        return this.checkLogin(url);
    },

    checkLogin: function (url) {
        var self = this;
        return new Promise((resolve) => {
            if (this.nfRegistryService.currentUser.canActivateResourcesAuthGuard === true) {
                resolve(true);
                return;
            }

            // Store the attempted URL for redirecting
            this.nfRegistryService.redirectUrl = url;

            // attempt Kerberos or OIDC authentication
            this.nfRegistryApi.ticketExchange().subscribe(function (jwt) {
                self.nfRegistryApi.loadCurrentUser().subscribe(function (currentUser) {
                    // there is no anonymous access and we don't know this user - open the login page which handles login/registration/etc
                    if (currentUser.error) {
                        if (currentUser.error.status === 401) {
                            self.nfStorage.removeItem('jwt');
                            self.router.navigateByUrl('login');
                            resolve(false);
                        }
                    } else {
                        self.nfRegistryService.currentUser = currentUser;
                        if (!currentUser || currentUser.anonymous === false) {
                            if (self.nfStorage.hasItem('jwt')) {
                                self.nfRegistryService.currentUser.canLogout = true;
                                self.nfRegistryService.currentUser.canActivateResourcesAuthGuard = true;
                                resolve(true);
                            } else {
                                self.router.navigateByUrl('login');
                                resolve(false);
                            }
                        } else if (currentUser.anonymous === true) {
                            // render the logout button if there is a token locally
                            if (self.nfStorage.getItem('jwt') !== null) {
                                self.nfRegistryService.currentUser.canLogout = true;
                            }
                            self.nfRegistryService.currentUser.canActivateResourcesAuthGuard = true;
                            resolve(true);
                        }
                    }
                });
            });
        });
    }
};

NfRegistryResourcesAuthGuard.parameters = [
    NfRegistryService,
    NfRegistryApi,
    NfStorage,
    Router
];

export {
    NfRegistryUsersAdministrationAuthGuard,
    NfRegistryWorkflowsAdministrationAuthGuard,
    NfRegistryLoginAuthGuard,
    NfRegistryResourcesAuthGuard
};
