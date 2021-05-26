/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the 'License'); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
    NfRegistryWorkflowsAdministrationAuthGuard,
    NfRegistryUsersAdministrationAuthGuard,
    NfRegistryResourcesAuthGuard,
    NfRegistryLoginAuthGuard
} from 'services/nf-registry.auth-guard.service';
import NfRegistryService from 'services/nf-registry.service';
import NfStorage from 'services/nf-storage.service';
import { of } from 'rxjs';

describe('NfRegistry Auth Guard Service NfRegistryResourcesAuthGuard isolated unit tests', function () {
    var nfRegistryService;
    var nfRegistryResourcesAuthGuard;
    var nfRegistryApi;
    var nfStorage;
    var router;
    var dialogService;

    beforeEach(function () {
        router = {
            navigateByUrl: function () {
            }
        };
        dialogService = {
            openConfirm: function () {
            }
        };
        nfRegistryApi = {
            ticketExchange: function () {
            },
            loadCurrentUser: function () {
            }
        };
        nfStorage = new NfStorage();
        nfRegistryService = new NfRegistryService(nfRegistryApi, nfStorage, {}, router, dialogService, {});

        // Spy
        spyOn(router, 'navigateByUrl');
        spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {
        }).and.returnValue(of({}));
        spyOn(dialogService, 'openConfirm');
    });

    it('should navigate to test url (registry security not configured) ', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: true
        }));
        spyOn(nfStorage, 'getItem').and.callFake(function () {
        }).and.returnValue(true);

        nfRegistryResourcesAuthGuard = new NfRegistryResourcesAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router);

        // The function to test
        nfRegistryResourcesAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(nfRegistryService.currentUser.canLogout).toBe(true);
                expect(nfRegistryService.currentUser.canActivateResourcesAuthGuard).toBe(true);
                expect(nfRegistryService.currentUser.anonymous).toBe(true);
                expect(canActivate).toBe(true);
                done();
            });
    });

    it('should navigate to test url (registry security configured and we know who you are) ', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: false
        }));
        spyOn(nfStorage, 'hasItem').and.callFake(function () {
        }).and.returnValue(true);

        nfRegistryResourcesAuthGuard = new NfRegistryResourcesAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router);

        // The function to test
        nfRegistryResourcesAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(nfRegistryService.currentUser.canLogout).toBe(true);
                expect(nfRegistryService.currentUser.canActivateResourcesAuthGuard).toBe(true);
                expect(canActivate).toBe(true);
                done();
            });
    });

    it('should navigate to login', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: false
        }));

        nfRegistryResourcesAuthGuard = new NfRegistryResourcesAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router);

        // The function to test
        nfRegistryResourcesAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('login');
                done();
            });
    });

    it('should navigate to login (error loading current user)', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            error: {
                status: 401
            }
        }));
        spyOn(nfStorage, 'removeItem').and.callFake(() => {});

        nfRegistryResourcesAuthGuard = new NfRegistryResourcesAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryResourcesAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfStorage.removeItem).toHaveBeenCalled();
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('login');
                done();
            });
    });
});

describe('NfRegistry Auth Guard Service NfRegistryLoginAuthGuard isolated unit tests', function () {
    var nfRegistryService;
    var nfRegistryLoginAuthGuard;
    var nfRegistryApi;
    var nfStorage;
    var router;
    var dialogService;

    beforeEach(function () {
        router = {
            navigateByUrl: function () {
            }
        };
        dialogService = {
            openConfirm: function () {
            }
        };
        nfRegistryApi = {
            ticketExchange: function () {
            },
            loadCurrentUser: function () {
            }
        };
        nfStorage = new NfStorage();
        nfRegistryService = new NfRegistryService(nfRegistryApi, nfStorage, {}, router, dialogService, {});

        // Spy
        spyOn(router, 'navigateByUrl');
        spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {
        }).and.returnValue(of({}));
        spyOn(dialogService, 'openConfirm');
    });

    it('should navigate to base nifi-registry url', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: true
        }));
        nfRegistryLoginAuthGuard = new NfRegistryLoginAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router);

        // The function to test
        nfRegistryLoginAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfRegistryService.currentUser.anonymous).toBe(true);
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('/nifi-registry');
                done();
            });
    });

    it('should navigate to test url', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: false
        }));
        nfRegistryLoginAuthGuard = new NfRegistryLoginAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router);

        // The function to test
        nfRegistryLoginAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfRegistryService.currentUser.canActivateResourcesAuthGuard).toBe(true);
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('explorer/grid-list');
                done();
            });
    });
});

describe('NfRegistry Auth Guard Service NfRegistryUsersAdministrationAuthGuard isolated unit tests', function () {
    var nfRegistryService;
    var nfRegistryUsersAdministrationAuthGuard;
    var nfRegistryApi;
    var nfStorage;
    var router;
    var dialogService;

    beforeEach(function () {
        router = {
            navigateByUrl: function () {
            }
        };
        dialogService = {
            openConfirm: function () {
            }
        };
        nfRegistryApi = {
            ticketExchange: function () {
            },
            loadCurrentUser: function () {
            }
        };
        nfStorage = new NfStorage();
        nfRegistryService = new NfRegistryService(nfRegistryApi, nfStorage, {}, router, dialogService, {});

        // Spy
        spyOn(router, 'navigateByUrl');
        spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {
        }).and.returnValue(of({}));
        spyOn(dialogService, 'openConfirm');
    });

    it('should navigate to login', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            error: {
                status: 401
            }
        }));
        nfRegistryUsersAdministrationAuthGuard = new NfRegistryUsersAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryUsersAdministrationAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfRegistryService.redirectUrl).toBe('test');
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('login');
                done();
            });
    });

    it('should deny access (registry security not configured) and navigate to administration workflow perspective', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: true
        }));
        nfRegistryUsersAdministrationAuthGuard = new NfRegistryUsersAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryUsersAdministrationAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfRegistryService.redirectUrl).toBe('test');
                var dialogServiceCall = dialogService.openConfirm.calls.first();
                expect(dialogServiceCall.args[0].title).toBe('Not Applicable');
                expect(dialogServiceCall.args[0].message).toBe('User administration is not configured for this registry.');
                expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
                expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('administration/workflow');
                done();
            });
    });

    it('should deny access (non-admin) and navigate to explorer perspective', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: false,
            resourcePermissions: {
                anyTopLevelResource: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                buckets: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                tenants: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                policies: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                proxy: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                }
            }
        }));
        nfRegistryUsersAdministrationAuthGuard = new NfRegistryUsersAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryUsersAdministrationAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfRegistryService.redirectUrl).toBe('test');
                var dialogServiceCall = dialogService.openConfirm.calls.first();
                expect(dialogServiceCall.args[0].title).toBe('Access denied');
                expect(dialogServiceCall.args[0].message).toBe('Please contact your system administrator.');
                expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
                expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('explorer');
                done();
            });
    });

    it('should deny access (no tenants permissions) and navigate to explorer perspective', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: false,
            resourcePermissions: {
                anyTopLevelResource: {
                    canRead: true,
                    canWrite: false,
                    canDelete: false
                },
                buckets: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                tenants: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                policies: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                proxy: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                }
            }
        }));
        nfRegistryUsersAdministrationAuthGuard = new NfRegistryUsersAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryUsersAdministrationAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfRegistryService.redirectUrl).toBe('test');
                var dialogServiceCall = dialogService.openConfirm.calls.first();
                expect(dialogServiceCall.args[0].title).toBe('Access denied');
                expect(dialogServiceCall.args[0].message).toBe('Please contact your system administrator.');
                expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
                expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('explorer');
                done();
            });
    });

    it('should deny access (no tenants permissions) and navigate to explorer url', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: false,
            resourcePermissions: {
                anyTopLevelResource: {
                    canRead: true,
                    canWrite: false,
                    canDelete: false
                },
                buckets: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                tenants: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                policies: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                proxy: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                }
            }
        }));
        nfRegistryUsersAdministrationAuthGuard = new NfRegistryUsersAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryUsersAdministrationAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfRegistryService.redirectUrl).toBe('test');
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('explorer');
                done();
            });
    });
});

describe('NfRegistry Auth Guard Service NfRegistryWorkflowsAdministrationAuthGuard isolated unit tests', function () {
    var nfRegistryService;
    var nfRegistryWorkflowsAdministrationAuthGuard;
    var nfRegistryApi;
    var nfStorage;
    var router;
    var dialogService;

    beforeEach(function () {
        router = {
            navigateByUrl: function () {
            }
        };
        dialogService = {
            openConfirm: function () {
            }
        };
        nfRegistryApi = {
            ticketExchange: function () {
            },
            loadCurrentUser: function () {
            }
        };
        nfStorage = new NfStorage();
        nfRegistryService = new NfRegistryService(nfRegistryApi, nfStorage, {}, router, dialogService, {});

        // Spy
        spyOn(router, 'navigateByUrl');
        spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {
        }).and.returnValue(of({}));
        spyOn(dialogService, 'openConfirm');
    });

    it('should navigate to login', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            error: {
                status: 401
            }
        }));
        nfRegistryWorkflowsAdministrationAuthGuard = new NfRegistryWorkflowsAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryWorkflowsAdministrationAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfRegistryService.redirectUrl).toBe('test');
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('login');
                done();
            });
    });

    it('should (registry security not configured) navigate to test url', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: true
        }));
        nfRegistryWorkflowsAdministrationAuthGuard = new NfRegistryWorkflowsAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryWorkflowsAdministrationAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(true);
                done();
            });
    });

    it('should deny access (non-admin) and navigate to explorer perspective', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: false,
            resourcePermissions: {
                anyTopLevelResource: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                buckets: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                tenants: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                policies: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                proxy: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                }
            }
        }));
        nfRegistryWorkflowsAdministrationAuthGuard = new NfRegistryWorkflowsAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryWorkflowsAdministrationAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfRegistryService.redirectUrl).toBe('test');
                var dialogServiceCall = dialogService.openConfirm.calls.first();
                expect(dialogServiceCall.args[0].title).toBe('Access denied');
                expect(dialogServiceCall.args[0].message).toBe('Please contact your system administrator.');
                expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
                expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('explorer');
                done();
            });
    });

    it('should deny access (no buckets permissions) and navigate to users administration perspective', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: false,
            resourcePermissions: {
                anyTopLevelResource: {
                    canRead: true,
                    canWrite: false,
                    canDelete: false
                },
                buckets: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                tenants: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                policies: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                proxy: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                }
            }
        }));
        nfRegistryWorkflowsAdministrationAuthGuard = new NfRegistryWorkflowsAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryWorkflowsAdministrationAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(false);
                expect(nfRegistryService.redirectUrl).toBe('test');
                var dialogServiceCall = dialogService.openConfirm.calls.first();
                expect(dialogServiceCall.args[0].title).toBe('Access denied');
                expect(dialogServiceCall.args[0].message).toBe('Please contact your system administrator.');
                expect(dialogServiceCall.args[0].acceptButton).toBe('Ok');
                expect(dialogServiceCall.args[0].acceptButtonColor).toBe('fds-warn');
                var navigateByUrlCall = router.navigateByUrl.calls.first();
                expect(navigateByUrlCall.args[0]).toBe('explorer');
                done();
            });
    });

    it('should (no tenants permissions) navigate to test url', function (done) {
        spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
        }).and.returnValue(of({
            anonymous: false,
            resourcePermissions: {
                anyTopLevelResource: {
                    canRead: true,
                    canWrite: false,
                    canDelete: false
                },
                buckets: {
                    canRead: true,
                    canWrite: false,
                    canDelete: false
                },
                tenants: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                policies: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                },
                proxy: {
                    canRead: false,
                    canWrite: false,
                    canDelete: false
                }
            }
        }));
        nfRegistryWorkflowsAdministrationAuthGuard = new NfRegistryWorkflowsAdministrationAuthGuard(nfRegistryService, nfRegistryApi, nfStorage, router, dialogService);

        // The function to test
        nfRegistryWorkflowsAdministrationAuthGuard.canActivate({}, {url: 'test'})
            .then((canActivate) => {
                //assertions
                expect(canActivate).toBe(true);
                done();
            });
    });
});
