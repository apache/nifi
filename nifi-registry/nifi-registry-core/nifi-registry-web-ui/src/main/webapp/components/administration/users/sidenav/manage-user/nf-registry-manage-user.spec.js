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

import { TestBed, fakeAsync, tick } from '@angular/core/testing';
import initTestBed from 'nf-registry.testbed-factory';
import { of } from 'rxjs';
import NfRegistryApi from 'services/nf-registry.api';
import NfRegistryService from 'services/nf-registry.service';
import { ActivatedRoute } from '@angular/router';

import NfRegistryManageUser from 'components/administration/users/sidenav/manage-user/nf-registry-manage-user';

describe('NfRegistryManageUser Component', function () {
    let comp;
    let fixture;
    let nfRegistryService;
    let nfRegistryApi;

    beforeEach((done) => {
        const providers = [
            {
                provide: ActivatedRoute,
                useValue: {
                    params: of({userId: '123'})
                }
            }
        ];

        initTestBed({providers})
            .then(() => {
                fixture = TestBed.createComponent(NfRegistryManageUser);

                // test instance
                comp = fixture.componentInstance;

                // from the root injector
                nfRegistryService = TestBed.get(NfRegistryService);
                nfRegistryApi = TestBed.get(NfRegistryApi);

                // because the NfRegistryManageUser component is a nested route component we need to set up the nfRegistryService service manually
                nfRegistryService.sidenav = {
                    open: function () {
                    },
                    close: function () {
                    }
                };
                nfRegistryService.user = {
                    identifier: 999,
                    identity: 'User #1',
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
                };
                nfRegistryService.users = [nfRegistryService.user];

                //Spy
                spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {
                }).and.returnValue(of({}));
                spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
                }).and.returnValue(of({}));

                done();
            });
    });

    it('should have a defined component', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.user.identifier).toEqual('123');

        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
    }));

    it('should FAIL to get user by id and redirect to admin users perspective', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const routerCall = comp.router.navigateByUrl.calls.first();
        expect(routerCall.args[0]).toBe('administration/users');
        expect(comp.router.navigateByUrl.calls.count()).toBe(1);
    }));

    it('should FAIL to get user by id and redirect to workflow perspective', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            status: 409
        }));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const routerCall = comp.router.navigateByUrl.calls.first();
        expect(routerCall.args[0]).toBe('administration/workflow');
        expect(comp.router.navigateByUrl.calls.count()).toBe(1);
    }));

    it('should redirect to users perspective', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        // the function to test
        comp.closeSideNav();

        //assertions
        const routerCall = comp.router.navigateByUrl.calls.first();
        expect(routerCall.args[0]).toBe('administration/users');
        expect(comp.router.navigateByUrl.calls.count()).toBe(1);
    }));

    it('should toggle to create the manage bucket privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(nfRegistryApi, 'postPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManageBucketsPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.postPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to update the manage bucket privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: []
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManageBucketsPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to remove the manage bucket privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 400,
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({}));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManageBucketsPrivileges({
            checked: false
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to create the manage proxy privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(nfRegistryApi, 'postPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManageProxyPrivileges({
            checked: true
        }, 'write');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.postPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to update the manage proxy privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: []
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManageProxyPrivileges({
            checked: true
        }, 'write');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to remove the manage proxy privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 400,
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({}));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManageProxyPrivileges({
            checked: false
        }, 'write');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to create the manage policies privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(nfRegistryApi, 'postPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManagePoliciesPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.postPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to update the manage policies privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: []
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManagePoliciesPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to remove the manage policies privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 400,
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({}));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManagePoliciesPrivileges({
            checked: false
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to create the manage tenants privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(nfRegistryApi, 'postPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManageTenantsPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.postPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to update the manage tenants privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: []
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManageTenantsPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should toggle to remove the manage tenants privileges for the current user', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 400,
            users: [
                {
                    identifier: '123',
                    identity: 'User #1',
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
                }
            ]
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({}));
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.toggleUserManageTenantsPrivileges({
            checked: false
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
    }));

    it('should open a modal dialog UX enabling the addition of the current user to a group(s)', fakeAsync(function () {
        // Spy
        spyOn(comp, 'filterGroups').and.callFake(function () {
        });
        spyOn(comp.dialog, 'open').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of({});
                }
            };
        });
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.addUserToGroups();

        //assertions
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
        expect(comp.filterGroups).toHaveBeenCalled();
    }));

    it('should sort `groups` by `column`', fakeAsync(function () {
        spyOn(comp, 'filterGroups').and.callFake(function () {
        });
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the async calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // object to be updated by the test
        const column = {name: 'name', label: 'Display Name', sortable: true};

        // The function to test
        comp.sortGroups(column);

        //assertions
        expect(column.active).toBe(true);
        const filterGroupsCall = comp.filterGroups.calls.first();
        expect(filterGroupsCall.args[0]).toBe('identity');
        expect(filterGroupsCall.args[1]).toBe('ASC');
    }));

    it('should remove user from group', fakeAsync(function () {
        // Spy
        spyOn(comp, 'filterGroups').and.callFake(function () {
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            users: [{
                identity: 'User #1'
            }],
            userGroups: [{
                identity: 'Group #1'
            }]
        }));
        spyOn(nfRegistryApi, 'updateUserGroup').and.callFake(function () {
        }).and.returnValue(of({}));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        const group = {
            identifier: '123'
        };

        // the function to test
        comp.removeUserFromGroup(group);

        //assertions
        expect(nfRegistryApi.getUser.calls.count()).toBe(2);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);
        expect(nfRegistryApi.updateUserGroup.calls.count()).toBe(1);
        expect(comp.snackBarService.openCoaster.calls.count()).toBe(1);
        expect(comp.filterGroups).toHaveBeenCalled();
    }));

    it('should update user name', fakeAsync(function () {
        // Spy
        spyOn(comp.dialogService, 'openConfirm').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of(true);
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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
        spyOn(nfRegistryApi, 'updateUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'test',
            status: 200
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.updateUserName('test');

        //assertions
        expect(comp.snackBarService.openCoaster.calls.count()).toBe(1);
        expect(comp.nfRegistryService.user.identity).toBe('test');
    }));

    it('should fail to update user name (409)', fakeAsync(function () {
        // Spy
        spyOn(comp.dialogService, 'openConfirm').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of(true);
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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
        spyOn(nfRegistryApi, 'updateUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'test',
            status: 409
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();

        //assertions
        const getUserCall = nfRegistryApi.getUser.calls.first();
        expect(getUserCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUser.calls.count()).toBe(1);

        // the function to test
        comp.updateUserName('test');

        //assertions
        expect(comp.dialogService.openConfirm.calls.count()).toBe(1);
        expect(comp.nfRegistryService.user.identity).toBe('User #1');
    }));

    it('should destroy the component', fakeAsync(function () {
        spyOn(nfRegistryService.sidenav, 'close');
        spyOn(nfRegistryApi, 'getUser').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'User #1',
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
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUser calls
        fixture.detectChanges();
        spyOn(comp.$subscription, 'unsubscribe');

        // The function to test
        comp.ngOnDestroy();

        //assertions
        expect(nfRegistryService.sidenav.close).toHaveBeenCalled();
        expect(nfRegistryService.user.identity).toBe('User #1');
        expect(comp.$subscription.unsubscribe).toHaveBeenCalled();
    }));
});
