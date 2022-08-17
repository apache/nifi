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

import {TestBed, fakeAsync, tick} from '@angular/core/testing';
import initTestBed from 'nf-registry.testbed-factory';
import {of} from 'rxjs';
import NfRegistryApi from 'services/nf-registry.api';
import NfRegistryService from 'services/nf-registry.service';

import NfRegistryManageGroup from 'components/administration/users/sidenav/manage-group/nf-registry-manage-group';
import {ActivatedRoute} from '@angular/router';

describe('NfRegistryManageGroup Component', function () {
    let comp;
    let fixture;
    let nfRegistryService;
    let nfRegistryApi;

    beforeEach((done) => {
        const providers = [
            {
                provide: ActivatedRoute,
                useValue: {
                    params: of({groupId: '123'})
                }
            }
        ];

        initTestBed({providers})
            .then(() => {
                fixture = TestBed.createComponent(NfRegistryManageGroup);

                // test instance
                comp = fixture.componentInstance;

                // from the root injector
                nfRegistryService = TestBed.get(NfRegistryService);
                nfRegistryApi = TestBed.get(NfRegistryApi);

                // because the NfRegistryManageGroup component is a nested route component we need to set up the nfRegistryService service manually
                nfRegistryService.sidenav = {
                    open: function () {
                    },
                    close: function () {
                    }
                };
                nfRegistryService.group = {
                    identifier: 999,
                    identity: 'Group #1',
                    users: [{
                        identifier: '123',
                        identity: 'Group #1',
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
                    }],
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
                nfRegistryService.groups = [nfRegistryService.group];

                //Spy
                spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {
                }).and.returnValue(of({}));
                spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
                }).and.returnValue(of({}));

                done();
            });
    });

    it('should have a defined component', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.group.identifier).toEqual('123');

        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
    }));

    it('should FAIL to get user by id and redirect to admin users perspective', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const routerCall = comp.router.navigateByUrl.calls.first();
        expect(routerCall.args[0]).toBe('administration/users');
        expect(comp.router.navigateByUrl.calls.count()).toBe(1);
    }));

    it('should FAIL to get user by id and redirect to workflow perspective', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            status: 409
        }));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const routerCall = comp.router.navigateByUrl.calls.first();
        expect(routerCall.args[0]).toBe('administration/workflow');
        expect(comp.router.navigateByUrl.calls.count()).toBe(1);
    }));

    it('should redirect to users perspective', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        // the function to test
        comp.closeSideNav();

        //assertions
        const routerCall = comp.router.navigateByUrl.calls.first();
        expect(routerCall.args[0]).toBe('administration/users');
        expect(comp.router.navigateByUrl.calls.count()).toBe(1);
    }));

    it('should toggle to create the manage bucket privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 404,
            userGroups: []
        }));
        spyOn(nfRegistryApi, 'postPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManageBucketsPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.postPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to update the manage bucket privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: []
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManageBucketsPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to remove the manage bucket privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 400,
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManageBucketsPrivileges({
            checked: false
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to create the manage proxy privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 404,
            userGroups: []
        }));
        spyOn(nfRegistryApi, 'postPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManageProxyPrivileges({
            checked: true
        }, 'write');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.postPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to update the manage proxy privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: []
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManageProxyPrivileges({
            checked: true
        }, 'write');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to remove the manage proxy privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 400,
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManageProxyPrivileges({
            checked: false
        }, 'write');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to create the manage policies privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 404,
            userGroups: []
        }));
        spyOn(nfRegistryApi, 'postPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManagePoliciesPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.postPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to update the manage policies privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: []
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManagePoliciesPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to remove the manage policies privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 400,
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManagePoliciesPrivileges({
            checked: false
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to create the manage tenants privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 404,
            userGroups: []
        }));
        spyOn(nfRegistryApi, 'postPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManageTenantsPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.postPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to update the manage tenants privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: []
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManageTenantsPrivileges({
            checked: true
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should toggle to remove the manage tenants privileges for the current group', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            status: 400,
            userGroups: [
                {
                    identifier: '123',
                    identity: 'Group #1',
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
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.toggleGroupManageTenantsPrivileges({
            checked: false
        }, 'read');

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(1);
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
    }));

    it('should open a modal dialog UX enabling the addition of the current user to a group(s)', fakeAsync(function () {
        // Spy
        spyOn(comp, 'filterUsers').and.callFake(function () {
        });
        spyOn(comp.dialog, 'open').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of({});
                }
            };
        });
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.addUsersToGroup();

        //assertions
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
        expect(comp.filterUsers).toHaveBeenCalled();
    }));

    it('should sort `users` by `column`', fakeAsync(function () {
        spyOn(comp, 'filterUsers').and.callFake(function () {
        });
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the async calls
        fixture.detectChanges();

        // object to be updated by the test
        const column = {name: 'name', label: 'Display Name', sortable: true};

        // The function to test
        comp.sortUsers(column);

        //assertions
        expect(column.active).toBe(true);
        const filterUsersCall = comp.filterUsers.calls.first();
        expect(filterUsersCall.args[0]).toBe('identity');
        expect(filterUsersCall.args[1]).toBe('ASC');
    }));

    it('should remove user from group', fakeAsync(function () {
        // Spy
        spyOn(comp, 'filterUsers').and.callFake(function () {
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        const user = {
            identifier: '123'
        };

        // the function to test
        comp.removeUserFromGroup(user);

        //assertions
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(2);
        expect(nfRegistryApi.updateUserGroup.calls.count()).toBe(1);
        expect(comp.snackBarService.openCoaster.calls.count()).toBe(1);
        expect(comp.filterUsers).toHaveBeenCalled();
    }));

    it('should update group name', fakeAsync(function () {
        // Spy
        spyOn(comp.dialogService, 'openConfirm').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of(true);
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));
        spyOn(nfRegistryApi, 'updateUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'test',
            status: 200
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.updateGroupName('test');

        //assertions
        expect(comp.snackBarService.openCoaster.calls.count()).toBe(1);
        expect(comp.nfRegistryService.group.identity).toBe('test');
    }));

    it('should fail to update group name (409)', fakeAsync(function () {
        // Spy
        spyOn(comp.dialogService, 'openConfirm').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of(true);
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));
        spyOn(nfRegistryApi, 'updateUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'test',
            status: 409
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();

        //assertions
        const getUserGroupCall = nfRegistryApi.getUserGroup.calls.first();
        expect(getUserGroupCall.args[0]).toBe('123');
        expect(nfRegistryApi.getUserGroup.calls.count()).toBe(1);

        // the function to test
        comp.updateGroupName('test');

        //assertions
        expect(comp.dialogService.openConfirm.calls.count()).toBe(1);
        expect(comp.nfRegistryService.group.identity).toBe('Group #1');
    }));

    it('should destroy the component', fakeAsync(function () {
        spyOn(nfRegistryService.sidenav, 'close');
        spyOn(nfRegistryApi, 'getUserGroup').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            identity: 'Group #1',
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
            },
            users: [{
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
            }]
        }));
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getUserGroup calls
        fixture.detectChanges();
        spyOn(comp.$subscription, 'unsubscribe');

        // The function to test
        comp.ngOnDestroy();

        //assertions
        expect(nfRegistryService.sidenav.close).toHaveBeenCalled();
        expect(nfRegistryService.group.identity).toBe('Group #1');
        expect(comp.$subscription.unsubscribe).toHaveBeenCalled();
    }));
});
