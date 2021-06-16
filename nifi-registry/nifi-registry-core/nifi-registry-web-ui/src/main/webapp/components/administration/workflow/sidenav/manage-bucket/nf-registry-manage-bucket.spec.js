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
import NfRegistryManageBucket from 'components/administration/workflow/sidenav/manage-bucket/nf-registry-manage-bucket';

describe('NfRegistryManageBucket Component', function () {
    let comp;
    let fixture;
    let nfRegistryService;
    let nfRegistryApi;

    beforeEach((done) => {
        const providers = [
            {
                provide: ActivatedRoute,
                useValue: {
                    params: of({bucketId: '123'})
                }
            }
        ];

        initTestBed({providers})
            .then(() => {
                fixture = TestBed.createComponent(NfRegistryManageBucket);

                // test instance
                comp = fixture.componentInstance;

                // from the root injector
                nfRegistryService = TestBed.get(NfRegistryService);
                nfRegistryApi = TestBed.get(NfRegistryApi);

                // because the NfRegistryManageBucket component is a nested route component we need to set up the nfRegistryService service manually
                nfRegistryService.sidenav = {
                    open: function () {
                    },
                    close: function () {
                    }
                };
                nfRegistryService.bucket = {
                    identifier: 999,
                    name: 'Bucket #1'
                };
                nfRegistryService.buckets = [nfRegistryService.bucket];

                //Spy
                spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {
                }).and.returnValue(of({}));
                spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {
                }).and.returnValue(of({}));
                spyOn(nfRegistryService, 'filterDroplets');

                done();
            });
    });

    it('should have a defined component', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: 'string',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        //assertions
        expect(comp).toBeDefined();
        expect(nfRegistryService.bucket.name).toEqual('Bucket #1');
        expect(nfRegistryApi.getPolicies).toHaveBeenCalled();

        const getBucketCall = nfRegistryApi.getBucket.calls.first();
        expect(getBucketCall.args[0]).toBe('123');
    }));

    it('should FAIL to get bucket by id and redirect to workflow perspective', fakeAsync(function () {
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of({
            status: 404
        }));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        //assertions
        const routerCall = comp.router.navigateByUrl.calls.first();
        expect(routerCall.args[0]).toBe('administration/workflow');
        expect(comp.router.navigateByUrl.calls.count()).toBe(1);
    }));

    it('should redirect to workflow perspective', fakeAsync(function () {
        // Spy
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: 'string',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        // the function to test
        comp.closeSideNav();

        //assertions
        const routerCall = comp.router.navigateByUrl.calls.first();
        expect(routerCall.args[0]).toBe('administration/workflow');
        expect(comp.router.navigateByUrl.calls.count()).toBe(1);
    }));

    it('should open a modal dialog UX enabling the creation of a user policy for a specific bucket', fakeAsync(function () {
        // Spy
        spyOn(comp, 'filterPolicies').and.callFake(function () {
        });
        spyOn(comp.dialog, 'open').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of({
                        userOrGroup: {
                            type: 'user'
                        }
                    });
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: '123',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        //assertions
        const getBucketCall = nfRegistryApi.getBucket.calls.first();
        expect(getBucketCall.args[0]).toBe('123');
        expect(nfRegistryApi.getBucket.calls.count()).toBe(1);

        // the function to test
        comp.addPolicy();

        //assertions
        expect(nfRegistryApi.getBucket.calls.count()).toBe(2);
        expect(comp.snackBarService.openCoaster).toHaveBeenCalled();
        expect(comp.filterPolicies).toHaveBeenCalled();
    }));

    it('should open a modal dialog UX enabling the creation of a user group policy for a specific bucket', fakeAsync(function () {
        // Spy
        spyOn(comp, 'filterPolicies').and.callFake(function () {
        });
        spyOn(comp.dialog, 'open').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of({
                        userOrGroup: {
                            type: 'group'
                        }
                    });
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: '123',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        //assertions
        const getBucketCall = nfRegistryApi.getBucket.calls.first();
        expect(getBucketCall.args[0]).toBe('123');
        expect(nfRegistryApi.getBucket.calls.count()).toBe(1);

        // the function to test
        comp.addPolicy();

        //assertions
        expect(nfRegistryApi.getBucket.calls.count()).toBe(2);
        expect(comp.snackBarService.openCoaster).toHaveBeenCalled();
        expect(comp.filterPolicies).toHaveBeenCalled();
    }));

    it('should open a modal dialog UX enabling the edit of a user policy for a specific bucket', fakeAsync(function () {
        // Spy
        spyOn(comp, 'filterPolicies').and.callFake(function () {
        });
        spyOn(comp.dialog, 'open').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of({
                        userOrGroup: {
                            type: 'user'
                        }
                    });
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: '123',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        //assertions
        const getBucketCall = nfRegistryApi.getBucket.calls.first();
        expect(getBucketCall.args[0]).toBe('123');
        expect(nfRegistryApi.getBucket.calls.count()).toBe(1);

        // the function to test
        comp.editPolicy();

        //assertions
        expect(nfRegistryApi.getBucket.calls.count()).toBe(2);
        expect(comp.snackBarService.openCoaster).toHaveBeenCalled();
        expect(comp.filterPolicies).toHaveBeenCalled();
    }));

    it('should open a modal dialog UX enabling the edit of a user group policy for a specific bucket', fakeAsync(function () {
        // Spy
        spyOn(comp, 'filterPolicies').and.callFake(function () {
        });
        spyOn(comp.dialog, 'open').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of({
                        userOrGroup: {
                            type: 'group'
                        }
                    });
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: '123',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        //assertions
        const getBucketCall = nfRegistryApi.getBucket.calls.first();
        expect(getBucketCall.args[0]).toBe('123');
        expect(nfRegistryApi.getBucket.calls.count()).toBe(1);

        // the function to test
        comp.editPolicy();

        //assertions
        expect(nfRegistryApi.getBucket.calls.count()).toBe(2);
        expect(comp.snackBarService.openCoaster).toHaveBeenCalled();
        expect(comp.filterPolicies).toHaveBeenCalled();
    }));

    it('should sort `buckets` by `column`', fakeAsync(function () {
        spyOn(comp, 'filterPolicies').and.callFake(function () {
        });
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: 'string',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the async calls
        fixture.detectChanges();

        // object to be updated by the test
        const column = {name: 'name', label: 'Display Name', sortable: true};

        // The function to test
        comp.sortBuckets(column);

        //assertions
        expect(column.active).toBe(true);
        const filterPoliciesCall = comp.filterPolicies.calls.first();
        expect(filterPoliciesCall.args[0]).toBe('identity');
        expect(filterPoliciesCall.args[1]).toBe('ASC');
    }));

    it('should remove policy from bucket', fakeAsync(function () {
        // Spy
        spyOn(comp, 'filterPolicies').and.callFake(function () {
        });
        spyOn(comp.dialogService, 'openConfirm').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of(true);
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: '456',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));
        spyOn(nfRegistryApi, 'getPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({
            users: [{
                identity: 'User #1'
            }],
            userGroups: [{
                identity: 'Group #1'
            }]
        }));
        spyOn(nfRegistryApi, 'putPolicyActionResource').and.callFake(function () {
        }).and.returnValue(of({}));
        spyOn(comp.router, 'navigateByUrl').and.callFake(function () {
        });
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        //assertions
        const getBucketCall = nfRegistryApi.getBucket.calls.first();
        expect(getBucketCall.args[0]).toBe('123');
        expect(nfRegistryApi.getBucket.calls.count()).toBe(1);

        const userOrGroup = {
            type: 'user',
            identity: '123',
            permissions: 'read, write, delete'
        };

        // the function to test
        comp.removePolicyFromBucket(userOrGroup);

        //assertions
        expect(nfRegistryApi.getPolicyActionResource.calls.count()).toBe(3);
        expect(nfRegistryApi.putPolicyActionResource.calls.count()).toBe(3);
        expect(nfRegistryApi.getPolicies.calls.count()).toBe(4);
        expect(comp.snackBarService.openCoaster.calls.count()).toBe(3);
        expect(comp.filterPolicies).toHaveBeenCalled();
    }));

    it('should update bucket name', fakeAsync(function () {
        // Spy
        spyOn(comp.dialogService, 'openConfirm').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of(true);
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: '456',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));
        spyOn(nfRegistryApi, 'updateBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'test',
            status: 200
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        //assertions
        const getBucketCall = nfRegistryApi.getBucket.calls.first();
        expect(getBucketCall.args[0]).toBe('123');
        expect(nfRegistryApi.getBucket.calls.count()).toBe(1);

        // the function to test
        comp.updateBucketName('test');

        //assertions
        expect(comp.snackBarService.openCoaster.calls.count()).toBe(1);
        expect(comp.nfRegistryService.bucket.name).toBe('test');
    }));

    it('should fail to update bucket name (409)', fakeAsync(function () {
        // Spy
        spyOn(comp.dialogService, 'openConfirm').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of(true);
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: '456',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));
        spyOn(nfRegistryApi, 'updateBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'test',
            status: 409
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        //assertions
        const getBucketCall = nfRegistryApi.getBucket.calls.first();
        expect(getBucketCall.args[0]).toBe('123');
        expect(nfRegistryApi.getBucket.calls.count()).toBe(1);

        // the function to test
        comp.updateBucketName('test');

        //assertions
        expect(comp.dialogService.openConfirm.calls.count()).toBe(1);
        expect(comp.nfRegistryService.bucket.name).toBe('Bucket #1');
    }));

    it('should fail to update bucket name (400)', fakeAsync(function () {
        // Spy
        spyOn(comp.dialogService, 'openConfirm').and.callFake(function () {
            return {
                afterClosed: function () {
                    return of(true);
                }
            };
        });
        spyOn(comp.snackBarService, 'openCoaster');
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: '456',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [{
                    identity: 'User #1'
                }],
                userGroups: [{
                    identity: 'Group #1'
                }]
            }
        ]));
        spyOn(nfRegistryApi, 'updateBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'test',
            status: 400
        }));

        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();

        //assertions
        const getBucketCall = nfRegistryApi.getBucket.calls.first();
        expect(getBucketCall.args[0]).toBe('123');
        expect(nfRegistryApi.getBucket.calls.count()).toBe(1);

        // the function to test
        comp.updateBucketName('test');

        //assertions
        expect(comp.dialogService.openConfirm.calls.count()).toBe(1);
        expect(comp.nfRegistryService.bucket.name).toBe('Bucket #1');
    }));

    it('should destroy the component', fakeAsync(function () {
        spyOn(nfRegistryService.sidenav, 'close');
        spyOn(nfRegistryApi, 'getBucket').and.callFake(function () {
        }).and.returnValue(of({
            identifier: '123',
            name: 'Bucket #1'
        }));
        spyOn(nfRegistryApi, 'getPolicies').and.callFake(function () {
        }).and.returnValue(of([
            {
                identifier: 'string',
                resource: '/buckets/123',
                action: 'READ',
                configurable: true,
                users: [],
                userGroups: []
            }
        ]));
        // 1st change detection triggers ngOnInit
        fixture.detectChanges();
        // wait for async calls
        tick();
        // 2nd change detection completes after the getBucket and getPolicies calls
        fixture.detectChanges();
        spyOn(comp.$subscription, 'unsubscribe');

        // The function to test
        comp.ngOnDestroy();

        //assertions
        expect(nfRegistryService.sidenav.close).toHaveBeenCalled();
        expect(nfRegistryService.bucket.name).toBe('Bucket #1');
        expect(comp.$subscription.unsubscribe).toHaveBeenCalled();
    }));
});
