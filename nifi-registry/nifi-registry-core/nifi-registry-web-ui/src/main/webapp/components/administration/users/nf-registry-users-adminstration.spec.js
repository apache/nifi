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

import { TestBed, fakeAsync, tick, async } from '@angular/core/testing';
import initTestBed from 'nf-registry.testbed-factory';
import { of } from 'rxjs';
import NfRegistryApi from 'services/nf-registry.api';
import NfRegistryService from 'services/nf-registry.service';
import { ActivatedRoute } from '@angular/router';

import * as ngPlatformBrowser from '@angular/platform-browser';
import NfRegistryUsersAdministration from 'components/administration/users/nf-registry-users-administration';

describe('NfRegistryUsersAdministration Component', function () {
    let comp;
    let fixture;
    let de;
    let nfRegistryService;
    let nfRegistryApi;

    beforeEach((done) => {
        const providers = [
            {
                provide: ActivatedRoute,
                useValue: {
                    params: of({})
                }
            }
        ];

        initTestBed({providers})
            .then(() => {
                fixture = TestBed.createComponent(NfRegistryUsersAdministration);

                // test instance
                comp = fixture.componentInstance;

                // from the root injector
                nfRegistryService = TestBed.get(NfRegistryService);
                nfRegistryApi = TestBed.get(NfRegistryApi);
                de = fixture.debugElement.query(ngPlatformBrowser.By.css('#nifi-registry-users-administration-perspective'));

                // Spy
                spyOn(nfRegistryApi, 'ticketExchange').and.callFake(function () {}).and.returnValue(of({}));
                spyOn(nfRegistryApi, 'loadCurrentUser').and.callFake(function () {}).and.returnValue(of({}));
                spyOn(nfRegistryApi, 'getUsers').and.callFake(function () {
                }).and.returnValue(of([{
                    'identifier': '2e04b4fb-9513-47bb-aa74-1ae34616bfdc',
                    'identity': 'User #1'
                }]));
                spyOn(nfRegistryApi, 'getUserGroups').and.callFake(function () {
                }).and.returnValue(of([{
                    'identifier': '5e04b4fb-9513-47bb-aa74-1ae34616bfdc',
                    'identity': 'Group #1'}]));
                spyOn(nfRegistryService, 'filterUsersAndGroups');

                done();
            });
    });

    it('should have a defined component', async(function () {
        fixture.detectChanges();
        fixture.whenStable().then(function () { // wait for async getBuckets
            fixture.detectChanges();

            //assertions
            expect(comp).toBeDefined();
            expect(de).toBeDefined();
            expect(nfRegistryService.adminPerspective).toBe('users');
            expect(nfRegistryService.inProgress).toBe(false);
            expect(nfRegistryService.users[0].identity).toEqual('User #1');
            expect(nfRegistryService.users.length).toBe(1);
            expect(nfRegistryService.groups[0].identity).toEqual('Group #1');
            expect(nfRegistryService.groups.length).toBe(1);
            expect(nfRegistryService.filterUsersAndGroups).toHaveBeenCalled();
        });
    }));

    it('should open a dialog to create a new user', function () {
        spyOn(comp.dialog, 'open');
        fixture.detectChanges();

        // the function to test
        comp.addUser();

        //assertions
        expect(comp.dialog.open).toHaveBeenCalled();
    });

    it('should open a dialog to create a new group', function () {
        spyOn(comp.dialog, 'open');
        fixture.detectChanges();

        // the function to test
        comp.createNewGroup();

        //assertions
        expect(comp.dialog.open).toHaveBeenCalled();
    });

    it('should destroy the component', fakeAsync(function () {
        fixture.detectChanges();
        // wait for async getBucket call
        tick();
        fixture.detectChanges();

        // The function to test
        comp.ngOnDestroy();

        //assertions
        expect(nfRegistryService.adminPerspective).toBe('');
        expect(nfRegistryService.users.length).toBe(0);
        expect(nfRegistryService.groups.length).toBe(0);
        expect(nfRegistryService.filteredUsers.length).toBe(0);
        expect(nfRegistryService.filteredUserGroups.length).toBe(0);
        expect(nfRegistryService.allUsersAndGroupsSelected).toBe(false);
    }));
});
