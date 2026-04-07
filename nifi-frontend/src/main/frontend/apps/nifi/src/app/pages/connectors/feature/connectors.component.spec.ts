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

import { TestBed } from '@angular/core/testing';
import { Connectors } from './connectors.component';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { resetConnectorsListingState } from '../state/connectors-listing/connectors-listing.actions';
import { NO_ERRORS_SCHEMA } from '@angular/core';

describe('Connectors', () => {
    async function setup() {
        await TestBed.configureTestingModule({
            declarations: [Connectors],
            providers: [provideMockStore({ initialState: {} })],
            schemas: [NO_ERRORS_SCHEMA]
        }).compileComponents();

        const fixture = TestBed.createComponent(Connectors);
        const component = fixture.componentInstance;
        const store = TestBed.inject(MockStore);

        fixture.detectChanges();

        return { component, fixture, store };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    it('should dispatch resetConnectorsListingState on destroy', async () => {
        const { component, store } = await setup();
        const dispatchSpy = vi.spyOn(store, 'dispatch');

        component.ngOnDestroy();

        expect(dispatchSpy).toHaveBeenCalledWith(resetConnectorsListingState());
    });

    it('should dispatch exactly one action on destroy', async () => {
        const { component, store } = await setup();
        const dispatchSpy = vi.spyOn(store, 'dispatch');

        component.ngOnDestroy();

        expect(dispatchSpy).toHaveBeenCalledTimes(1);
        expect(dispatchSpy).toHaveBeenCalledWith(
            expect.objectContaining({
                type: resetConnectorsListingState().type
            })
        );
    });
});
