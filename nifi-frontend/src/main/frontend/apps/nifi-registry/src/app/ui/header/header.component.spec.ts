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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { HeaderComponent } from './header.component';
import * as fromUser from '../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../state/current-user';
import { selectCurrentUser } from '../../state/current-user/current-user.selectors';
import { loadCurrentUser, startCurrentUserPolling } from '../../state/current-user/current-user.actions';

describe('HeaderComponent', () => {
    let component: HeaderComponent;
    let fixture: ComponentFixture<HeaderComponent>;
    let store: MockStore;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [HeaderComponent, RouterTestingModule],
            providers: [
                provideMockStore({
                    initialState: {
                        [currentUserFeatureKey]: fromUser.initialState
                    },
                    selectors: [
                        {
                            selector: selectCurrentUser,
                            value: fromUser.initialState.user
                        }
                    ]
                })
            ]
        });

        store = TestBed.inject(MockStore);
        fixture = TestBed.createComponent(HeaderComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should render title', () => {
        const compiled = fixture.nativeElement as HTMLElement;
        expect(compiled.querySelector('h1')?.textContent).toContain('NiFi Registry');
    });

    it('should dispatch selectSignal on construction', () => {
        const selectSignalSpy = jest.spyOn(store, 'selectSignal');
        TestBed.createComponent(HeaderComponent);

        expect(selectSignalSpy).toHaveBeenCalledWith(selectCurrentUser);
    });

    it('should dispatch loadCurrentUser and startCurrentUserPolling on load', () => {
        const dispatchSpy = jest.spyOn(store, 'dispatch');
        component.ngOnInit();

        expect(dispatchSpy).toHaveBeenCalledWith(loadCurrentUser());
        expect(dispatchSpy).toHaveBeenCalledWith(startCurrentUserPolling());
    });
});
