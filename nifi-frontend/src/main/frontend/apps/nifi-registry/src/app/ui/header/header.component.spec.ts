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
import { HeaderComponent } from './header.component';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { NiFiRegistryState } from '../../state';
import { openAboutDialog } from '../../state/about/about.actions';
import { selectAbout } from '../../state/about/about.selectors';
import { aboutFeatureKey } from '../../state/about';
import { initialState as aboutInitialState } from '../../state/about/about.reducer';
import { logout } from '../../state/current-user/current-user.actions';
import { selectCurrentUser, selectLogoutSupported } from '../../state/current-user/current-user.selectors';
import { currentUserFeatureKey } from '../../state/current-user';
import { initialState as currentUserInitialState } from '../../state/current-user/current-user.reducer';
import { MatDialog } from '@angular/material/dialog';
import { of } from 'rxjs';

describe('HeaderComponent', () => {
    let component: HeaderComponent;
    let fixture: ComponentFixture<HeaderComponent>;
    let store: MockStore<NiFiRegistryState>;
    let dialogOpenSpy: jest.Mock;

    beforeEach(() => {
        const matDialogMock = {
            open: jest.fn().mockReturnValue({ afterClosed: () => of(true) })
        };

        TestBed.configureTestingModule({
            imports: [HeaderComponent],
            providers: [
                provideMockStore({
                    initialState: {
                        [currentUserFeatureKey]: currentUserInitialState,
                        [aboutFeatureKey]: aboutInitialState,
                        error: {
                            bannerErrors: {}
                        }
                    }
                }),
                {
                    provide: MatDialog,
                    useValue: matDialogMock
                }
            ]
        });

        fixture = TestBed.createComponent(HeaderComponent);
        component = fixture.componentInstance;
        store = TestBed.inject(MockStore);
        dialogOpenSpy = matDialogMock.open;

        store.overrideSelector(selectCurrentUser, currentUserInitialState.currentUser);
        store.overrideSelector(selectLogoutSupported, currentUserInitialState.currentUser.canLogout);
        store.overrideSelector(selectAbout, aboutInitialState.about);

        jest.spyOn(store, 'dispatch');
        store.refreshState();
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should dispatch openAboutDialog when viewAbout is called', () => {
        component.viewAbout();
        expect(store.dispatch).toHaveBeenCalledWith(openAboutDialog());
        expect(dialogOpenSpy).not.toHaveBeenCalled();
    });

    it('should dispatch logout when logout is called', () => {
        component.logout();
        expect(store.dispatch).toHaveBeenCalledWith(logout());
    });
});
