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

import { Settings } from './settings.component';
import { provideMockStore } from '@ngrx/store/testing';
import { MatTabsModule } from '@angular/material/tabs';
import { RouterModule } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { initialState } from '../state/general/general.reducer';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { settingsFeatureKey } from '../state';
import { generalFeatureKey } from '../state/general';
import { initialState as initialErrorState } from '../../../state/error/error.reducer';
import { errorFeatureKey } from '../../../state/error';
import { initialState as initialCurrentUserState } from '../../../state/current-user/current-user.reducer';
import { currentUserFeatureKey } from '../../../state/current-user';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';

describe('Settings', () => {
    let component: Settings;
    let fixture: ComponentFixture<Settings>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [Settings],
            imports: [
                MatTabsModule,
                RouterModule,
                RouterTestingModule,
                MockComponent(BannerText),
                MockComponent(Navigation)
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [settingsFeatureKey]: {
                            [generalFeatureKey]: initialState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(Settings);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
