/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Users } from './users.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { RouterModule } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { UserListing } from '../ui/user-listing/user-listing.component';
import { initialState } from '../state/user-listing/user-listing.reducer';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { usersFeatureKey } from '../state';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';

describe('Users', () => {
    let component: Users;
    let fixture: ComponentFixture<Users>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [Users, UserListing],
            imports: [RouterModule, RouterTestingModule, MockComponent(BannerText), MockComponent(Navigation)],
            providers: [
                provideMockStore({
                    initialState: {
                        [usersFeatureKey]: {
                            [usersFeatureKey]: initialState
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(Users);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
