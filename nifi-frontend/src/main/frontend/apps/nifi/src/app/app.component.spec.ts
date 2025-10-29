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
import { RouterTestingModule } from '@angular/router/testing';
import { AppComponent } from './app.component';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { provideMockStore } from '@ngrx/store/testing';
import { navigationFeatureKey } from './state/navigation';
import * as fromNavigation from './state/navigation/navigation.reducer';
import { initialState as initialErrorState } from './state/error/error.reducer';
import { errorFeatureKey } from './state/error';
import { initialState as initialCurrentUserState } from './state/current-user/current-user.reducer';
import { currentUserFeatureKey } from './state/current-user';

describe('AppComponent', () => {
    beforeEach(() =>
        TestBed.configureTestingModule({
            imports: [RouterTestingModule, MatProgressSpinnerModule],
            declarations: [AppComponent],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState,
                        [currentUserFeatureKey]: initialCurrentUserState,
                        [navigationFeatureKey]: fromNavigation.initialState
                    }
                })
            ]
        })
    );

    it('should create the app', () => {
        const fixture = TestBed.createComponent(AppComponent);
        const app = fixture.componentInstance;
        expect(app).toBeTruthy();
    });

    it(`should have as title 'nifi'`, () => {
        const fixture = TestBed.createComponent(AppComponent);
        const app = fixture.componentInstance;
        expect(app.title).toEqual('nifi');
    });
});
