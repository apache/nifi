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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterModule } from '@angular/router';
import { provideMockStore } from '@ngrx/store/testing';
import { ResourcesComponent } from './resources.component';
import { initialState as dropletsInitialState } from '../../../state/droplets/droplets.reducer';
import { initialState as bucketsInitialState } from '../../../state/buckets/buckets.reducer';
import { resourcesFeatureKey } from '../../../state';
import { dropletsFeatureKey } from '../../../state/droplets';
import { bucketsFeatureKey } from '../../../state/buckets';
import { DropletTableComponent } from './ui/droplet-table/droplet-table.component';
import { DropletTableFilterComponent } from './ui/droplet-table-filter/droplet-table-filter.component';
import { ContextErrorBanner } from '../../../ui/common/context-error-banner/context-error-banner.component';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { HeaderComponent } from '../../../ui/header/header.component';
import { currentUserFeatureKey } from '../../../state/current-user';
import { initialState as currentUserInitialState } from '../../../state/current-user/current-user.reducer';
import { aboutFeatureKey } from '../../../state/about';
import { initialState as aboutInitialState } from '../../../state/about/about.reducer';

describe('Resources', () => {
    let component: ResourcesComponent;
    let fixture: ComponentFixture<ResourcesComponent>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [ResourcesComponent],
            imports: [
                RouterModule,
                DropletTableComponent,
                DropletTableFilterComponent,
                ContextErrorBanner,
                MatButtonModule,
                MatIconModule,
                HeaderComponent
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [resourcesFeatureKey]: {
                            [dropletsFeatureKey]: dropletsInitialState,
                            [bucketsFeatureKey]: bucketsInitialState
                        },
                        [currentUserFeatureKey]: currentUserInitialState,
                        [aboutFeatureKey]: aboutInitialState,
                        error: {
                            bannerErrors: {}
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(ResourcesComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
