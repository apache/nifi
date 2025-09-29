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
import { BucketsComponent } from './buckets.component';
import { provideMockStore } from '@ngrx/store/testing';
import { NiFiCommon } from '@nifi/shared';
import { BucketTableComponent } from './ui/bucket-table/bucket-table.component';
import { BucketTableFilterComponent } from './ui/bucket-table-filter/bucket-table-filter.component';
import { ContextErrorBanner } from '../../../ui/common/context-error-banner/context-error-banner.component';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { Router } from '@angular/router';

describe('BucketsComponent', () => {
    let component: BucketsComponent;
    let fixture: ComponentFixture<BucketsComponent>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            declarations: [BucketsComponent],
            imports: [
                BucketTableComponent,
                BucketTableFilterComponent,
                ContextErrorBanner,
                MatButtonModule,
                MatIconModule
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        resources: {
                            buckets: {
                                buckets: [],
                                status: 'pending'
                            }
                        },
                        error: {
                            bannerErrors: {}
                        }
                    }
                }),
                NiFiCommon,
                { provide: Router, useValue: { navigate: jest.fn() } }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(BucketsComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should initialize with correct default values', () => {
        expect(component.displayedColumns).toEqual(['name', 'description', 'identifier', 'actions']);
        expect(component.sort).toEqual({
            active: 'name',
            direction: 'asc'
        });
        expect(component.filterTerm).toBe('');
        expect(component.filterColumn).toBe('name');
    });
});
