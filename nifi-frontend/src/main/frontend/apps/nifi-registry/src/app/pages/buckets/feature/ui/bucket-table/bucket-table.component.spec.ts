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
import { BucketTableComponent } from './bucket-table.component';
import { MatTableDataSource } from '@angular/material/table';
import { Bucket } from '../../../../../state/buckets';
import { provideMockStore } from '@ngrx/store/testing';
import { NiFiCommon } from '@nifi/shared';
import {
    openDeleteBucketDialog,
    openEditBucketDialog,
    openManageBucketPoliciesDialog
} from '../../../../../state/buckets/buckets.actions';
import { Store } from '@ngrx/store';
import { MatMenuModule } from '@angular/material/menu';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatSortModule } from '@angular/material/sort';
import { MatTableModule } from '@angular/material/table';

const createBucket = (overrides: Partial<Bucket> = {}): Bucket => ({
    allowBundleRedeploy: false,
    allowPublicRead: false,
    createdTimestamp: Date.now(),
    description: 'Test bucket',
    identifier: 'bucket-1',
    link: {
        href: '',
        params: { rel: '' }
    },
    name: 'A Bucket',
    permissions: {
        canRead: true,
        canWrite: true
    },
    revision: {
        version: 0
    },
    ...overrides
});

describe('BucketTableComponent', () => {
    let component: BucketTableComponent;
    let fixture: ComponentFixture<BucketTableComponent>;
    let store: Store;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [
                BucketTableComponent,
                MatTableModule,
                MatSortModule,
                MatMenuModule,
                MatButtonModule,
                MatIconModule,
                NoopAnimationsModule
            ],
            providers: [provideMockStore(), NiFiCommon]
        }).compileComponents();

        fixture = TestBed.createComponent(BucketTableComponent);
        component = fixture.componentInstance;
        store = TestBed.inject(Store);
        jest.spyOn(store, 'dispatch');

        const buckets = [
            createBucket({ identifier: 'bucket-1', name: 'Alpha' }),
            createBucket({ identifier: 'bucket-2', name: 'Bravo' })
        ];
        component.dataSource = new MatTableDataSource<Bucket>(buckets);

        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should emit when a bucket row is selected', () => {
        jest.spyOn(component.selectBucket, 'next');
        component.select(component.dataSource.data[0]);
        expect(component.selectBucket.next).toHaveBeenCalledWith(component.dataSource.data[0]);
    });

    it('should dispatch edit dialog action', () => {
        component.openEditBucketDialog(component.dataSource.data[0]);
        expect(store.dispatch).toHaveBeenCalledWith(
            openEditBucketDialog({ request: { bucket: component.dataSource.data[0] } })
        );
    });

    it('should dispatch delete dialog action', () => {
        component.openDeleteBucketDialog(component.dataSource.data[0]);
        expect(store.dispatch).toHaveBeenCalledWith(
            openDeleteBucketDialog({ request: { bucket: component.dataSource.data[0] } })
        );
    });

    it('should dispatch manage policies dialog action', () => {
        component.openManageBucketPoliciesDialog(component.dataSource.data[0]);
        expect(store.dispatch).toHaveBeenCalledWith(
            openManageBucketPoliciesDialog({ request: { bucket: component.dataSource.data[0] } })
        );
    });

    it('should sort data on init', () => {
        component.sortData({ active: 'name', direction: 'desc' });
        expect(component.dataSource.data[0].name).toBe('Bravo');
    });
});
