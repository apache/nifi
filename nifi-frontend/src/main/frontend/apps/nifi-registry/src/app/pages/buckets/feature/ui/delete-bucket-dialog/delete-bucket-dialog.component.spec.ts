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
import { DeleteBucketDialogComponent, DeleteBucketDialogData } from './delete-bucket-dialog.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { Bucket } from '../../../../../state/buckets';
import { Store } from '@ngrx/store';
import { deleteBucket } from '../../../../../state/buckets/buckets.actions';

const bucket: Bucket = {
    allowBundleRedeploy: false,
    allowPublicRead: false,
    createdTimestamp: Date.now(),
    description: 'desc',
    identifier: 'bucket-1',
    link: {
        href: '',
        params: { rel: '' }
    },
    name: 'Bucket 1',
    permissions: {
        canRead: true,
        canWrite: true
    },
    revision: {
        version: 1
    }
};

describe('DeleteBucketDialogComponent', () => {
    let component: DeleteBucketDialogComponent;
    let fixture: ComponentFixture<DeleteBucketDialogComponent>;
    let store: Store;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [DeleteBucketDialogComponent, NoopAnimationsModule],
            providers: [
                provideMockStore({
                    initialState: {
                        error: {
                            bannerErrors: {}
                        }
                    }
                }),
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: { bucket } as DeleteBucketDialogData
                }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(DeleteBucketDialogComponent);
        component = fixture.componentInstance;
        store = TestBed.inject(Store);
        jest.spyOn(store, 'dispatch');
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should dispatch delete action with correct payload', () => {
        component.onDeleteBucket();
        expect(store.dispatch).toHaveBeenCalledWith(
            deleteBucket({
                request: {
                    bucket,
                    version: bucket.revision.version
                }
            })
        );
    });
});
