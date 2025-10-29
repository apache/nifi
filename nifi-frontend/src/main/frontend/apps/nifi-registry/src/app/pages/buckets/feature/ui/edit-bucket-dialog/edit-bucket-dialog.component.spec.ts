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
import { EditBucketDialogComponent, EditBucketDialogData } from './edit-bucket-dialog.component';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { Bucket } from '../../../../../state/buckets';
import { provideMockStore } from '@ngrx/store/testing';
import { updateBucket } from '../../../../../state/buckets/buckets.actions';
import { Store } from '@ngrx/store';
import { FormBuilder } from '@angular/forms';

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

describe('EditBucketDialogComponent', () => {
    let component: EditBucketDialogComponent;
    let fixture: ComponentFixture<EditBucketDialogComponent>;
    let store: Store;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [EditBucketDialogComponent, NoopAnimationsModule],
            providers: [
                FormBuilder,
                provideMockStore({
                    initialState: {
                        error: {
                            bannerErrors: {}
                        }
                    }
                }),
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: { bucket } as EditBucketDialogData
                }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(EditBucketDialogComponent);
        component = fixture.componentInstance;
        store = TestBed.inject(Store);
        jest.spyOn(store, 'dispatch');

        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should dispatch update action when form valid', () => {
        component.bucketForm.patchValue({
            name: 'Updated',
            allowPublicRead: true
        });
        component.onSaveBucket();

        expect(store.dispatch).toHaveBeenCalledWith(
            updateBucket({
                request: {
                    bucket: {
                        ...bucket,
                        name: 'Updated',
                        description: bucket.description,
                        allowPublicRead: true,
                        allowBundleRedeploy: bucket.allowBundleRedeploy
                    }
                }
            })
        );
    });

    it('should not dispatch when form invalid', () => {
        component.bucketForm.patchValue({ name: '' });
        component.onSaveBucket();
        expect(store.dispatch).not.toHaveBeenCalled();
    });
});
