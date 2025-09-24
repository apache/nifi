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
import { ImportNewDropletDialogComponent } from './import-new-droplet-dialog.component';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { Subject } from 'rxjs';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { CommonModule } from '@angular/common';
import { FormGroup, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { createNewDroplet } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';

describe('ImportNewDropletDialogComponent', () => {
    let component: ImportNewDropletDialogComponent;
    let fixture: ComponentFixture<ImportNewDropletDialogComponent>;
    let store: MockStore;
    let form: FormGroup;
    const buckets = [
        {
            allowBundleRedeploy: false,
            allowPublicRead: false,
            createdTimestamp: 1736800325035,
            description: '',
            identifier: '311c6295-d8b6-47bd-806e-b5ee27dd8187',
            link: {
                href: 'buckets/311c6295-d8b6-47bd-806e-b5ee27dd8187',
                params: {
                    rel: 'self'
                }
            },
            name: 'Test Bucket',
            permissions: {
                canDelete: true,
                canRead: true,
                canWrite: true
            },
            revision: {
                version: 0
            }
        },
        {
            allowBundleRedeploy: false,
            allowPublicRead: false,
            createdTimestamp: 1738008373583,
            description: '',
            identifier: '6c2f8d9a-2d72-4637-86d7-3e125296db85',
            link: {
                href: 'buckets/6c2f8d9a-2d72-4637-86d7-3e125296db85',
                params: {
                    rel: 'self'
                }
            },
            name: 'Test Bucket #2',
            permissions: {
                canDelete: true,
                canRead: true,
                canWrite: false
            },
            revision: {
                version: 0
            }
        }
    ];

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [
                ImportNewDropletDialogComponent,
                CommonModule,
                MatDialogModule,
                FormsModule,
                ReactiveFormsModule,
                MatFormFieldModule,
                MatSelectModule,
                MatInputModule,
                MatButtonModule
            ],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: { buckets } },
                {
                    provide: MatDialogRef,
                    useValue: {
                        close: () => null,
                        keydownEvents: () => new Subject<KeyboardEvent>()
                    }
                },
                provideMockStore({
                    initialState: {
                        error: {
                            bannerErrors: {}
                        }
                    }
                })
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ImportNewDropletDialogComponent);
        store = TestBed.inject(MockStore);
        component = fixture.componentInstance;
        form = component.importNewFlowForm;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should filter writable buckets', () => {
        expect(component.writableBuckets.length).toBe(1);
    });

    it('should import a new flow', () => {
        const dispatchSpy = jest.spyOn(store, 'dispatch');
        const file = new File([''], 'testFile');
        form.get('name')?.setValue('Flow Import 1');
        form.get('description')?.setValue('test description');
        form.get('bucket')?.setValue('311c6295-d8b6-47bd-806e-b5ee27dd8187');
        component.fileToUpload = file;
        component.importNewFlow();
        expect(dispatchSpy).toHaveBeenCalledWith(
            createNewDroplet({
                request: {
                    bucket: buckets[0],
                    file: file,
                    name: 'Flow Import 1',
                    description: 'test description'
                }
            })
        );
    });
});
