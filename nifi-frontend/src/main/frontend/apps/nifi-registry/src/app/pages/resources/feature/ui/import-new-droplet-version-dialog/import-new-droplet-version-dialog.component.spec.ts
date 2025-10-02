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
import { ImportNewDropletVersionDialogComponent } from './import-new-droplet-version-dialog.component';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { Subject } from 'rxjs';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';

describe('ImportNewDropletVersionDialogComponent', () => {
    let component: ImportNewDropletVersionDialogComponent;
    let fixture: ComponentFixture<ImportNewDropletVersionDialogComponent>;
    let store: MockStore;
    const mockDroplet = {
        bucketIdentifier: '1234',
        bucketName: 'testBucket',
        createdTimestamp: 123456789,
        description: 'testDescription',
        identifier: '1234',
        link: { href: 'testHref', params: { rel: 'testRel' } },
        modifiedTimestamp: 123456789,
        name: 'testName',
        permissions: { canRead: true, canWrite: true },
        revision: { version: 1 },
        type: 'FLOW',
        versionCount: 2
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [
                ImportNewDropletVersionDialogComponent,
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
                { provide: MAT_DIALOG_DATA, useValue: { droplet: mockDroplet } },
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

        fixture = TestBed.createComponent(ImportNewDropletVersionDialogComponent);
        store = TestBed.inject(MockStore);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should import a new flow version to the correct bucket', () => {
        const dispatchSpy = jest.spyOn(store, 'dispatch');
        const file = new File([''], 'testFile');
        component.fileToUpload = file;
        component.name = 'testName';
        component.description = 'testDescription';
        component.importNewFlowVersion();
        expect(dispatchSpy).toHaveBeenCalled();
    });
});
