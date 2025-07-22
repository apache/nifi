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
import { DeleteDropletDialogComponent } from './delete-droplet-dialog.component';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { DebugElement } from '@angular/core';
import { By } from '@angular/platform-browser';
import { Subject } from 'rxjs';
import { deleteDroplet } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';

describe('DeleteDropletDialogComponent', () => {
    let component: DeleteDropletDialogComponent;
    let fixture: ComponentFixture<DeleteDropletDialogComponent>;
    let debug: DebugElement;
    let store: MockStore;
    const mockData = {
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

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [DeleteDropletDialogComponent, MatDialogModule, MatButtonModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: { droplet: mockData } },
                {
                    provide: MatDialogRef,
                    useValue: {
                        close: () => null,
                        keydownEvents: () => new Subject<KeyboardEvent>()
                    }
                },
                provideMockStore({})
            ]
        }).compileComponents();

        store = TestBed.inject(MockStore);
        fixture = TestBed.createComponent(DeleteDropletDialogComponent);
        component = fixture.componentInstance;
        debug = fixture.debugElement;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should delete droplet', () => {
        const deleteDropletSpy = jest.spyOn(store, 'dispatch');
        const deleteBtn = debug.query(By.css('[data-test-id=delete-btn]')).nativeElement;
        deleteBtn.click();
        expect(deleteDropletSpy).toHaveBeenCalledWith(deleteDroplet({ request: { droplet: mockData } }));
    });
});
