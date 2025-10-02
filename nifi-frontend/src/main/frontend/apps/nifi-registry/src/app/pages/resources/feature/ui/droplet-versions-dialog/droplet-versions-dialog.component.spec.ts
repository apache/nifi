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
import { DropletVersionsDialogComponent } from './droplet-versions-dialog.component';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { Subject } from 'rxjs';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatTableModule } from '@angular/material/table';
import { MatSortModule } from '@angular/material/sort';
import { MatMenuModule } from '@angular/material/menu';
import { MatButtonModule } from '@angular/material/button';
import { exportDropletVersion } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';

describe('DropletVersionsDialogComponent', () => {
    let component: DropletVersionsDialogComponent;
    let fixture: ComponentFixture<DropletVersionsDialogComponent>;
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
        permissions: { canRead: true, canWrite: true, canDelete: true },
        revision: { version: 1 },
        type: 'FLOW',
        versionCount: 1
    };
    const mockVersions = {
        author: 'anonymous',
        bucketIdentifier: '311c6295-d8b6-47bd-806e-b5ee27dd8187',
        flowIdentifier: 'ed9f3f48-751b-4237-a8a7-a4a2020f1efe',
        link: {
            href: 'buckets/311c6295-d8b6-47bd-806e-b5ee27dd8187/flows/ed9f3f48-751b-4237-a8a7-a4a2020f1efe/versions/1',
            params: { rel: 'content' }
        },
        timestamp: 1741788614197,
        version: 1
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [
                DropletVersionsDialogComponent,
                MatTableModule,
                MatSortModule,
                MatDialogModule,
                MatMenuModule,
                MatButtonModule
            ],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: { droplet: mockDroplet, versions: mockVersions } },
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

        store = TestBed.inject(MockStore);
        fixture = TestBed.createComponent(DropletVersionsDialogComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should export flow', () => {
        const dispatchSpy = jest.spyOn(store, 'dispatch');
        component.exportVersion(2);
        expect(dispatchSpy).toHaveBeenCalledWith(
            exportDropletVersion({ request: { droplet: mockDroplet, version: 2 } })
        );
    });
});
