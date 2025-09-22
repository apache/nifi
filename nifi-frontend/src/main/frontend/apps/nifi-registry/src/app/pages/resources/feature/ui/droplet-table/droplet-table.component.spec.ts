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
import { DropletTableComponent } from './droplet-table.component';
import { provideMockStore } from '@ngrx/store/testing';
import { CommonModule } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { MatMenuModule } from '@angular/material/menu';
import { MatSortModule } from '@angular/material/sort';
import { MatTableModule } from '@angular/material/table';

describe('DropletTableComponent', () => {
    let component: DropletTableComponent;
    let fixture: ComponentFixture<DropletTableComponent>;
    const droplets = [
        {
            bucketIdentifier: '39a248d9-4ac6-42b2-8288-4b9ee6b2e3ed',
            bucketName: 'Bucket 2',
            createdTimestamp: 1747167193176,
            description: 'This is a test description',
            identifier: 'a48c0576-9075-45e5-af4d-5a6e76682f42',
            link: {
                href: 'buckets/39a248d9-4ac6-42b2-8288-4b9ee6b2e3ed/flows/a48c0576-9075-45e5-af4d-5a6e76682f42',
                params: {
                    rel: 'self'
                }
            },
            modifiedTimestamp: 1747167193204,
            name: 'Test Flow 1',
            permissions: {
                canDelete: true,
                canRead: true,
                canWrite: true
            },
            revision: {
                version: 0
            },
            type: 'Flow',
            versionCount: 1
        }
    ];

    const buckets = [
        {
            allowBundleRedeploy: false,
            allowPublicRead: false,
            createdTimestamp: 1747167167303,
            description: '',
            identifier: '39a248d9-4ac6-42b2-8288-4b9ee6b2e3ed',
            link: {
                href: 'buckets/39a248d9-4ac6-42b2-8288-4b9ee6b2e3ed',
                params: {
                    rel: 'self'
                }
            },
            name: 'Bucket 2',
            permissions: {
                canDelete: true,
                canRead: true,
                canWrite: true
            },
            revision: {
                version: 0
            }
        }
    ];

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [
                DropletTableComponent,
                CommonModule,
                MatTableModule,
                MatSortModule,
                MatMenuModule,
                MatButtonModule,
                MatButtonModule
            ],
            providers: [provideMockStore({})]
        }).compileComponents();

        fixture = TestBed.createComponent(DropletTableComponent);
        component = fixture.componentInstance;
        component.buckets = buckets;
        component.dataSource.data = [...droplets];
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should render the droplet table with the correct headings', () => {
        const rows = fixture.nativeElement.querySelectorAll('tr');

        // Header row
        const headerRow = rows[0];
        expect(headerRow.cells[0].innerHTML).toContain('Name');
        expect(headerRow.cells[1].innerHTML).toContain('Type');
        expect(headerRow.cells[2].innerHTML).toContain('Bucket');
        expect(headerRow.cells[3].innerHTML).toContain('Bucket ID');
        expect(headerRow.cells[4].innerHTML).toContain('Resource ID');
        expect(headerRow.cells[5].innerHTML).toContain('Versions');
    });

    it('should render the droplet table with the correct row values', () => {
        const rows = fixture.nativeElement.querySelectorAll('tr');
        expect(rows.length).toBe(2);

        // Droplet row
        const row1 = rows[1];
        expect(row1.cells[0].innerHTML).toContain(droplets[0].name);
        expect(row1.cells[1].innerHTML).toContain(droplets[0].type);
        expect(row1.cells[2].innerHTML).toContain(droplets[0].bucketName);
        expect(row1.cells[3].innerHTML).toContain(droplets[0].bucketIdentifier);
        expect(row1.cells[4].innerHTML).toContain(droplets[0].identifier);
        expect(row1.cells[5].innerHTML).toContain(droplets[0].versionCount.toString());
    });
});
