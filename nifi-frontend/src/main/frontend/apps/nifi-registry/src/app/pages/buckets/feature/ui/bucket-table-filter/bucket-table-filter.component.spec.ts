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

import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { BucketTableFilterComponent, BucketTableFilterColumn } from './bucket-table-filter.component';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { ReactiveFormsModule } from '@angular/forms';

const columns: BucketTableFilterColumn[] = [
    { key: 'name', label: 'Name' },
    { key: 'description', label: 'Description' }
];

describe('BucketTableFilterComponent', () => {
    let component: BucketTableFilterComponent;
    let fixture: ComponentFixture<BucketTableFilterComponent>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [
                BucketTableFilterComponent,
                MatFormFieldModule,
                MatInputModule,
                MatSelectModule,
                NoopAnimationsModule,
                ReactiveFormsModule
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(BucketTableFilterComponent);
        component = fixture.componentInstance;
        component.filterableColumns = columns;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should emit filter changes when term changes', fakeAsync(() => {
        jest.spyOn(component.filterChanged, 'emit');
        component.filterForm.get('filterTerm')?.setValue('test');
        fixture.detectChanges();
        tick(500); // Wait for debounceTime
        expect(component.filterChanged.emit).toHaveBeenCalledWith({
            filterTerm: 'test',
            filterColumn: 'name',
            changedField: 'filterTerm'
        });
    }));

    it('should emit filter changes when column changes', fakeAsync(() => {
        jest.spyOn(component.filterChanged, 'emit');
        component.filterForm.get('filterColumn')?.setValue('description');
        fixture.detectChanges();
        expect(component.filterChanged.emit).toHaveBeenCalledWith({
            filterTerm: '',
            filterColumn: 'description',
            changedField: 'filterColumn'
        });
    }));
});
