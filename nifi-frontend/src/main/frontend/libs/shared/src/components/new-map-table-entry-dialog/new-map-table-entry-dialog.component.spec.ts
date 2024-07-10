/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NewMapTableEntryDialog } from './new-map-table-entry-dialog.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MapTableEntryData } from '../../index';

describe('NewMapTableEntryDialog', () => {
    let component: NewMapTableEntryDialog;
    let fixture: ComponentFixture<NewMapTableEntryDialog>;
    const data: MapTableEntryData = {
        entryTypeLabel: '',
        existingEntries: []
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [NewMapTableEntryDialog, NoopAnimationsModule],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                { provide: MatDialogRef, useValue: null }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(NewMapTableEntryDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
