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
import { ReferencedAttributesDialog, ReferencedAttributesDialogData } from './referenced-attributes-dialog.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';

describe('ReferencedAttributesDialog', () => {
    let component: ReferencedAttributesDialog;
    let fixture: ComponentFixture<ReferencedAttributesDialog>;
    const data: ReferencedAttributesDialogData = {
        attributes: []
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ReferencedAttributesDialog],
            providers: [
                { provide: MatDialogRef, useValue: null },
                { provide: MAT_DIALOG_DATA, useValue: data }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ReferencedAttributesDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
