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

import { NewPropertyDialog } from './new-property-dialog.component';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { NewPropertyDialogRequest } from '../../../state/shared';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('NewPropertyDialog', () => {
    let component: NewPropertyDialog;
    let fixture: ComponentFixture<NewPropertyDialog>;

    const data: NewPropertyDialogRequest = {
        existingProperties: ['topic'],
        allowsSensitive: false
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [NewPropertyDialog, NoopAnimationsModule, FormsModule, ReactiveFormsModule],
            providers: [{ provide: MAT_DIALOG_DATA, useValue: data }]
        });
        fixture = TestBed.createComponent(NewPropertyDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
