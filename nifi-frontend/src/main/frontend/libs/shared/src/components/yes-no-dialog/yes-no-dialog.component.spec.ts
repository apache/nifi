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

import { YesNoDialog, YesNoDialogRequest } from '../..';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { By } from '@angular/platform-browser';

describe('YesNoDialog', () => {
    let component: YesNoDialog;
    let fixture: ComponentFixture<YesNoDialog>;

    const data: YesNoDialogRequest = {
        title: 'Title',
        message: 'Message'
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [YesNoDialog],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(YesNoDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should emit when yes clicked', () => {
        jest.spyOn(component.yes, 'next');
        component.yesClicked();
        expect(component.yes.next).toHaveBeenCalled();
    });

    it('should title be set', () => {
        const title = fixture.debugElement.query(By.css('h2[data-qa="yes-no-title"]'));
        expect(title.nativeElement.textContent).toEqual(data.title);
    });

    it('should message be set', () => {
        const title = fixture.debugElement.query(By.css('div[data-qa="yes-no-message"]'));
        expect(title.nativeElement.textContent).toEqual(data.message);
    });
});
