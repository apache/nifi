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
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { By } from '@angular/platform-browser';

import {
    SaveParameterContextChangesDialog,
    SaveParameterContextChangesDialogRequest
} from './save-parameter-context-changes-dialog.component';

describe('SaveParameterContextChangesDialog', () => {
    let component: SaveParameterContextChangesDialog;
    let fixture: ComponentFixture<SaveParameterContextChangesDialog>;

    function setup(data: SaveParameterContextChangesDialogRequest) {
        TestBed.configureTestingModule({
            imports: [SaveParameterContextChangesDialog],
            providers: [
                { provide: MAT_DIALOG_DATA, useValue: data },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(SaveParameterContextChangesDialog);
        component = fixture.componentInstance;
        fixture.detectChanges();
    }

    afterEach(() => {
        TestBed.resetTestingModule();
    });

    it('should create', () => {
        setup({ destination: 'Parameter', canSave: true });
        expect(component).toBeTruthy();
    });

    it('should emit when save clicked', () => {
        setup({ destination: 'Parameter', canSave: true });
        const emitSpy = vi.spyOn(component.save, 'next');
        component.saveClicked();
        expect(emitSpy).toHaveBeenCalled();
    });

    it('should emit when discard clicked', () => {
        setup({ destination: 'Parameter', canSave: true });
        const emitSpy = vi.spyOn(component.discard, 'next');
        component.discardClicked();
        expect(emitSpy).toHaveBeenCalled();
    });

    it('should enable Save, focus Save, and mention discarding pending changes when the form can be saved', () => {
        setup({ destination: 'Parameter', canSave: true });

        const saveButton = fixture.debugElement.query(
            By.css('button[data-qa="save-parameter-context-changes-save-button"]')
        );
        const discardButton = fixture.debugElement.query(
            By.css('button[data-qa="save-parameter-context-changes-discard-button"]')
        );
        const message = fixture.debugElement.query(By.css('div[data-qa="save-parameter-context-changes-message"]'));

        expect(saveButton.nativeElement.disabled).toBe(false);
        expect(saveButton.attributes['cdkFocusInitial']).toBeDefined();
        expect(discardButton.attributes['cdkFocusInitial']).toBeUndefined();
        expect(message.nativeElement.textContent).toContain('discard all pending changes');
    });

    it("should disable Save, focus Don't Save, and indicate the form is not saveable when the form cannot be saved", () => {
        setup({ destination: 'Parameter', canSave: false });

        const saveButton = fixture.debugElement.query(
            By.css('button[data-qa="save-parameter-context-changes-save-button"]')
        );
        const discardButton = fixture.debugElement.query(
            By.css('button[data-qa="save-parameter-context-changes-discard-button"]')
        );
        const message = fixture.debugElement.query(By.css('div[data-qa="save-parameter-context-changes-message"]'));

        expect(saveButton.nativeElement.disabled).toBe(true);
        expect(discardButton.attributes['cdkFocusInitial']).toBeDefined();
        expect(saveButton.attributes['cdkFocusInitial']).toBeUndefined();
        expect(message.nativeElement.textContent).toContain('not in a saveable');
    });

    it('should always render a Cancel button that only closes the dialog', () => {
        setup({ destination: 'Parameter', canSave: false });

        const cancelButton = fixture.debugElement.query(
            By.css('button[data-qa="save-parameter-context-changes-cancel-button"]')
        );
        expect(cancelButton).toBeTruthy();
        expect(cancelButton.attributes['mat-dialog-close']).toBeDefined();
    });
});
