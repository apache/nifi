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
import { EditPolicyDialogComponent, EditPolicyDialogData } from './edit-policy-dialog.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

describe('EditPolicyDialogComponent', () => {
    let component: EditPolicyDialogComponent;
    let fixture: ComponentFixture<EditPolicyDialogComponent>;
    let dialogRef: jest.Mocked<MatDialogRef<EditPolicyDialogComponent>>;

    beforeEach(async () => {
        const mockDialogRef = {
            close: jest.fn()
        };

        await TestBed.configureTestingModule({
            imports: [EditPolicyDialogComponent, NoopAnimationsModule],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: {
                        identity: 'alice',
                        type: 'user',
                        currentPermissions: ['read', 'write']
                    } as EditPolicyDialogData
                },
                { provide: MatDialogRef, useValue: mockDialogRef }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(EditPolicyDialogComponent);
        component = fixture.componentInstance;
        dialogRef = TestBed.inject(MatDialogRef) as jest.Mocked<MatDialogRef<EditPolicyDialogComponent>>;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should initialize with current permissions checked', () => {
        expect(component.readChecked).toBe(true);
        expect(component.writeChecked).toBe(true);
        expect(component.deleteChecked).toBe(false);
    });

    it('should calculate allChecked correctly', () => {
        component.readChecked = true;
        component.writeChecked = true;
        component.deleteChecked = true;
        expect(component.allChecked).toBe(true);

        component.readChecked = false;
        expect(component.allChecked).toBe(false);
    });

    it('should toggle all permissions', () => {
        component.toggleAll(true);
        expect(component.readChecked).toBe(true);
        expect(component.writeChecked).toBe(true);
        expect(component.deleteChecked).toBe(true);

        component.toggleAll(false);
        expect(component.readChecked).toBe(false);
        expect(component.writeChecked).toBe(false);
        expect(component.deleteChecked).toBe(false);
    });

    it('should disable apply when no permissions checked', () => {
        component.readChecked = false;
        component.writeChecked = false;
        component.deleteChecked = false;
        expect(component.canApply).toBe(false);
    });

    it('should enable apply when at least one permission checked', () => {
        component.readChecked = true;
        expect(component.canApply).toBe(true);
    });

    it('should close dialog with updated permissions when apply clicked', () => {
        component.deleteChecked = true; // Add delete permission
        component.apply();

        expect(dialogRef.close).toHaveBeenCalledWith({
            permissions: ['read', 'write', 'delete']
        });
    });

    it('should return only checked permissions', () => {
        component.readChecked = true;
        component.writeChecked = false;
        component.deleteChecked = true;

        component.apply();

        expect(dialogRef.close).toHaveBeenCalledWith({
            permissions: ['read', 'delete']
        });
    });

    it('should close dialog without result when cancel clicked', () => {
        component.cancel();
        expect(dialogRef.close).toHaveBeenCalledWith();
    });

    it('should handle group type correctly', () => {
        component['data'] = {
            identity: 'admins',
            type: 'group',
            currentPermissions: ['delete']
        };

        component.ngOnInit();

        expect(component['data'].type).toBe('group');
        expect(component.deleteChecked).toBe(true);
        expect(component.readChecked).toBe(false);
        expect(component.writeChecked).toBe(false);
    });
});
