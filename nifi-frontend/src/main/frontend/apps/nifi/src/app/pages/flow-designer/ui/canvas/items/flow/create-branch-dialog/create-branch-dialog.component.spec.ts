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

import { Signal, signal } from '@angular/core';
import { CreateBranchDialog } from './create-branch-dialog.component';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { CreateBranchDialogRequest } from '../../../../../state/flow';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../../state/flow/flow.reducer';
import { canvasFeatureKey, flowFeatureKey } from '../../../../../state';
import { errorFeatureKey } from '../../../../../../../state/error';
import { initialState as errorInitialState } from '../../../../../../../state/error/error.reducer';

describe('CreateBranchDialog', () => {
    let component: CreateBranchDialog;
    let fixture: ComponentFixture<CreateBranchDialog>;

    const data: CreateBranchDialogRequest = {
        processGroupId: '5752a5ae-018d-1000-0990-c3709f5466f3',
        revision: {
            version: 0
        },
        versionControlInformation: {
            groupId: '5752a5ae-018d-1000-0990-c3709f5466f3',
            registryId: '324e0ab1-0197-1000-ffff-ffffb3123c5c',
            registryName: 'ConnectorFlowRegistryClient',
            branch: 'main',
            bucketId: 'connectors',
            bucketName: 'connectors',
            flowId: 'kafka',
            flowName: 'kafka',
            flowDescription: '',
            version: '0.1.0',
            state: 'UP_TO_DATE',
            stateExplanation: 'Flow version is current'
        }
    };

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [CreateBranchDialog, MatDialogModule, NoopAnimationsModule],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: data
                },
                provideMockStore({
                    initialState: {
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialState
                        },
                        [errorFeatureKey]: errorInitialState
                    }
                }),
                { provide: MatDialogRef, useValue: null }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(CreateBranchDialog);
        component = fixture.componentInstance;
        component.saving = (() => false) as Signal<boolean>;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
        expect(component.currentBranch).toBe('main');
    });

    it('should be invalid when the branch name is empty', () => {
        component.createBranchForm.controls['branch'].setValue('');
        expect(component.createBranchForm.invalid).toBe(true);
        expect(component.createBranchForm.controls['branch'].hasError('required')).toBe(true);
    });

    it('should be invalid when the branch name starts with whitespace', () => {
        component.createBranchForm.controls['branch'].setValue(' feature');
        expect(component.createBranchForm.controls['branch'].hasError('pattern')).toBe(true);
    });

    it('should be invalid when the branch name matches the current branch', () => {
        component.createBranchForm.controls['branch'].setValue('main');
        expect(component.createBranchForm.controls['branch'].hasError('branchConflicts')).toBe(true);
    });

    it('should show an error and disable Create when the branch name matches the current branch', () => {
        component.createBranchForm.controls['branch'].setValue('main');
        fixture.detectChanges();

        const error: HTMLElement = fixture.nativeElement.querySelector('mat-error');
        const btn: HTMLButtonElement = fixture.nativeElement.querySelector('button[aria-label="Create"]');

        expect(error.textContent).toContain('Must differ from current branch.');
        expect(btn.disabled).toBe(true);
    });

    it('should emit the trimmed branch name when the form is valid', () => {
        const emitted: string[] = [];
        component.createBranch.subscribe((branch) => emitted.push(branch));

        component.createBranchForm.controls['branch'].setValue('feature-branch');
        component.submitForm();

        expect(emitted).toEqual(['feature-branch']);
    });

    it('should not emit when the form is invalid', () => {
        const emitted: string[] = [];
        component.createBranch.subscribe((branch) => emitted.push(branch));

        component.createBranchForm.controls['branch'].setValue('main');
        component.submitForm();

        expect(emitted).toEqual([]);
    });

    it('should disable the Create button while a request is in flight', () => {
        const saving = signal(true);
        component.saving = saving;
        component.createBranchForm.controls['branch'].setValue('feature-branch');
        fixture.detectChanges();
        const btn: HTMLButtonElement = fixture.nativeElement.querySelector('button[aria-label="Create"]');
        expect(btn.disabled).toBe(true);
    });

    it('should re-enable the Create button when the request completes', () => {
        const saving = signal(true);
        component.saving = saving;
        component.createBranchForm.controls['branch'].setValue('feature-branch');
        fixture.detectChanges();
        saving.set(false);
        fixture.detectChanges();
        const btn: HTMLButtonElement = fixture.nativeElement.querySelector('button[aria-label="Create"]');
        expect(btn.disabled).toBe(false);
    });
});