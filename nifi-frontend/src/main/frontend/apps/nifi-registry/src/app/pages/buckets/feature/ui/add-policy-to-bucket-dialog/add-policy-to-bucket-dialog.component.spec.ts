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
import { AddPolicyToBucketDialogComponent, AddPolicyToBucketDialogData } from './add-policy-to-bucket-dialog.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { PolicySubject } from 'apps/nifi-registry/src/app/service/buckets.service';
import { Bucket } from 'apps/nifi-registry/src/app/state/buckets';

const bucket: Bucket = {
    allowBundleRedeploy: false,
    allowPublicRead: false,
    createdTimestamp: Date.now(),
    description: 'Test Bucket',
    identifier: 'bucket-1',
    link: { href: '', params: { rel: '' } },
    name: 'Test Bucket',
    permissions: { canRead: true, canWrite: true },
    revision: { version: 1 }
};

const user1: PolicySubject = {
    identifier: 'user-1',
    identity: 'alice',
    type: 'user',
    configurable: false
};

const user2: PolicySubject = {
    identifier: 'user-2',
    identity: 'bob',
    type: 'user',
    configurable: false
};

const group1: PolicySubject = {
    identifier: 'group-1',
    identity: 'admins',
    type: 'group',
    configurable: false
};

describe('AddPolicyToBucketDialogComponent', () => {
    let component: AddPolicyToBucketDialogComponent;
    let fixture: ComponentFixture<AddPolicyToBucketDialogComponent>;
    let dialogRef: jest.Mocked<MatDialogRef<AddPolicyToBucketDialogComponent>>;

    beforeEach(async () => {
        const mockDialogRef = {
            close: jest.fn()
        };

        await TestBed.configureTestingModule({
            imports: [AddPolicyToBucketDialogComponent, NoopAnimationsModule],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: {
                        bucket,
                        existingUsers: [],
                        existingGroups: [],
                        availableUsers: [user1, user2],
                        availableGroups: [group1]
                    } as AddPolicyToBucketDialogData
                },
                { provide: MatDialogRef, useValue: mockDialogRef }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(AddPolicyToBucketDialogComponent);
        component = fixture.componentInstance;
        dialogRef = TestBed.inject(MatDialogRef) as jest.Mocked<MatDialogRef<AddPolicyToBucketDialogComponent>>;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should initialize with available users and groups', () => {
        expect(component.dataSource.data.length).toBe(3); // 2 users + 1 group
        expect(component.dataSource.data[0].type).toBe('group'); // Groups sorted first
        expect(component.dataSource.data[1].identity).toBe('alice');
        expect(component.dataSource.data[2].identity).toBe('bob');
    });

    it('should filter out existing users and groups', () => {
        // Re-initialize component with different data by updating the data source
        component['data'] = {
            bucket,
            existingUsers: ['alice'],
            existingGroups: ['admins'],
            availableUsers: [user1, user2],
            availableGroups: [group1]
        };

        component.ngOnInit();

        expect(component.dataSource.data.length).toBe(1); // Only bob
        expect(component.dataSource.data[0].identity).toBe('bob');
    });

    it('should select a row when clicked', () => {
        const row = component.dataSource.data[1];
        component.selectRow(row);

        expect(component.selectedRow).toBe(row);
        expect(row.selected).toBe(true);
    });

    it('should deselect other rows when selecting a new row', () => {
        const row1 = component.dataSource.data[0];
        const row2 = component.dataSource.data[1];

        component.selectRow(row1);
        expect(row1.selected).toBe(true);

        component.selectRow(row2);
        expect(row1.selected).toBe(false);
        expect(row2.selected).toBe(true);
        expect(component.selectedRow).toBe(row2);
    });

    it('should toggle all permissions when toggleAll is called', () => {
        component.toggleAll(true);
        expect(component.readChecked).toBe(true);
        expect(component.writeChecked).toBe(true);
        expect(component.deleteChecked).toBe(true);

        component.toggleAll(false);
        expect(component.readChecked).toBe(false);
        expect(component.writeChecked).toBe(false);
        expect(component.deleteChecked).toBe(false);
    });

    it('should calculate allChecked getter correctly', () => {
        component.readChecked = true;
        component.writeChecked = true;
        component.deleteChecked = true;
        expect(component.allChecked).toBe(true);

        component.deleteChecked = false;
        expect(component.allChecked).toBe(false);
    });

    it('should disable apply when no row selected', () => {
        component.readChecked = true;
        expect(component.canApply).toBe(false);
    });

    it('should disable apply when no permissions checked', () => {
        component.selectRow(component.dataSource.data[0]);
        component.readChecked = false;
        component.writeChecked = false;
        component.deleteChecked = false;
        expect(component.canApply).toBe(false);
    });

    it('should enable apply when row selected and permission checked', () => {
        component.selectRow(component.dataSource.data[0]);
        component.readChecked = true;
        expect(component.canApply).toBe(true);
    });

    it('should close dialog with result when apply is clicked', () => {
        const row = component.dataSource.data[1]; // alice
        component.selectRow(row);
        component.readChecked = true;
        component.writeChecked = true;

        component.apply();

        expect(dialogRef.close).toHaveBeenCalledWith({
            userOrGroup: {
                identifier: 'user-1',
                identity: 'alice',
                type: 'user'
            },
            permissions: ['read', 'write']
        });
    });

    it('should not apply when no row selected', () => {
        component.readChecked = true;
        component.apply();
        expect(dialogRef.close).not.toHaveBeenCalled();
    });

    it('should close dialog without result when cancel is clicked', () => {
        component.cancel();
        expect(dialogRef.close).toHaveBeenCalledWith();
    });

    it('should sort data correctly', () => {
        component.sortData({ active: 'identity', direction: 'desc' });
        expect(component.dataSource.data[0].identity).toBe('bob'); // Descending order
    });
});
