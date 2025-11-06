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
import {
    ManageBucketPoliciesDialogComponent,
    ManageBucketPoliciesDialogData
} from './manage-bucket-policies-dialog.component';
import { MAT_DIALOG_DATA, MatDialog, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { Bucket } from '../../../../../state/buckets';
import { BehaviorSubject, of, Subject } from 'rxjs';
import { PolicySelection, BucketPolicyOptionsView } from '../../../../../state/policies';
import { PolicySubject } from '../../../../../service/buckets.service';
import { AddPolicyToBucketDialogComponent } from '../add-policy-to-bucket-dialog/add-policy-to-bucket-dialog.component';
import { EditPolicyDialogComponent } from '../edit-policy-dialog/edit-policy-dialog.component';
import { provideMockStore } from '@ngrx/store/testing';
import { errorFeatureKey } from '../../../../../state/error';

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

const mockOptions: BucketPolicyOptionsView = {
    groups: [{ key: 'group-1', label: 'admins', type: 'group', subject: group1 }],
    users: [
        { key: 'user-1', label: 'alice', type: 'user', subject: user1 },
        { key: 'user-2', label: 'bob', type: 'user', subject: user2 }
    ],
    all: [
        { key: 'group-1', label: 'admins', type: 'group', subject: group1 },
        { key: 'user-1', label: 'alice', type: 'user', subject: user1 },
        { key: 'user-2', label: 'bob', type: 'user', subject: user2 }
    ],
    lookup: {
        'group-1': group1,
        'user-1': user1,
        'user-2': user2
    }
};

const mockSelection: Partial<Record<'read' | 'write' | 'delete', PolicySelection>> = {
    read: {
        policyId: 'policy-1',
        revision: { version: 0 },
        users: [user1],
        userGroups: [group1]
    },
    write: {
        policyId: 'policy-2',
        revision: { version: 0 },
        users: [user1],
        userGroups: []
    }
};

describe('ManageBucketPoliciesDialogComponent', () => {
    let component: ManageBucketPoliciesDialogComponent;
    let fixture: ComponentFixture<ManageBucketPoliciesDialogComponent>;
    let dialogRef: jest.Mocked<MatDialogRef<ManageBucketPoliciesDialogComponent>>;

    beforeEach(async () => {
        const options$ = new BehaviorSubject<BucketPolicyOptionsView>(mockOptions);
        const selection$ = new BehaviorSubject(mockSelection);
        const loading$ = new BehaviorSubject<boolean>(false);
        const saving$ = new BehaviorSubject<boolean>(false);
        const isAddPolicyDisabled$ = new BehaviorSubject<boolean>(false);
        const isPolicyError$ = new BehaviorSubject<boolean>(false);

        const mockDialogRef = {
            close: jest.fn()
        };

        const afterOpenedSubject = new Subject();
        const afterAllClosedSubject = new Subject();
        const mockDialog = {
            open: jest.fn().mockReturnValue({
                afterClosed: () => of(undefined)
            }),
            _getAfterAllClosed: jest.fn(),
            get afterAllClosed() {
                return afterAllClosedSubject.asObservable();
            },
            _openDialogsAtThisLevel: [],
            _afterAllClosedAtThisLevel: afterAllClosedSubject,
            _afterOpenedAtThisLevel: afterOpenedSubject,
            get afterOpened() {
                return afterOpenedSubject.asObservable();
            },
            openDialogs: []
        };

        await TestBed.configureTestingModule({
            imports: [ManageBucketPoliciesDialogComponent, NoopAnimationsModule],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: {
                            bannerErrors: {},
                            dialogErrors: {}
                        }
                    }
                }),
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: {
                        bucket,
                        options$,
                        selection$,
                        loading$,
                        isPolicyError$,
                        isAddPolicyDisabled$,
                        saving$
                    } as ManageBucketPoliciesDialogData
                },
                { provide: MatDialogRef, useValue: mockDialogRef },
                { provide: MatDialog, useValue: mockDialog }
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ManageBucketPoliciesDialogComponent);
        component = fixture.componentInstance;
        dialogRef = TestBed.inject(MatDialogRef) as jest.Mocked<MatDialogRef<ManageBucketPoliciesDialogComponent>>;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should build table rows from policy selections', () => {
        expect(component.dataSource.data.length).toBe(2);

        const aliceRow = component.dataSource.data.find((r) => r.identity === 'alice');
        expect(aliceRow).toBeDefined();
        expect(aliceRow!.permissions).toBe('read, write');
        expect(aliceRow!.actions).toEqual(['read', 'write']);

        const adminsRow = component.dataSource.data.find((r) => r.identity === 'admins');
        expect(adminsRow).toBeDefined();
        expect(adminsRow!.permissions).toBe('read');
        expect(adminsRow!.actions).toEqual(['read']);
    });

    it('should sort table data correctly', () => {
        component.sortData({ active: 'identity', direction: 'desc' });
        expect(component.dataSource.data[0].identity).toBe('alice'); // Descending
    });

    it('should open add policy dialog when addPolicy is called', () => {
        const dialogSpy = jest.spyOn(component['dialog'], 'open').mockReturnValue({
            afterClosed: () => of(undefined)
        } as any);

        component.addPolicy();

        expect(dialogSpy).toHaveBeenCalledWith(
            AddPolicyToBucketDialogComponent,
            expect.objectContaining({
                autoFocus: false,
                data: expect.objectContaining({
                    bucket
                })
            })
        );
    });

    it('should open edit policy dialog when editPolicy is called', () => {
        const dialogSpy = jest.spyOn(component['dialog'], 'open').mockReturnValue({
            afterClosed: () => of(undefined)
        } as any);

        const row = component.dataSource.data[0];
        component.editPolicy(row);

        expect(dialogSpy).toHaveBeenCalledWith(
            EditPolicyDialogComponent,
            expect.objectContaining({
                data: expect.objectContaining({
                    identity: row.identity,
                    type: row.type,
                    currentPermissions: row.actions
                })
            })
        );
    });

    it('should emit savePolicies when addPolicy dialog returns result', (done) => {
        const addResult = {
            userOrGroup: user2,
            permissions: ['read', 'write']
        };
        jest.spyOn(component['dialog'], 'open').mockReturnValue({
            afterClosed: () => of(addResult)
        } as any);

        let emitCount = 0;
        component.savePolicies.subscribe((request) => {
            expect(request.bucketId).toBe(bucket.identifier);
            expect(['read', 'write']).toContain(request.action);
            emitCount++;
            if (emitCount === 2) {
                done();
            }
        });

        component.addPolicy();
    });

    it('should emit savePolicies when editPolicy dialog returns result', (done) => {
        // Wait for initialization to complete
        setTimeout(() => {
            const row = component.dataSource.data.find((r) => r.identity === 'alice')!; // alice with ['read', 'write']
            const editResult = {
                permissions: ['read'] // Removing 'write'
            };
            jest.spyOn(component['dialog'], 'open').mockReturnValue({
                afterClosed: () => of(editResult)
            } as any);

            let emitCount = 0;
            component.savePolicies.subscribe((request) => {
                expect(request.bucketId).toBe(bucket.identifier);
                emitCount++;
                // Should emit once for removing 'write'
                if (emitCount === 1) {
                    expect(request.action).toBe('write');
                    done();
                }
            });

            component.editPolicy(row);
        }, 100);
    });

    it('should open confirmation dialog and emit savePolicies when removePolicy is called', (done) => {
        const yesSubject = new Subject();
        jest.spyOn(component['dialog'], 'open').mockReturnValue({
            componentInstance: {
                yes: yesSubject.asObservable()
            },
            afterClosed: () => of(undefined)
        } as any);

        const row = component.dataSource.data[0];

        let emitCount = 0;
        const expectedEmissions = row.actions.length;
        component.savePolicies.subscribe((request) => {
            expect(request.bucketId).toBe(bucket.identifier);
            expect(row.actions).toContain(request.action);
            emitCount++;
            if (emitCount === expectedEmissions) {
                done();
            }
        });

        component.removePolicy(row);
        yesSubject.next(true);
    });

    it('should format permissions correctly', () => {
        const permissions = component['formatPermissions'](['write', 'read', 'delete']);
        expect(permissions).toBe('delete, read, write'); // Sorted alphabetically
    });

    it('should sort policies by identity', () => {
        component.sortData({ active: 'identity', direction: 'asc' });
        const firstRow = component.dataSource.data[0];
        expect(['admins', 'alice']).toContain(firstRow.identity);
    });

    it('should sort policies by type', () => {
        component.sortData({ active: 'type', direction: 'asc' });
        const data = component.dataSource.data;
        expect(data[0].type).toBe('group'); // 'group' comes before 'user'
    });

    it('should close dialog when close is called', () => {
        component.close();
        expect(dialogRef.close).toHaveBeenCalled();
    });
});
