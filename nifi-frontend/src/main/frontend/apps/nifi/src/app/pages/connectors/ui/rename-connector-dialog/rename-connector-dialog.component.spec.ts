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

import { TestBed } from '@angular/core/testing';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { BehaviorSubject, EMPTY } from 'rxjs';
import { RenameConnectorDialog, RenameConnectorDialogData } from './rename-connector-dialog.component';
import { ConnectorEntity } from '@nifi/shared';
import { errorFeatureKey, ErrorState } from '../../../../state/error';

describe('RenameConnectorDialog', () => {
    function createMockConnector(overrides: Partial<ConnectorEntity> = {}): ConnectorEntity {
        return {
            id: 'connector-1',
            uri: '/connectors/connector-1',
            revision: { version: 1 },
            permissions: { canRead: true, canWrite: true },
            bulletins: [],
            status: {},
            component: {
                id: 'connector-1',
                name: 'Test Connector',
                type: 'org.apache.nifi.TestConnector',
                state: 'STOPPED',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-test-nar',
                    version: '1.0.0'
                },
                managedProcessGroupId: 'pg-root-123',
                availableActions: [
                    { name: 'START', description: 'Start action', allowed: true },
                    { name: 'STOP', description: 'Stop action', allowed: true },
                    { name: 'CONFIGURE', description: 'Configure action', allowed: true },
                    { name: 'DELETE', description: 'Delete action', allowed: true }
                ]
            },
            ...overrides
        };
    }

    interface SetupOptions {
        connector?: ConnectorEntity;
        saving?: boolean;
    }

    async function setup(options: SetupOptions = {}) {
        const connector = options.connector ?? createMockConnector();
        const savingSubject = new BehaviorSubject<boolean>(options.saving ?? false);

        const dialogData: RenameConnectorDialogData = { connector };

        const initialErrorState: ErrorState = {
            bannerErrors: {},
            fullScreenError: null,
            routedToFullScreenError: false
        };

        await TestBed.configureTestingModule({
            imports: [RenameConnectorDialog, MatDialogModule, NoopAnimationsModule],
            providers: [
                provideMockStore({
                    initialState: {
                        [errorFeatureKey]: initialErrorState
                    }
                }),
                { provide: MAT_DIALOG_DATA, useValue: dialogData },
                {
                    provide: MatDialogRef,
                    useValue: {
                        close: vi.fn(),
                        keydownEvents: vi.fn().mockReturnValue(EMPTY)
                    }
                }
            ]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const fixture = TestBed.createComponent(RenameConnectorDialog);
        const component = fixture.componentInstance;
        component.saving$ = savingSubject.asObservable();

        fixture.detectChanges();

        return { fixture, component, store, savingSubject, connector };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should initialize form with current connector name', async () => {
            const connector = createMockConnector({
                component: {
                    id: 'connector-1',
                    name: 'My Connector',
                    type: 'org.apache.nifi.TestConnector',
                    state: 'STOPPED',
                    bundle: {
                        group: 'org.apache.nifi',
                        artifact: 'nifi-test-nar',
                        version: '1.0.0'
                    },
                    managedProcessGroupId: 'pg-root-123',
                    availableActions: [
                        { name: 'START', description: 'Start action', allowed: true },
                        { name: 'STOP', description: 'Stop action', allowed: true },
                        { name: 'CONFIGURE', description: 'Configure action', allowed: true },
                        { name: 'DELETE', description: 'Delete action', allowed: true }
                    ]
                }
            });
            const { component } = await setup({ connector });

            expect(component.renameForm.get('name')?.value).toBe('My Connector');
            expect(component.currentName).toBe('My Connector');
        });
    });

    describe('form validation', () => {
        it('should be invalid when name is empty', async () => {
            const { component } = await setup();

            component.renameForm.get('name')?.setValue('');
            expect(component.renameForm.invalid).toBe(true);
        });

        it('should be invalid when name is only whitespace', async () => {
            const { component } = await setup();

            component.renameForm.get('name')?.setValue('   ');
            expect(component.renameForm.invalid).toBe(true);
        });

        it('should be invalid when name is same as current name', async () => {
            const { component } = await setup();

            expect(component.renameForm.invalid).toBe(true);
        });

        it('should be valid when name is different from current name', async () => {
            const { component } = await setup();

            component.renameForm.get('name')?.setValue('New Connector Name');
            expect(component.renameForm.valid).toBe(true);
        });

        it('should show required error message', async () => {
            const { component } = await setup();

            component.renameForm.get('name')?.setValue('');
            expect(component.getNameErrorMessage()).toBe('Name is required.');
        });

        it('should show not blank error message', async () => {
            const { component } = await setup();

            component.renameForm.get('name')?.setValue('   ');
            expect(component.getNameErrorMessage()).toBe('Name cannot be blank.');
        });

        it('should show same name error message', async () => {
            const { component } = await setup();

            expect(component.getNameErrorMessage()).toBe('Name must be different from the current name.');
        });
    });

    describe('submit behavior', () => {
        it('should emit rename event with correct data when form is valid', async () => {
            const { component, connector } = await setup();
            const renameSpy = vi.spyOn(component.rename, 'emit');

            component.renameForm.get('name')?.setValue('New Connector Name');
            component.renameClicked();

            expect(renameSpy).toHaveBeenCalledWith({
                connector,
                newName: 'New Connector Name'
            });
        });

        it('should trim the new name before emitting', async () => {
            const { component, connector } = await setup();
            const renameSpy = vi.spyOn(component.rename, 'emit');

            component.renameForm.get('name')?.setValue('  New Name  ');
            component.renameClicked();

            expect(renameSpy).toHaveBeenCalledWith({
                connector,
                newName: 'New Name'
            });
        });

        it('should not emit rename event when form is invalid', async () => {
            const { component } = await setup();
            const renameSpy = vi.spyOn(component.rename, 'emit');

            component.renameClicked();

            expect(renameSpy).not.toHaveBeenCalled();
        });
    });

    describe('cancel behavior', () => {
        it('should emit exit event when cancel is clicked', async () => {
            const { component } = await setup();
            const exitSpy = vi.spyOn(component.exit, 'emit');

            component.cancelClicked();

            expect(exitSpy).toHaveBeenCalled();
        });
    });

    describe('dirty state', () => {
        it('should report dirty when form is modified', async () => {
            const { component } = await setup();

            expect(component.isDirty()).toBe(false);

            component.renameForm.get('name')?.setValue('New Name');
            component.renameForm.get('name')?.markAsDirty();

            expect(component.isDirty()).toBe(true);
        });
    });

    describe('submit button state', () => {
        it('should disable submit button when saving', async () => {
            const { fixture, savingSubject } = await setup();

            savingSubject.next(true);
            fixture.detectChanges();

            const submitButton = fixture.nativeElement.querySelector('[data-qa="rename-connector-submit-button"]');
            expect(submitButton.disabled).toBe(true);
        });

        it('should disable submit button when form is invalid', async () => {
            const { fixture, component } = await setup();

            component.renameForm.get('name')?.setValue('');
            fixture.detectChanges();

            const submitButton = fixture.nativeElement.querySelector('[data-qa="rename-connector-submit-button"]');
            expect(submitButton.disabled).toBe(true);
        });

        it('should enable submit button when form is valid and not saving', async () => {
            const { fixture, component } = await setup();

            component.renameForm.get('name')?.setValue('New Connector Name');
            fixture.detectChanges();

            const submitButton = fixture.nativeElement.querySelector('[data-qa="rename-connector-submit-button"]');
            expect(submitButton.disabled).toBe(false);
        });
    });
});
