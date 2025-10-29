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

import { Component } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { MatDialogRef } from '@angular/material/dialog';
import { Storage } from '@nifi/shared';
import { TabbedDialog, TABBED_DIALOG_ID } from './tabbed-dialog.component';

// Concrete test implementation of the abstract TabbedDialog class
@Component({
    selector: 'test-tabbed-dialog',
    template: '<div>Test Component</div>',
    standalone: true
})
class TestTabbedDialogComponent extends TabbedDialog {
    // Expose private properties for testing
    getDialogId(): string {
        return (this as any).dialogId;
    }

    getStorage(): Storage {
        return (this as any).storage;
    }
}

interface SetupOptions {
    customDialogId?: string;
    initialStorageValue?: number;
}

async function setup(options: SetupOptions = {}) {
    const mockStorage = {
        getItem: jest.fn(),
        setItem: jest.fn()
    };

    const mockDialogRef = {
        keydownEvents: jest.fn().mockReturnValue({
            pipe: jest.fn().mockReturnValue({
                subscribe: jest.fn()
            })
        }),
        close: jest.fn()
    };

    const providers: any[] = [
        { provide: Storage, useValue: mockStorage },
        { provide: MatDialogRef, useValue: mockDialogRef }
    ];

    // Add custom dialog ID provider if specified
    if (options.customDialogId !== undefined) {
        providers.push({
            provide: TABBED_DIALOG_ID,
            useValue: options.customDialogId
        });
    }

    // Setup initial storage value if specified
    if (options.initialStorageValue !== undefined) {
        mockStorage.getItem.mockReturnValue(options.initialStorageValue);
    }

    await TestBed.configureTestingModule({
        imports: [TestTabbedDialogComponent],
        providers
    }).compileComponents();

    const fixture = TestBed.createComponent(TestTabbedDialogComponent);
    const component = fixture.componentInstance;

    return {
        fixture,
        component,
        mockStorage,
        mockDialogRef
    };
}

describe('TabbedDialog', () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    describe('Dialog ID Configuration', () => {
        it('should use default dialog ID when no injection token is provided', async () => {
            const { component } = await setup();

            expect(component.getDialogId()).toBe('tabbed-dialog-selected-index');
        });

        it('should use injected dialog ID when injection token is provided', async () => {
            const customDialogId = 'custom-test-dialog-id';
            const { component } = await setup({ customDialogId });

            expect(component.getDialogId()).toBe(customDialogId);
        });

        it('should use injected dialog ID even when it is an empty string', async () => {
            const { component } = await setup({ customDialogId: '' });

            expect(component.getDialogId()).toBe('');
        });
    });

    describe('Storage Integration', () => {
        it('should retrieve previous selected index from storage using default dialog ID', async () => {
            const { component, mockStorage } = await setup({
                initialStorageValue: 2
            });

            expect(mockStorage.getItem).toHaveBeenCalledWith('tabbed-dialog-selected-index');
            expect(component.selectedIndex).toBe(2);
        });

        it('should retrieve previous selected index from storage using custom dialog ID', async () => {
            const customDialogId = 'edit-parameter-context-dialog';
            const { component, mockStorage } = await setup({
                customDialogId,
                initialStorageValue: 3
            });

            expect(mockStorage.getItem).toHaveBeenCalledWith(customDialogId);
            expect(component.selectedIndex).toBe(3);
        });

        it('should use default selectedIndex when storage returns null', async () => {
            const { component, mockStorage } = await setup();

            mockStorage.getItem.mockReturnValue(null);

            expect(component.selectedIndex).toBe(0);
        });

        it('should use default selectedIndex when storage returns undefined', async () => {
            const { component, mockStorage } = await setup();

            mockStorage.getItem.mockReturnValue(undefined);

            expect(component.selectedIndex).toBe(0);
        });
    });

    describe('Tab Change Functionality', () => {
        it('should save tab index to storage using default dialog ID', async () => {
            const { component, mockStorage } = await setup();

            component.tabChanged(1);

            expect(mockStorage.setItem).toHaveBeenCalledWith('tabbed-dialog-selected-index', 1);
        });

        it('should save tab index to storage using custom dialog ID', async () => {
            const customDialogId = 'custom-dialog-test';
            const { component, mockStorage } = await setup({ customDialogId });

            component.tabChanged(2);

            expect(mockStorage.setItem).toHaveBeenCalledWith(customDialogId, 2);
        });

        it('should handle zero tab index correctly', async () => {
            const { component, mockStorage } = await setup();

            component.tabChanged(0);

            expect(mockStorage.setItem).toHaveBeenCalledWith('tabbed-dialog-selected-index', 0);
        });

        it('should handle negative tab index (edge case)', async () => {
            const { component, mockStorage } = await setup();

            component.tabChanged(-1);

            expect(mockStorage.setItem).toHaveBeenCalledWith('tabbed-dialog-selected-index', -1);
        });
    });

    describe('Integration Scenarios', () => {
        it('should maintain separate storage entries for different dialog IDs', async () => {
            // Test first dialog with custom ID
            const mockStorage1 = {
                getItem: jest.fn().mockReturnValue(1),
                setItem: jest.fn()
            };

            TestBed.resetTestingModule();
            await TestBed.configureTestingModule({
                imports: [TestTabbedDialogComponent],
                providers: [
                    { provide: Storage, useValue: mockStorage1 },
                    {
                        provide: MatDialogRef,
                        useValue: {
                            keydownEvents: jest
                                .fn()
                                .mockReturnValue({ pipe: jest.fn().mockReturnValue({ subscribe: jest.fn() }) }),
                            close: jest.fn()
                        }
                    },
                    { provide: TABBED_DIALOG_ID, useValue: 'dialog-1' }
                ]
            }).compileComponents();

            const fixture1 = TestBed.createComponent(TestTabbedDialogComponent);
            const component1 = fixture1.componentInstance;

            // Verify first dialog uses its own storage key
            expect(mockStorage1.getItem).toHaveBeenCalledWith('dialog-1');
            expect(component1.selectedIndex).toBe(1);

            // Change tab in first dialog
            component1.tabChanged(3);
            expect(mockStorage1.setItem).toHaveBeenCalledWith('dialog-1', 3);

            // Test second dialog with different custom ID
            const mockStorage2 = {
                getItem: jest.fn().mockReturnValue(2),
                setItem: jest.fn()
            };

            TestBed.resetTestingModule();
            await TestBed.configureTestingModule({
                imports: [TestTabbedDialogComponent],
                providers: [
                    { provide: Storage, useValue: mockStorage2 },
                    {
                        provide: MatDialogRef,
                        useValue: {
                            keydownEvents: jest
                                .fn()
                                .mockReturnValue({ pipe: jest.fn().mockReturnValue({ subscribe: jest.fn() }) }),
                            close: jest.fn()
                        }
                    },
                    { provide: TABBED_DIALOG_ID, useValue: 'dialog-2' }
                ]
            }).compileComponents();

            const fixture2 = TestBed.createComponent(TestTabbedDialogComponent);
            const component2 = fixture2.componentInstance;

            // Verify second dialog uses its own storage key
            expect(mockStorage2.getItem).toHaveBeenCalledWith('dialog-2');
            expect(component2.selectedIndex).toBe(2);

            // Change tab in second dialog
            component2.tabChanged(4);
            expect(mockStorage2.setItem).toHaveBeenCalledWith('dialog-2', 4);
        });

        it('should work correctly when switching from default to custom dialog ID', async () => {
            // Test default dialog ID
            const mockStorage1 = {
                getItem: jest.fn().mockReturnValue(1),
                setItem: jest.fn()
            };

            TestBed.resetTestingModule();
            await TestBed.configureTestingModule({
                imports: [TestTabbedDialogComponent],
                providers: [
                    { provide: Storage, useValue: mockStorage1 },
                    {
                        provide: MatDialogRef,
                        useValue: {
                            keydownEvents: jest
                                .fn()
                                .mockReturnValue({ pipe: jest.fn().mockReturnValue({ subscribe: jest.fn() }) }),
                            close: jest.fn()
                        }
                    }
                ]
            }).compileComponents();

            TestBed.createComponent(TestTabbedDialogComponent);

            expect(mockStorage1.getItem).toHaveBeenCalledWith('tabbed-dialog-selected-index');

            // Test custom dialog ID
            const mockStorage2 = {
                getItem: jest.fn().mockReturnValue(5),
                setItem: jest.fn()
            };

            TestBed.resetTestingModule();
            await TestBed.configureTestingModule({
                imports: [TestTabbedDialogComponent],
                providers: [
                    { provide: Storage, useValue: mockStorage2 },
                    {
                        provide: MatDialogRef,
                        useValue: {
                            keydownEvents: jest
                                .fn()
                                .mockReturnValue({ pipe: jest.fn().mockReturnValue({ subscribe: jest.fn() }) }),
                            close: jest.fn()
                        }
                    },
                    { provide: TABBED_DIALOG_ID, useValue: 'custom-id' }
                ]
            }).compileComponents();

            const fixture2 = TestBed.createComponent(TestTabbedDialogComponent);
            const component2 = fixture2.componentInstance;

            expect(mockStorage2.getItem).toHaveBeenCalledWith('custom-id');
            expect(component2.selectedIndex).toBe(5);
        });
    });

    describe('Error Handling', () => {
        it('should handle storage errors gracefully during initialization', async () => {
            const mockStorage = {
                getItem: jest.fn().mockImplementation(() => {
                    throw new Error('storage error');
                }),
                setItem: jest.fn()
            };

            const mockDialogRef = {
                keydownEvents: jest.fn().mockReturnValue({
                    pipe: jest.fn().mockReturnValue({
                        subscribe: jest.fn()
                    })
                }),
                close: jest.fn()
            };

            // Should not throw an error during component creation
            expect(() => {
                TestBed.resetTestingModule();
                TestBed.configureTestingModule({
                    imports: [TestTabbedDialogComponent],
                    providers: [
                        { provide: Storage, useValue: mockStorage },
                        { provide: MatDialogRef, useValue: mockDialogRef }
                    ]
                }).compileComponents();

                const fixture = TestBed.createComponent(TestTabbedDialogComponent);
                const component = fixture.componentInstance;

                // Component should still be created with default selectedIndex
                expect(component.selectedIndex).toBe(0);
            }).not.toThrow();
        });

        it('should handle storage errors gracefully during tabChanged', async () => {
            const { component, mockStorage } = await setup();

            mockStorage.setItem.mockImplementation(() => {
                throw new Error('storage error');
            });

            // Should not throw an error when calling tabChanged
            expect(() => {
                component.tabChanged(1);
            }).not.toThrow();
        });
    });
});
