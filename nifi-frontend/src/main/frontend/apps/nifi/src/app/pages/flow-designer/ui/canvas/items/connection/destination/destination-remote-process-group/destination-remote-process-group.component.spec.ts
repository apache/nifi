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
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { DestinationRemoteProcessGroup } from './destination-remote-process-group.component';

describe('DestinationRemoteProcessGroup Component', () => {
    // Mock data factories
    function createMockInputPort(
        id: string = 'input-port-id',
        name: string = 'Test Input Port',
        comments: string = '',
        exists: boolean = true
    ) {
        return {
            id,
            name,
            comments,
            exists
        };
    }

    function createMockRemoteProcessGroup(groupName: string = 'Test Remote Process Group', inputPorts: any[] = []) {
        return {
            component: {
                name: groupName,
                contents: {
                    inputPorts
                }
            }
        };
    }

    // Setup function for component configuration
    async function setup(
        options: {
            remoteProcessGroup?: any;
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [NoopAnimationsModule, DestinationRemoteProcessGroup]
        }).compileComponents();

        const fixture = TestBed.createComponent(DestinationRemoteProcessGroup);
        const component = fixture.componentInstance;

        // Set input properties if provided
        if (options.remoteProcessGroup) {
            component.remoteProcessGroup = options.remoteProcessGroup;
        }

        // Set up mock callbacks
        component.onChange = jest.fn();
        component.onTouched = jest.fn();

        fixture.detectChanges();

        return { component, fixture };
    }

    beforeEach(() => {
        jest.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should initialize with default values', async () => {
            const { component } = await setup();
            expect(component.isDisabled).toBe(false);
            expect(component.isTouched).toBe(false);
            expect(component.noPorts).toBe(false);
        });
    });

    describe('Remote process group setter logic', () => {
        it('should set group name from remote process group', async () => {
            const remoteProcessGroup = createMockRemoteProcessGroup('My Remote Process Group');
            const { component } = await setup({ remoteProcessGroup });

            expect(component.groupName).toBe('My Remote Process Group');
        });

        it('should process input ports correctly', async () => {
            const inputPorts = [
                createMockInputPort('port-1', 'Input Port 1', 'Description 1', true),
                createMockInputPort('port-2', 'Input Port 2', 'Description 2', true)
            ];
            const remoteProcessGroup = createMockRemoteProcessGroup('Test Group', inputPorts);
            const { component } = await setup({ remoteProcessGroup });

            expect(component.inputPortItems).toEqual([
                { value: 'port-1', text: 'Input Port 1', description: 'Description 1', disabled: false },
                { value: 'port-2', text: 'Input Port 2', description: 'Description 2', disabled: false }
            ]);
        });

        it('should handle disabled input ports when exists is false', async () => {
            const inputPorts = [
                createMockInputPort('port-1', 'Input Port 1', 'Description 1', true),
                createMockInputPort('port-2', 'Input Port 2', 'Description 2', false)
            ];
            const remoteProcessGroup = createMockRemoteProcessGroup('Test Group', inputPorts);
            const { component } = await setup({ remoteProcessGroup });

            expect(component.inputPortItems[0].disabled).toBe(false);
            expect(component.inputPortItems[1].disabled).toBe(true);
        });

        it('should set noPorts to true when input ports array is empty', async () => {
            const remoteProcessGroup = createMockRemoteProcessGroup('Test Group', []);
            const { component } = await setup({ remoteProcessGroup });

            expect(component.noPorts).toBe(true);
            expect(component.inputPortItems).toEqual([]);
        });

        it('should handle missing input ports array', async () => {
            const remoteProcessGroup = {
                component: {
                    name: 'Test Group',
                    contents: {} // Missing inputPorts
                }
            };
            const { component } = await setup({ remoteProcessGroup });

            expect(component.groupName).toBe('Test Group');
            expect(component.inputPortItems).toBeUndefined();
        });

        it('should handle null remote process group', async () => {
            const { component } = await setup({ remoteProcessGroup: null });

            expect(component.groupName).toBeUndefined();
            expect(component.inputPortItems).toBeUndefined();
        });
    });

    describe('ControlValueAccessor implementation', () => {
        it('should register onChange callback', async () => {
            const { component } = await setup();
            const callback = jest.fn();
            component.registerOnChange(callback);
            expect(component.onChange).toBe(callback);
        });

        it('should register onTouched callback', async () => {
            const { component } = await setup();
            const callback = jest.fn();
            component.registerOnTouched(callback);
            expect(component.onTouched).toBe(callback);
        });

        it('should handle disabled state', async () => {
            const { component } = await setup();
            component.setDisabledState(true);
            expect(component.isDisabled).toBe(true);
        });

        it('should handle writeValue', async () => {
            const { component } = await setup();
            component.writeValue('test-port-id');
            expect(component.selectedInputPort).toBe('test-port-id');
        });

        it('should handle value changes and call callbacks', async () => {
            const { component } = await setup();
            const mockOnChange = jest.fn();
            const mockOnTouched = jest.fn();

            component.registerOnChange(mockOnChange);
            component.registerOnTouched(mockOnTouched);
            component.selectedInputPort = 'new-port-id';

            component.handleChanged();

            expect(component.isTouched).toBe(true);
            expect(mockOnTouched).toHaveBeenCalled();
            expect(mockOnChange).toHaveBeenCalledWith('new-port-id');
        });

        it('should not call onTouched multiple times', async () => {
            const { component } = await setup();
            const mockOnChange = jest.fn();
            const mockOnTouched = jest.fn();

            component.registerOnChange(mockOnChange);
            component.registerOnTouched(mockOnTouched);
            component.isTouched = true;

            component.handleChanged();

            expect(mockOnTouched).not.toHaveBeenCalled();
            expect(mockOnChange).toHaveBeenCalled();
        });
    });

    describe('Template logic', () => {
        it('should display error section when no ports available', async () => {
            const remoteProcessGroup = createMockRemoteProcessGroup('Test Group', []);
            const { fixture } = await setup({ remoteProcessGroup });

            const errorSection = fixture.nativeElement.querySelector('[data-qa="error-section"]');
            expect(errorSection).toBeTruthy();

            const errorLabel = fixture.nativeElement.querySelector('[data-qa="to-input-error-label"]');
            expect(errorLabel).toBeTruthy();
            expect(errorLabel.textContent.trim()).toBe('To Input');

            const noPortsError = fixture.nativeElement.querySelector('[data-qa="no-ports-error"]');
            expect(noPortsError).toBeTruthy();
            expect(noPortsError.textContent.trim()).toBe('Test Group does not have any input ports.');
        });

        it('should display select dropdown when ports are available', async () => {
            const inputPorts = [
                createMockInputPort('port-1', 'Input Port 1'),
                createMockInputPort('port-2', 'Input Port 2')
            ];
            const remoteProcessGroup = createMockRemoteProcessGroup('Test Group', inputPorts);
            const { fixture } = await setup({ remoteProcessGroup });

            const toInputLabel = fixture.nativeElement.querySelector('[data-qa="to-input-label"]');
            expect(toInputLabel).toBeTruthy();
            expect(toInputLabel.textContent.trim()).toBe('To Input');

            const inputPortSelect = fixture.nativeElement.querySelector('[data-qa="input-port-select"]');
            expect(inputPortSelect).toBeTruthy();

            // Should not show error section
            const errorSection = fixture.nativeElement.querySelector('[data-qa="error-section"]');
            expect(errorSection).toBeFalsy();
        });

        it('should display "Within Group" label', async () => {
            const { fixture } = await setup();

            const withinGroupLabel = fixture.nativeElement.querySelector('[data-qa="within-group-label"]');
            expect(withinGroupLabel).toBeTruthy();
            expect(withinGroupLabel.textContent.trim()).toBe('Within Group');
        });

        it('should display group name when available', async () => {
            const remoteProcessGroup = createMockRemoteProcessGroup('My Test Remote Group');
            const { fixture } = await setup({ remoteProcessGroup });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay).toBeTruthy();
            expect(groupNameDisplay.textContent.trim()).toBe('My Test Remote Group');
            expect(groupNameDisplay.getAttribute('title')).toBe('My Test Remote Group');
        });

        it('should handle empty group name display', async () => {
            const remoteProcessGroup = createMockRemoteProcessGroup('');
            const { fixture } = await setup({ remoteProcessGroup });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay).toBeTruthy();
            expect(groupNameDisplay.textContent.trim()).toBe('');
            expect(groupNameDisplay.getAttribute('title')).toBe('');
        });

        it('should apply overflow styles to group name display', async () => {
            const remoteProcessGroup = createMockRemoteProcessGroup('Test Group');
            const { fixture } = await setup({ remoteProcessGroup });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.classList).toContain('overflow-ellipsis');
            expect(groupNameDisplay.classList).toContain('overflow-hidden');
            expect(groupNameDisplay.classList).toContain('whitespace-nowrap');
        });

        it('should toggle between error and select display based on ports availability', async () => {
            const { component, fixture } = await setup();

            // Start with no ports
            component.remoteProcessGroup = createMockRemoteProcessGroup('Test Group', []);
            fixture.detectChanges();

            expect(fixture.nativeElement.querySelector('[data-qa="error-section"]')).toBeTruthy();
            expect(fixture.nativeElement.querySelector('[data-qa="input-port-select"]')).toBeFalsy();

            // Add ports
            component.remoteProcessGroup = createMockRemoteProcessGroup('Test Group', [
                createMockInputPort('port-1', 'Port 1')
            ]);
            fixture.detectChanges();

            expect(fixture.nativeElement.querySelector('[data-qa="error-section"]')).toBeFalsy();
            expect(fixture.nativeElement.querySelector('[data-qa="input-port-select"]')).toBeTruthy();
        });
    });
});
