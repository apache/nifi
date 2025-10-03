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
import { DestinationProcessGroup } from './destination-process-group.component';

describe('DestinationProcessGroup Component', () => {
    // Mock data factories
    function createMockProcessGroup(
        id: string = 'process-group-id',
        canRead: boolean = true,
        name: string = 'Test Process Group'
    ) {
        return {
            id,
            permissions: { canRead, canWrite: true },
            component: { name }
        };
    }

    function createMockInputPort(
        id: string = 'input-port-id',
        name: string = 'Test Input Port',
        canRead: boolean = true,
        canWrite: boolean = true,
        allowRemoteAccess: boolean = false,
        comments: string = ''
    ) {
        return {
            id,
            permissions: { canRead, canWrite },
            allowRemoteAccess,
            component: { name, comments }
        };
    }

    // Setup function for component configuration
    async function setup(
        options: {
            processGroup?: any;
            inputPorts?: any[];
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [DestinationProcessGroup, NoopAnimationsModule]
        }).compileComponents();

        const fixture = TestBed.createComponent(DestinationProcessGroup);
        const component = fixture.componentInstance;

        // Set input properties
        if (options.processGroup !== undefined) {
            component.processGroup = options.processGroup;
        }
        if (options.inputPorts !== undefined) {
            component.inputPorts = options.inputPorts;
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
            expect(component.hasUnauthorizedPorts).toBe(false);
        });
    });

    describe('ProcessGroup input logic', () => {
        it('should set groupName from name when canRead is true', async () => {
            const processGroup = createMockProcessGroup('pg-1', true, 'My Process Group');
            const { component } = await setup({ processGroup });

            expect(component.groupName).toBe('My Process Group');
        });

        it('should set groupName from id when canRead is false', async () => {
            const processGroup = createMockProcessGroup('pg-1', false, 'My Process Group');
            const { component } = await setup({ processGroup });

            expect(component.groupName).toBe('pg-1');
        });

        it('should handle null processGroup', async () => {
            const { component } = await setup({ processGroup: null });
            expect(component.groupName).toBeUndefined();
        });
    });

    describe('InputPorts processing logic', () => {
        it('should set noPorts flag when input ports array is empty', async () => {
            const { component } = await setup({ inputPorts: [] });

            expect(component.noPorts).toBe(true);
            expect(component.hasUnauthorizedPorts).toBe(false);
            expect(component.inputPortItems).toEqual([]);
        });

        it('should process authorized input ports correctly', async () => {
            const inputPorts = [
                createMockInputPort('port-1', 'Input Port 1', true, true, false, 'Description 1'),
                createMockInputPort('port-2', 'Input Port 2', true, true, false, 'Description 2')
            ];
            const { component } = await setup({ inputPorts });

            expect(component.noPorts).toBe(false);
            expect(component.hasUnauthorizedPorts).toBe(false);
            expect(component.inputPortItems).toEqual([
                { value: 'port-1', text: 'Input Port 1', description: 'Description 1' },
                { value: 'port-2', text: 'Input Port 2', description: 'Description 2' }
            ]);
        });

        it('should filter unauthorized ports and set hasUnauthorizedPorts flag', async () => {
            const inputPorts = [
                createMockInputPort('port-1', 'Input Port 1', true, true),
                createMockInputPort('port-2', 'Input Port 2', false, true), // no read permission
                createMockInputPort('port-3', 'Input Port 3', true, false) // no write permission
            ];
            const { component } = await setup({ inputPorts });

            expect(component.noPorts).toBe(false);
            expect(component.hasUnauthorizedPorts).toBe(true);
            expect(component.inputPortItems).toEqual([{ value: 'port-1', text: 'Input Port 1', description: '' }]);
        });

        it('should filter out remote access ports', async () => {
            const inputPorts = [
                createMockInputPort('port-1', 'Input Port 1', true, true, false),
                createMockInputPort('port-2', 'Input Port 2', true, true, true) // allowRemoteAccess = true
            ];
            const { component } = await setup({ inputPorts });

            expect(component.noPorts).toBe(false);
            expect(component.hasUnauthorizedPorts).toBe(false);
            expect(component.inputPortItems).toEqual([{ value: 'port-1', text: 'Input Port 1', description: '' }]);
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

        it('should handle changes and call callbacks', async () => {
            const { component } = await setup();
            const onChangeSpy = jest.fn();
            const onTouchedSpy = jest.fn();

            component.registerOnChange(onChangeSpy);
            component.registerOnTouched(onTouchedSpy);
            component.selectedInputPort = 'new-port-id';

            component.handleChanged();

            expect(component.isTouched).toBe(true);
            expect(onTouchedSpy).toHaveBeenCalled();
            expect(onChangeSpy).toHaveBeenCalledWith('new-port-id');
        });

        it('should not call onTouched when already touched', async () => {
            const { component } = await setup();
            const onTouchedSpy = jest.fn();

            component.registerOnTouched(onTouchedSpy);
            component.isTouched = true;

            component.handleChanged();

            expect(onTouchedSpy).not.toHaveBeenCalled();
        });
    });

    describe('Template logic', () => {
        it('should display error section when no ports', async () => {
            const processGroup = createMockProcessGroup('pg-1', true, 'Test Group');
            const { fixture } = await setup({ processGroup, inputPorts: [] });

            const errorSection = fixture.nativeElement.querySelector('[data-qa="error-section"]');
            const noPortsError = fixture.nativeElement.querySelector('[data-qa="no-ports-error"]');
            const inputPortSection = fixture.nativeElement.querySelector('[data-qa="input-port-section"]');

            expect(errorSection).toBeTruthy();
            expect(noPortsError).toBeTruthy();
            expect(noPortsError.textContent.trim()).toBe('Test Group does not have any local input ports.');
            expect(inputPortSection).toBeFalsy();
        });

        it('should display error section when unauthorized ports', async () => {
            const processGroup = createMockProcessGroup('pg-1', true, 'Test Group');
            const inputPorts = [createMockInputPort('port-1', 'Input Port 1', false, true)];
            const { fixture } = await setup({ processGroup, inputPorts });

            const errorSection = fixture.nativeElement.querySelector('[data-qa="error-section"]');
            const unauthorizedError = fixture.nativeElement.querySelector('[data-qa="unauthorized-ports-error"]');
            const inputPortSection = fixture.nativeElement.querySelector('[data-qa="input-port-section"]');

            expect(errorSection).toBeTruthy();
            expect(unauthorizedError).toBeTruthy();
            expect(unauthorizedError.textContent.trim()).toBe('Not authorized for any local input ports in Test Group');
            expect(inputPortSection).toBeFalsy();
        });

        it('should display input port section when ports are available', async () => {
            const processGroup = createMockProcessGroup('pg-1', true, 'Test Group');
            const inputPorts = [createMockInputPort('port-1', 'Input Port 1')];
            const { fixture } = await setup({ processGroup, inputPorts });

            const inputPortSection = fixture.nativeElement.querySelector('[data-qa="input-port-section"]');
            const toInputLabel = fixture.nativeElement.querySelector('[data-qa="to-input-label"]');
            const inputPortSelect = fixture.nativeElement.querySelector('[data-qa="input-port-select"]');
            const errorSection = fixture.nativeElement.querySelector('[data-qa="error-section"]');

            expect(inputPortSection).toBeTruthy();
            expect(toInputLabel).toBeTruthy();
            expect(toInputLabel.textContent.trim()).toBe('To Input');
            expect(inputPortSelect).toBeTruthy();
            expect(errorSection).toBeFalsy();
        });

        it('should display "Within Group" label', async () => {
            const { fixture } = await setup();

            const withinGroupLabel = fixture.nativeElement.querySelector('[data-qa="within-group-label"]');
            expect(withinGroupLabel).toBeTruthy();
            expect(withinGroupLabel.textContent.trim()).toBe('Within Group');
        });

        it('should display groupName in template', async () => {
            const processGroup = createMockProcessGroup('pg-1', true, 'My Test Group');
            const { fixture } = await setup({ processGroup });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay).toBeTruthy();
            expect(groupNameDisplay.textContent.trim()).toBe('My Test Group');
        });

        it('should set title attribute for groupName element', async () => {
            const processGroup = createMockProcessGroup('pg-1', true, 'Group with Title');
            const { fixture } = await setup({ processGroup });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.getAttribute('title')).toBe('Group with Title');
        });
    });
});
