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

import { SourceProcessGroup } from './source-process-group.component';

describe('SourceProcessGroup', () => {
    // Mock data factories
    function createMockProcessGroup(
        options: {
            id?: string;
            name?: string;
            canRead?: boolean;
        } = {}
    ) {
        const { id = 'test-process-group-id', name = 'Test Process Group', canRead = true } = options;

        return {
            id,
            permissions: {
                canRead
            },
            component: {
                name
            }
        };
    }

    function createMockOutputPort(
        options: {
            id?: string;
            name?: string;
            comments?: string;
            canRead?: boolean;
            canWrite?: boolean;
            allowRemoteAccess?: boolean;
        } = {}
    ) {
        const {
            id = 'test-output-port-id',
            name = 'Test Output Port',
            comments = 'Test output port comments',
            canRead = true,
            canWrite = true,
            allowRemoteAccess = false
        } = options;

        return {
            id,
            permissions: {
                canRead,
                canWrite
            },
            component: {
                name,
                comments
            },
            allowRemoteAccess
        };
    }

    // Setup function for component configuration
    async function setup(
        options: {
            processGroup?: any;
            outputPorts?: any[];
            selectedOutputPort?: string;
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [NoopAnimationsModule, SourceProcessGroup]
        }).compileComponents();

        const fixture = TestBed.createComponent(SourceProcessGroup);
        const component = fixture.componentInstance;

        // Set up mock callbacks
        component.onChange = jest.fn();
        component.onTouched = jest.fn();

        // Set processGroup if provided
        if (options.processGroup !== undefined) {
            component.processGroup = options.processGroup;
        }

        // Set outputPorts if provided
        if (options.outputPorts !== undefined) {
            component.outputPorts = options.outputPorts;
        }

        // Set selectedOutputPort if provided
        if (options.selectedOutputPort !== undefined) {
            component.writeValue(options.selectedOutputPort);
        }

        // Initial detection to trigger lifecycle hooks
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

    describe('ProcessGroup setter logic', () => {
        it('should set groupName from process group name when canRead is true', async () => {
            const mockProcessGroup = createMockProcessGroup({
                name: 'Custom Process Group',
                canRead: true
            });
            const { component } = await setup({ processGroup: mockProcessGroup });

            expect(component.groupName).toBe('Custom Process Group');
        });

        it('should set groupName from process group ID when canRead is false', async () => {
            const mockProcessGroup = createMockProcessGroup({
                id: 'custom-process-group-id',
                name: 'Custom Process Group',
                canRead: false
            });
            const { component } = await setup({ processGroup: mockProcessGroup });

            expect(component.groupName).toBe('custom-process-group-id');
        });

        it('should handle null process group', async () => {
            const { component } = await setup({ processGroup: null });
            expect(component.groupName).toBeUndefined();
        });
    });

    describe('OutputPorts setter logic', () => {
        it('should process authorized output ports correctly', async () => {
            const mockOutputPorts = [
                createMockOutputPort({ id: 'port1', name: 'Port 1', canRead: true, canWrite: true }),
                createMockOutputPort({ id: 'port2', name: 'Port 2', canRead: true, canWrite: true })
            ];
            const { component } = await setup({ outputPorts: mockOutputPorts });

            expect(component.outputPortItems).toHaveLength(2);
            expect(component.outputPortItems[0]).toEqual({
                value: 'port1',
                text: 'Port 1',
                description: 'Test output port comments'
            });
            expect(component.noPorts).toBe(false);
            expect(component.hasUnauthorizedPorts).toBe(false);
        });

        it('should filter out unauthorized output ports', async () => {
            const mockOutputPorts = [
                createMockOutputPort({ id: 'port1', name: 'Port 1', canRead: true, canWrite: true }),
                createMockOutputPort({ id: 'port2', name: 'Port 2', canRead: false, canWrite: true })
            ];
            const { component } = await setup({ outputPorts: mockOutputPorts });

            expect(component.outputPortItems).toHaveLength(1);
            expect(component.outputPortItems[0].value).toBe('port1');
            expect(component.hasUnauthorizedPorts).toBe(true);
        });

        it('should filter out output ports with remote access', async () => {
            const mockOutputPorts = [
                createMockOutputPort({ id: 'port1', name: 'Port 1', allowRemoteAccess: false }),
                createMockOutputPort({ id: 'port2', name: 'Port 2', allowRemoteAccess: true })
            ];
            const { component } = await setup({ outputPorts: mockOutputPorts });

            expect(component.outputPortItems).toHaveLength(1);
            expect(component.outputPortItems[0].value).toBe('port1');
        });

        it('should set noPorts to true when no output ports provided', async () => {
            const { component } = await setup({ outputPorts: [] });

            expect(component.noPorts).toBe(true);
            expect(component.outputPortItems).toHaveLength(0);
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
            component.writeValue('test-output-port-id');
            expect(component.selectedOutputPort).toBe('test-output-port-id');
        });

        it('should emit changes when handleChanged is called', async () => {
            const { component } = await setup();
            const onChangeSpy = jest.fn();
            const onTouchedSpy = jest.fn();
            component.registerOnChange(onChangeSpy);
            component.registerOnTouched(onTouchedSpy);

            component.selectedOutputPort = 'test-port-id';
            component.handleChanged();

            expect(onChangeSpy).toHaveBeenCalledWith('test-port-id');
            expect(onTouchedSpy).toHaveBeenCalled();
            expect(component.isTouched).toBe(true);
        });

        it('should not call onTouched again if already touched', async () => {
            const { component } = await setup();
            const onChangeSpy = jest.fn();
            const onTouchedSpy = jest.fn();
            component.registerOnChange(onChangeSpy);
            component.registerOnTouched(onTouchedSpy);

            component.isTouched = true;
            component.selectedOutputPort = 'test-port-id';
            component.handleChanged();

            expect(onChangeSpy).toHaveBeenCalledWith('test-port-id');
            expect(onTouchedSpy).not.toHaveBeenCalled();
        });
    });

    describe('Template logic', () => {
        it('should show error section when no ports available', async () => {
            const mockProcessGroup = createMockProcessGroup({ name: 'Test Group' });
            const { fixture } = await setup({
                processGroup: mockProcessGroup,
                outputPorts: []
            });

            const errorSection = fixture.nativeElement.querySelector('[data-qa="error-section"]');
            expect(errorSection).toBeTruthy();

            const noPortsError = fixture.nativeElement.querySelector('[data-qa="no-ports-error"]');
            expect(noPortsError).toBeTruthy();
            expect(noPortsError.textContent.trim()).toBe('Test Group does not have any local output ports.');
        });

        it('should show unauthorized ports error when unauthorized ports exist', async () => {
            const mockProcessGroup = createMockProcessGroup({ name: 'Test Group' });
            const mockOutputPorts = [createMockOutputPort({ canRead: false, canWrite: true })];
            const { fixture } = await setup({
                processGroup: mockProcessGroup,
                outputPorts: mockOutputPorts
            });

            const errorSection = fixture.nativeElement.querySelector('[data-qa="error-section"]');
            expect(errorSection).toBeTruthy();

            const unauthorizedError = fixture.nativeElement.querySelector('[data-qa="unauthorized-ports-error"]');
            expect(unauthorizedError).toBeTruthy();
            expect(unauthorizedError.textContent.trim()).toBe(
                'Not authorized for any local output ports in Test Group'
            );
        });

        it('should show output port select when ports are available', async () => {
            const mockProcessGroup = createMockProcessGroup();
            const mockOutputPorts = [createMockOutputPort({ id: 'port1', name: 'Port 1' })];
            const { fixture } = await setup({
                processGroup: mockProcessGroup,
                outputPorts: mockOutputPorts
            });

            const outputPortSelect = fixture.nativeElement.querySelector('[data-qa="output-port-select"]');
            expect(outputPortSelect).toBeTruthy();

            const errorSection = fixture.nativeElement.querySelector('[data-qa="error-section"]');
            expect(errorSection).toBeFalsy();
        });

        it('should display group name correctly', async () => {
            const mockProcessGroup = createMockProcessGroup({ name: 'Test Group Name' });
            const { fixture } = await setup({ processGroup: mockProcessGroup });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay).toBeTruthy();
            expect(groupNameDisplay.textContent.trim()).toBe('Test Group Name');
            expect(groupNameDisplay.getAttribute('title')).toBe('Test Group Name');
        });

        it('should update display when processGroup changes', async () => {
            const { component, fixture } = await setup();

            component.processGroup = createMockProcessGroup({ name: 'New Group Name' });
            fixture.detectChanges();

            expect(component.groupName).toBe('New Group Name');

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');
            expect(groupNameDisplay.textContent.trim()).toBe('New Group Name');
            expect(groupNameDisplay.getAttribute('title')).toBe('New Group Name');
        });
    });
});
