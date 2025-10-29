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
import { SourceRemoteProcessGroup } from './source-remote-process-group.component';

describe('SourceRemoteProcessGroup', () => {
    // Mock data factories
    function createMockOutputPort(id: string, hasDescription = false, exists = true): any {
        const port: any = {
            id,
            name: id,
            exists
        };

        if (hasDescription) {
            port.comments = `Description for ${id}`;
        }

        return port;
    }

    function createMockRemoteProcessGroup(name: string, outputPorts: any[] = []): any {
        return {
            component: {
                name,
                contents: {
                    outputPorts
                }
            }
        };
    }

    // Setup function for component configuration
    async function setup(
        options: {
            remoteProcessGroup?: any;
            selectedOutputPort?: string | null;
        } = {}
    ) {
        await TestBed.configureTestingModule({
            imports: [NoopAnimationsModule, SourceRemoteProcessGroup]
        }).compileComponents();

        const fixture = TestBed.createComponent(SourceRemoteProcessGroup);
        const component = fixture.componentInstance;

        // Set up component state
        if (options.remoteProcessGroup !== undefined) {
            component.remoteProcessGroup = options.remoteProcessGroup;
        }
        if (options.selectedOutputPort !== undefined && options.selectedOutputPort !== null) {
            component.writeValue(options.selectedOutputPort);
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
            expect(component.selectedOutputPort).toBeUndefined();
        });
    });

    describe('RemoteProcessGroup input logic', () => {
        it('should set group name and create output port items when remote process group provided', async () => {
            const outputPorts = [createMockOutputPort('port1'), createMockOutputPort('port2')];
            const rpg = createMockRemoteProcessGroup('TestGroup', outputPorts);

            const { component } = await setup({ remoteProcessGroup: rpg });

            expect(component.groupName).toBe('TestGroup');
            expect(component.outputPortItems).toHaveLength(2);
            expect(component.outputPortItems[0].text).toBe('port1');
            expect(component.outputPortItems[1].text).toBe('port2');
        });

        it('should handle remote process group with no ports', async () => {
            const rpg = createMockRemoteProcessGroup('EmptyGroup', []);
            const { component } = await setup({ remoteProcessGroup: rpg });

            expect(component.groupName).toBe('EmptyGroup');
            expect(component.noPorts).toBe(true);
            expect(component.outputPortItems).toHaveLength(0);
        });

        it('should handle null remote process group', async () => {
            const { component } = await setup({ remoteProcessGroup: null });

            expect(component.groupName).toBeUndefined();
            expect(component.outputPortItems).toBeUndefined();
            expect(component.noPorts).toBe(false);
        });
    });

    describe('Output port processing logic', () => {
        it('should create output port items with descriptions', async () => {
            const outputPorts = [createMockOutputPort('port1', true), createMockOutputPort('port2', false)];
            const rpg = createMockRemoteProcessGroup('TestGroup', outputPorts);

            const { component } = await setup({ remoteProcessGroup: rpg });

            expect(component.outputPortItems[0].description).toBe('Description for port1');
            expect(component.outputPortItems[1].description).toBeUndefined();
        });

        it('should mark non-existent ports as disabled', async () => {
            const outputPorts = [
                createMockOutputPort('port1', false, true),
                createMockOutputPort('port2', false, false)
            ];
            const rpg = createMockRemoteProcessGroup('TestGroup', outputPorts);

            const { component } = await setup({ remoteProcessGroup: rpg });

            expect(component.outputPortItems[0].disabled).toBe(false);
            expect(component.outputPortItems[1].disabled).toBe(true);
        });

        it('should set noPorts flag to false when ports are available', async () => {
            const rpgWithPorts = createMockRemoteProcessGroup('WithPorts', [createMockOutputPort('port1')]);
            const { component } = await setup({ remoteProcessGroup: rpgWithPorts });
            expect(component.noPorts).toBe(false);
        });

        it('should set noPorts flag to true when no ports are available', async () => {
            const rpgWithoutPorts = createMockRemoteProcessGroup('WithoutPorts', []);
            const { component } = await setup({ remoteProcessGroup: rpgWithoutPorts });
            expect(component.noPorts).toBe(true);
        });
    });

    describe('handleChanged method logic', () => {
        it('should call callbacks when selection changes', async () => {
            const { component } = await setup();
            const onChangeSpy = jest.fn();
            const onTouchedSpy = jest.fn();

            component.registerOnChange(onChangeSpy);
            component.registerOnTouched(onTouchedSpy);
            component.selectedOutputPort = 'port1';

            component.handleChanged();

            expect(onChangeSpy).toHaveBeenCalledWith('port1');
            expect(onTouchedSpy).toHaveBeenCalled();
            expect(component.isTouched).toBe(true);
        });

        it('should handle null callbacks gracefully', async () => {
            const { component } = await setup();

            // Clear callbacks to test null handling
            component.onChange = undefined as any;
            component.onTouched = undefined as any;
            component.selectedOutputPort = 'port1';

            // This should not throw errors when callbacks are null
            component.handleChanged();
            expect(component.selectedOutputPort).toBe('port1');
        });

        it('should not call onTouched callback when already touched', async () => {
            const { component } = await setup();
            const onTouchedSpy = jest.fn();

            component.registerOnTouched(onTouchedSpy);
            component.isTouched = true;

            component.handleChanged();

            expect(onTouchedSpy).not.toHaveBeenCalled();
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

            component.setDisabledState(false);
            expect(component.isDisabled).toBe(false);
        });

        it('should handle writeValue', async () => {
            const { component } = await setup();
            component.writeValue('port1');
            expect(component.selectedOutputPort).toBe('port1');
        });

        it('should handle writeValue with null', async () => {
            const { component } = await setup();
            component.writeValue(null as any);
            expect(component.selectedOutputPort).toBeNull();
        });
    });

    describe('Template logic', () => {
        it('should display main container', async () => {
            const { fixture } = await setup();
            const container = fixture.nativeElement.querySelector('[data-qa="source-remote-process-group-container"]');
            expect(container).toBeTruthy();
        });

        it('should display no ports error when no ports available', async () => {
            const rpg = createMockRemoteProcessGroup('TestGroup', []);
            const { fixture } = await setup({ remoteProcessGroup: rpg });

            const noPortsSection = fixture.nativeElement.querySelector('[data-qa="no-ports-section"]');
            const noPortsError = fixture.nativeElement.querySelector('[data-qa="no-ports-error"]');

            expect(noPortsSection).toBeTruthy();
            expect(noPortsError).toBeTruthy();
            expect(noPortsError.textContent).toContain('TestGroup does not have any local output ports');
        });

        it('should display output port selection when ports are available', async () => {
            const rpg = createMockRemoteProcessGroup('TestGroup', [createMockOutputPort('port1')]);
            const { fixture } = await setup({ remoteProcessGroup: rpg });

            const outputPortSection = fixture.nativeElement.querySelector('[data-qa="output-port-section"]');
            const outputPortSelect = fixture.nativeElement.querySelector('[data-qa="output-port-select"]');

            expect(outputPortSection).toBeTruthy();
            expect(outputPortSelect).toBeTruthy();
        });

        it('should display group name correctly', async () => {
            const rpg = createMockRemoteProcessGroup('TestGroup', []);
            const { fixture } = await setup({ remoteProcessGroup: rpg });

            const groupNameDisplay = fixture.nativeElement.querySelector('[data-qa="group-name-display"]');

            expect(groupNameDisplay).toBeTruthy();
            expect(groupNameDisplay.textContent.trim()).toBe('TestGroup');
        });

        it('should hide output port selection when no ports available', async () => {
            const rpg = createMockRemoteProcessGroup('TestGroup', []);
            const { fixture } = await setup({ remoteProcessGroup: rpg });

            const outputPortSection = fixture.nativeElement.querySelector('[data-qa="output-port-section"]');

            expect(outputPortSection).toBeFalsy();
        });

        it('should hide no ports error when ports are available', async () => {
            const rpg = createMockRemoteProcessGroup('TestGroup', [createMockOutputPort('port1')]);
            const { fixture } = await setup({ remoteProcessGroup: rpg });

            const noPortsSection = fixture.nativeElement.querySelector('[data-qa="no-ports-section"]');

            expect(noPortsSection).toBeFalsy();
        });
    });
});
