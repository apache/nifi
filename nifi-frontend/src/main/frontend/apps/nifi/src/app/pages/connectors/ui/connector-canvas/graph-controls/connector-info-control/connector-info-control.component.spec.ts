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
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { ConnectorInfoControl } from './connector-info-control.component';
import { ConnectorEntity, ConnectorStatus, Storage } from '@nifi/shared';
import * as ConnectorCanvasEntityActions from '../../../../state/connector-canvas-entity/connector-canvas-entity.actions';
import {
    navigateToViewConnectorDetails,
    navigateToConfigureConnector
} from '../../../../state/connectors-listing/connectors-listing.actions';

function createMockConnectorEntity(overrides: Partial<ConnectorEntity> = {}): ConnectorEntity {
    return {
        id: 'connector-1',
        uri: '/api/connectors/connector-1',
        revision: { version: 0 },
        permissions: { canRead: true, canWrite: true },
        operatePermissions: { canRead: true, canWrite: true },
        bulletins: [],
        status: {} as ConnectorStatus,
        component: {
            id: 'connector-1',
            name: 'Test Connector',
            type: 'org.apache.nifi.TestConnector',
            state: 'RUNNING',
            bundle: { group: 'org.apache.nifi', artifact: 'nifi-test-nar', version: '1.0.0' },
            availableActions: [
                { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: true },
                { name: 'CANCEL_DRAIN_FLOWFILES', description: 'Cancel drain', allowed: false },
                { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false },
                { name: 'CONFIGURE', description: 'Configure', allowed: true },
                { name: 'START', description: 'Start', allowed: false, reasonNotAllowed: 'Already running' },
                { name: 'STOP', description: 'Stop', allowed: true }
            ],
            managedProcessGroupId: 'pg-1'
        },
        ...overrides
    };
}

interface SetupOptions {
    connectorEntity?: ConnectorEntity | null;
    entitySaving?: boolean;
    storedVisibility?: { [key: string]: boolean } | null;
}

async function setup(options: SetupOptions = {}) {
    const mockStorage = {
        getItem: vi.fn().mockReturnValue(options.storedVisibility ?? null),
        setItem: vi.fn(),
        removeItem: vi.fn(),
        hasItem: vi.fn().mockReturnValue(false),
        getItemExpiration: vi.fn().mockReturnValue(null)
    };

    await TestBed.configureTestingModule({
        imports: [ConnectorInfoControl, NoopAnimationsModule],
        providers: [provideMockStore(), { provide: Storage, useValue: mockStorage }]
    }).compileComponents();

    const store = TestBed.inject(MockStore);
    const dispatchSpy = vi.spyOn(store, 'dispatch');
    const fixture: ComponentFixture<ConnectorInfoControl> = TestBed.createComponent(ConnectorInfoControl);
    const component = fixture.componentInstance;

    fixture.componentRef.setInput('connectorEntity', options.connectorEntity ?? null);
    fixture.componentRef.setInput('entitySaving', options.entitySaving ?? false);

    fixture.detectChanges();

    return { fixture, component, store, dispatchSpy, mockStorage };
}

describe('ConnectorInfoControl', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should default to expanded state', async () => {
            const { component } = await setup();
            expect(component.connectorCollapsed).toBe(false);
        });

        it('should restore collapsed state from storage', async () => {
            const { component } = await setup({
                storedVisibility: { 'connector-info-control': false }
            });
            expect(component.connectorCollapsed).toBe(true);
        });

        it('should remain expanded when stored state indicates visible', async () => {
            const { component } = await setup({
                storedVisibility: { 'connector-info-control': true }
            });
            expect(component.connectorCollapsed).toBe(false);
        });
    });

    describe('Connector info display', () => {
        it('should render the connector-detail-header when entity is provided', async () => {
            const entity = createMockConnectorEntity();
            const { fixture } = await setup({ connectorEntity: entity });

            const header = fixture.nativeElement.querySelector('connector-detail-header');
            expect(header).toBeTruthy();
        });

        it('should display connector name via connector-detail-header', async () => {
            const entity = createMockConnectorEntity();
            const { fixture } = await setup({ connectorEntity: entity });

            const name = fixture.nativeElement.querySelector('[data-qa="connector-name"]');
            expect(name).toBeTruthy();
            expect(name.textContent.trim()).toBe('Test Connector');
        });

        it('should display connector state badge via connector-detail-header', async () => {
            const entity = createMockConnectorEntity();
            const { fixture } = await setup({ connectorEntity: entity });

            const badge = fixture.nativeElement.querySelector('[data-qa="connector-state"]');
            expect(badge).toBeTruthy();
        });

        it('should display simple connector type via connector-detail-header', async () => {
            const entity = createMockConnectorEntity();
            const { fixture } = await setup({ connectorEntity: entity });

            const type = fixture.nativeElement.querySelector('[data-qa="connector-type"]');
            expect(type).toBeTruthy();
            expect(type.textContent.trim()).toBe('TestConnector');
        });

        it('should not render content when connectorEntity is null', async () => {
            const { fixture } = await setup({ connectorEntity: null });

            const header = fixture.nativeElement.querySelector('connector-detail-header');
            expect(header).toBeNull();
        });
    });

    describe('View details action', () => {
        it('should show view details button when canRead is true', async () => {
            const entity = createMockConnectorEntity();
            const { fixture } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-view-details"]');
            expect(button).toBeTruthy();
            expect(button.getAttribute('aria-label')).toBe('Details');
        });

        it('should not show view details button when canRead is false', async () => {
            const entity = createMockConnectorEntity({
                permissions: { canRead: false, canWrite: false }
            });
            const { fixture } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-view-details"]');
            expect(button).toBeNull();
        });

        it('should dispatch navigateToViewConnectorDetails when clicked', async () => {
            const entity = createMockConnectorEntity();
            const { fixture, dispatchSpy } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-view-details"]');
            button.click();

            expect(dispatchSpy).toHaveBeenCalledWith(navigateToViewConnectorDetails({ id: entity.id }));
        });
    });

    describe('Configure action', () => {
        it('should show configure button when canRead and canModify', async () => {
            const entity = createMockConnectorEntity();
            const { fixture } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-configure"]');
            expect(button).toBeTruthy();
            expect(button.getAttribute('aria-label')).toBe('Configure');
        });

        it('should not show configure button when canModify is false', async () => {
            const entity = createMockConnectorEntity({
                permissions: { canRead: true, canWrite: false }
            });
            const { fixture } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-configure"]');
            expect(button).toBeNull();
        });

        it('should disable configure button when CONFIGURE action is not allowed', async () => {
            const entity = createMockConnectorEntity({
                component: {
                    ...createMockConnectorEntity().component,
                    availableActions: [
                        { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: true },
                        { name: 'CANCEL_DRAIN_FLOWFILES', description: 'Cancel', allowed: false },
                        { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false },
                        {
                            name: 'CONFIGURE',
                            description: 'Configure',
                            allowed: false,
                            reasonNotAllowed: 'Connector is running'
                        }
                    ]
                }
            });
            const { fixture } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-configure"]');
            expect(button).toBeTruthy();
            expect(button.disabled).toBe(true);
        });

        it('should dispatch navigateToConfigureConnector when clicked', async () => {
            const entity = createMockConnectorEntity();
            const { fixture, dispatchSpy } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-configure"]');
            button.click();

            expect(dispatchSpy).toHaveBeenCalledWith(
                navigateToConfigureConnector({ request: { id: entity.id, connector: entity } })
            );
        });
    });

    describe('Drain action', () => {
        it('should show drain button when canDrain is true', async () => {
            const entity = createMockConnectorEntity();
            const { fixture } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-drain"]');
            expect(button).toBeTruthy();
            expect(button.textContent).toContain('Drain');
        });

        it('should not show drain button when DRAIN_FLOWFILES is not allowed', async () => {
            const entity = createMockConnectorEntity({
                component: {
                    ...createMockConnectorEntity().component,
                    availableActions: [
                        { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: false },
                        { name: 'CANCEL_DRAIN_FLOWFILES', description: 'Cancel', allowed: false },
                        { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false },
                        { name: 'CONFIGURE', description: 'Configure', allowed: true }
                    ]
                }
            });
            const { fixture } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-drain"]');
            expect(button).toBeNull();
        });

        it('should render but disable drain button when entity is saving', async () => {
            const entity = createMockConnectorEntity();
            const { fixture } = await setup({ connectorEntity: entity, entitySaving: true });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-drain"]');
            expect(button).toBeTruthy();
            expect(button.disabled).toBe(true);
        });

        it('should dispatch promptDrainConnector when drain button is clicked', async () => {
            const entity = createMockConnectorEntity();
            const { fixture, dispatchSpy } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-drain"]');
            button.click();

            expect(dispatchSpy).toHaveBeenCalledWith(
                ConnectorCanvasEntityActions.promptDrainConnector({ connector: entity })
            );
        });

        it('showDrain should remain true while entity is saving so the button stays mounted', async () => {
            const entity = createMockConnectorEntity();
            const { component } = await setup({ connectorEntity: entity, entitySaving: true });

            expect(component.showDrain()).toBe(true);
        });

        it('canDrain should be false while entity is saving', async () => {
            const entity = createMockConnectorEntity();
            const { component } = await setup({ connectorEntity: entity, entitySaving: true });

            expect(component.canDrain()).toBe(false);
        });
    });

    describe('Cancel drain action', () => {
        it('should show cancel drain button when canCancelDrain is true', async () => {
            const entity = createMockConnectorEntity({
                component: {
                    ...createMockConnectorEntity().component,
                    availableActions: [
                        { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: false },
                        { name: 'CANCEL_DRAIN_FLOWFILES', description: 'Cancel drain', allowed: true },
                        { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false },
                        { name: 'CONFIGURE', description: 'Configure', allowed: true }
                    ]
                }
            });
            const { fixture } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-cancel-drain"]');
            expect(button).toBeTruthy();
            expect(button.textContent).toContain('Cancel Drain');
        });

        it('should not show cancel drain button when CANCEL_DRAIN_FLOWFILES is not allowed', async () => {
            const entity = createMockConnectorEntity();
            const { fixture } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-cancel-drain"]');
            expect(button).toBeNull();
        });

        it('should render but disable cancel drain button when entity is saving', async () => {
            const entity = createMockConnectorEntity({
                component: {
                    ...createMockConnectorEntity().component,
                    availableActions: [
                        { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: false },
                        { name: 'CANCEL_DRAIN_FLOWFILES', description: 'Cancel drain', allowed: true },
                        { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false },
                        { name: 'CONFIGURE', description: 'Configure', allowed: true },
                        { name: 'START', description: 'Start', allowed: false, reasonNotAllowed: 'Already running' },
                        { name: 'STOP', description: 'Stop', allowed: true }
                    ]
                }
            });
            const { fixture } = await setup({ connectorEntity: entity, entitySaving: true });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-cancel-drain"]');
            expect(button).toBeTruthy();
            expect(button.disabled).toBe(true);
        });

        it('should dispatch cancelConnectorDrain when cancel drain button is clicked', async () => {
            const entity = createMockConnectorEntity({
                component: {
                    ...createMockConnectorEntity().component,
                    availableActions: [
                        { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: false },
                        { name: 'CANCEL_DRAIN_FLOWFILES', description: 'Cancel drain', allowed: true },
                        { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false },
                        { name: 'CONFIGURE', description: 'Configure', allowed: true }
                    ]
                }
            });
            const { fixture, dispatchSpy } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-cancel-drain"]');
            button.click();

            expect(dispatchSpy).toHaveBeenCalledWith(
                ConnectorCanvasEntityActions.cancelConnectorDrain({ connector: entity })
            );
        });

        it('showCancelDrain should remain true while entity is saving so the button stays mounted', async () => {
            const entity = createMockConnectorEntity({
                component: {
                    ...createMockConnectorEntity().component,
                    availableActions: [
                        { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: false },
                        { name: 'CANCEL_DRAIN_FLOWFILES', description: 'Cancel drain', allowed: true },
                        { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false },
                        { name: 'CONFIGURE', description: 'Configure', allowed: true }
                    ]
                }
            });
            const { component } = await setup({ connectorEntity: entity, entitySaving: true });

            expect(component.showCancelDrain()).toBe(true);
        });

        it('canCancelDrain should be false while entity is saving', async () => {
            const entity = createMockConnectorEntity({
                component: {
                    ...createMockConnectorEntity().component,
                    availableActions: [
                        { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: false },
                        { name: 'CANCEL_DRAIN_FLOWFILES', description: 'Cancel drain', allowed: true },
                        { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false },
                        { name: 'CONFIGURE', description: 'Configure', allowed: true }
                    ]
                }
            });
            const { component } = await setup({ connectorEntity: entity, entitySaving: true });

            expect(component.canCancelDrain()).toBe(false);
        });
    });

    describe('No operate permissions', () => {
        it('should not show drain button when user lacks operate permissions', async () => {
            const entity = createMockConnectorEntity({
                permissions: { canRead: true, canWrite: false },
                operatePermissions: { canRead: true, canWrite: false }
            });
            const { fixture } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-drain"]');
            expect(button).toBeNull();
        });

        it('should not show start/stop buttons when user lacks operate permissions', async () => {
            const entity = createMockConnectorEntity({
                permissions: { canRead: true, canWrite: false },
                operatePermissions: { canRead: true, canWrite: false }
            });
            const { fixture } = await setup({ connectorEntity: entity });

            expect(fixture.nativeElement.querySelector('[data-qa="connector-panel-start"]')).toBeNull();
            expect(fixture.nativeElement.querySelector('[data-qa="connector-panel-stop"]')).toBeNull();
        });
    });

    describe('Start action', () => {
        function runningEntity(): ConnectorEntity {
            return createMockConnectorEntity();
        }

        function stoppedEntity(): ConnectorEntity {
            return createMockConnectorEntity({
                component: {
                    ...createMockConnectorEntity().component,
                    state: 'STOPPED',
                    availableActions: [
                        { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: false },
                        { name: 'CANCEL_DRAIN_FLOWFILES', description: 'Cancel', allowed: false },
                        { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false },
                        { name: 'CONFIGURE', description: 'Configure', allowed: true },
                        { name: 'START', description: 'Start', allowed: true },
                        { name: 'STOP', description: 'Stop', allowed: false, reasonNotAllowed: 'Not running' }
                    ]
                }
            });
        }

        it('should render start button when operate permissions are granted', async () => {
            const { fixture } = await setup({ connectorEntity: runningEntity() });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-start"]');
            expect(button).toBeTruthy();
            expect(button.getAttribute('aria-label')).toBe('Start');
        });

        it('should enable start button when START is allowed', async () => {
            const { fixture } = await setup({ connectorEntity: stoppedEntity() });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-start"]');
            expect(button.disabled).toBe(false);
        });

        it('should disable start button when START is not allowed', async () => {
            const { fixture } = await setup({ connectorEntity: runningEntity() });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-start"]');
            expect(button.disabled).toBe(true);
        });

        it('should disable start button when entity is saving', async () => {
            const { fixture } = await setup({ connectorEntity: stoppedEntity(), entitySaving: true });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-start"]');
            expect(button.disabled).toBe(true);
        });

        it('should dispatch startConnector when start button is clicked', async () => {
            const entity = stoppedEntity();
            const { fixture, dispatchSpy } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-start"]');
            button.click();

            expect(dispatchSpy).toHaveBeenCalledWith(
                ConnectorCanvasEntityActions.startConnector({ connector: entity })
            );
        });
    });

    describe('Stop action', () => {
        function runningEntity(): ConnectorEntity {
            return createMockConnectorEntity();
        }

        function stoppedEntity(): ConnectorEntity {
            return createMockConnectorEntity({
                component: {
                    ...createMockConnectorEntity().component,
                    state: 'STOPPED',
                    availableActions: [
                        { name: 'DRAIN_FLOWFILES', description: 'Drain', allowed: false },
                        { name: 'CANCEL_DRAIN_FLOWFILES', description: 'Cancel', allowed: false },
                        { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false },
                        { name: 'CONFIGURE', description: 'Configure', allowed: true },
                        { name: 'START', description: 'Start', allowed: true },
                        { name: 'STOP', description: 'Stop', allowed: false, reasonNotAllowed: 'Not running' }
                    ]
                }
            });
        }

        it('should render stop button when operate permissions are granted', async () => {
            const { fixture } = await setup({ connectorEntity: runningEntity() });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-stop"]');
            expect(button).toBeTruthy();
            expect(button.getAttribute('aria-label')).toBe('Stop');
        });

        it('should enable stop button when STOP is allowed', async () => {
            const { fixture } = await setup({ connectorEntity: runningEntity() });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-stop"]');
            expect(button.disabled).toBe(false);
        });

        it('should disable stop button when STOP is not allowed', async () => {
            const { fixture } = await setup({ connectorEntity: stoppedEntity() });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-stop"]');
            expect(button.disabled).toBe(true);
        });

        it('should disable stop button when entity is saving', async () => {
            const { fixture } = await setup({ connectorEntity: runningEntity(), entitySaving: true });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-stop"]');
            expect(button.disabled).toBe(true);
        });

        it('should dispatch stopConnector when stop button is clicked', async () => {
            const entity = runningEntity();
            const { fixture, dispatchSpy } = await setup({ connectorEntity: entity });

            const button = fixture.nativeElement.querySelector('[data-qa="connector-panel-stop"]');
            button.click();

            expect(dispatchSpy).toHaveBeenCalledWith(ConnectorCanvasEntityActions.stopConnector({ connector: entity }));
        });
    });

    describe('Collapse persistence', () => {
        it('should save collapsed state to Storage under the graph-control-visibility key', async () => {
            const { component, mockStorage } = await setup();

            component.toggleCollapsed(true);

            expect(mockStorage.setItem).toHaveBeenCalledWith('graph-control-visibility', {
                'connector-info-control': false
            });
        });

        it('should preserve sibling control keys when toggling visibility', async () => {
            const { component, mockStorage } = await setup({
                storedVisibility: { 'other-control': true, 'connector-info-control': true }
            });
            mockStorage.getItem.mockReturnValue({ 'other-control': true, 'connector-info-control': true });

            component.toggleCollapsed(true);

            expect(mockStorage.setItem).toHaveBeenCalledWith(
                'graph-control-visibility',
                expect.objectContaining({
                    'other-control': true,
                    'connector-info-control': false
                })
            );
        });
    });
});
