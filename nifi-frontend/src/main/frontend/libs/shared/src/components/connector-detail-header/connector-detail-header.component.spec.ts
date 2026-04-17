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
import { ConnectorDetailHeader } from './connector-detail-header.component';
import { ConnectorEntity, ConnectorState, ConnectorStatus } from '../../types';

interface SetupOptions {
    connector?: ConnectorEntity;
    condensed?: boolean;
}

function createMockConnector(overrides: Partial<ConnectorEntity> = {}): ConnectorEntity {
    return {
        id: 'test-connector-id',
        uri: 'http://localhost:4200/nifi-api/connectors/test-connector-id',
        permissions: {
            canRead: true,
            canWrite: true
        },
        status: {
            runStatus: 'RUNNING',
            validationStatus: 'VALID'
        } as ConnectorStatus,
        component: {
            id: 'test-connector-id',
            name: 'Test Connector',
            type: 'org.apache.nifi.connector.TestConnector',
            state: ConnectorState.RUNNING,
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-test-connector',
                version: '1.0.0'
            },
            managedProcessGroupId: 'test-pg-id',
            availableActions: [
                { name: 'START', description: 'Start action', allowed: true },
                { name: 'STOP', description: 'Stop action', allowed: true }
            ]
        },
        bulletins: [],
        revision: {
            version: 1
        },
        ...overrides
    };
}

async function setup(options: SetupOptions = {}) {
    const connector = options.connector ?? createMockConnector();

    await TestBed.configureTestingModule({
        imports: [ConnectorDetailHeader]
    }).compileComponents();

    const fixture: ComponentFixture<ConnectorDetailHeader> = TestBed.createComponent(ConnectorDetailHeader);
    const component = fixture.componentInstance;

    fixture.componentRef.setInput('connector', connector);
    if (options.condensed !== undefined) {
        fixture.componentRef.setInput('condensed', options.condensed);
    }
    fixture.detectChanges();

    return { fixture, component, connector };
}

describe('ConnectorDetailHeader', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    it('should display the connector name', async () => {
        const { fixture } = await setup();
        const nameEl = fixture.nativeElement.querySelector('[data-qa="connector-name"]');
        expect(nameEl.textContent.trim()).toBe('Test Connector');
    });

    it('should display the simple connector type', async () => {
        const { fixture } = await setup();
        const typeEl = fixture.nativeElement.querySelector('[data-qa="connector-type"]');
        expect(typeEl.textContent.trim()).toBe('TestConnector');
    });

    it('should display the connector state badge with the state value', async () => {
        const { fixture } = await setup();
        const stateBadge = fixture.nativeElement.querySelector('[data-qa="connector-state"]');
        expect(stateBadge).toBeTruthy();
        expect(stateBadge.textContent.trim()).toBe('RUNNING');
    });

    describe('getStateVariant', () => {
        it('should return success for RUNNING', async () => {
            const { component } = await setup();
            expect(component.getStateVariant(ConnectorState.RUNNING)).toBe('success');
        });

        it('should return neutral for STOPPED', async () => {
            const { component } = await setup();
            expect(component.getStateVariant(ConnectorState.STOPPED)).toBe('neutral');
        });

        it('should return neutral for DISABLED', async () => {
            const { component } = await setup();
            expect(component.getStateVariant(ConnectorState.DISABLED)).toBe('neutral');
        });

        it('should return info for STARTING', async () => {
            const { component } = await setup();
            expect(component.getStateVariant(ConnectorState.STARTING)).toBe('info');
        });

        it('should return info for UPDATING', async () => {
            const { component } = await setup();
            expect(component.getStateVariant(ConnectorState.UPDATING)).toBe('info');
        });

        it('should return info for STOPPING', async () => {
            const { component } = await setup();
            expect(component.getStateVariant(ConnectorState.STOPPING)).toBe('info');
        });

        it('should return info for DRAINING', async () => {
            const { component } = await setup();
            expect(component.getStateVariant(ConnectorState.DRAINING)).toBe('info');
        });

        it('should return info for PREPARING_FOR_UPDATE', async () => {
            const { component } = await setup();
            expect(component.getStateVariant(ConnectorState.PREPARING_FOR_UPDATE)).toBe('info');
        });

        it('should return critical for UPDATE_FAILED', async () => {
            const { component } = await setup();
            expect(component.getStateVariant(ConnectorState.UPDATE_FAILED)).toBe('critical');
        });

        it('should return neutral for unknown state', async () => {
            const { component } = await setup();
            expect(component.getStateVariant('UNKNOWN')).toBe('neutral');
        });
    });

    describe('getStateColorClass', () => {
        it('should map success variant to success-color-default', async () => {
            const { component } = await setup();
            expect(component.getStateColorClass(ConnectorState.RUNNING)).toBe('success-color-default');
        });

        it('should map neutral variant to neutral-color', async () => {
            const { component } = await setup();
            expect(component.getStateColorClass(ConnectorState.STOPPED)).toBe('neutral-color');
        });

        it('should map info variant to primary-color', async () => {
            const { component } = await setup();
            expect(component.getStateColorClass(ConnectorState.STARTING)).toBe('primary-color');
        });

        it('should map critical variant to error-color', async () => {
            const { component } = await setup();
            expect(component.getStateColorClass(ConnectorState.UPDATE_FAILED)).toBe('error-color');
        });
    });

    describe('hasUnappliedEdits', () => {
        it('should return false when no DISCARD_WORKING_CONFIGURATION action exists', async () => {
            const { component } = await setup();
            expect(component.hasUnappliedEdits()).toBe(false);
        });

        it('should return true when DISCARD_WORKING_CONFIGURATION action is allowed', async () => {
            const connector = createMockConnector({
                component: {
                    ...createMockConnector().component,
                    availableActions: [{ name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: true }]
                }
            });
            const { component } = await setup({ connector });
            expect(component.hasUnappliedEdits()).toBe(true);
        });

        it('should return false when DISCARD_WORKING_CONFIGURATION action is not allowed', async () => {
            const connector = createMockConnector({
                component: {
                    ...createMockConnector().component,
                    availableActions: [
                        { name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: false }
                    ]
                }
            });
            const { component } = await setup({ connector });
            expect(component.hasUnappliedEdits()).toBe(false);
        });
    });

    it('should show unapplied edits badge when edits are not applied', async () => {
        const connector = createMockConnector({
            component: {
                ...createMockConnector().component,
                availableActions: [{ name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: true }]
            }
        });
        const { fixture } = await setup({ connector });
        const badge = fixture.nativeElement.querySelector('[data-qa="edits-not-applied-badge"]');
        expect(badge).toBeTruthy();
    });

    it('should not show unapplied edits badge when no unapplied edits', async () => {
        const { fixture } = await setup();
        const badge = fixture.nativeElement.querySelector('[data-qa="edits-not-applied-badge"]');
        expect(badge).toBeNull();
    });

    describe('simpleType', () => {
        it('should extract simple class name from fully qualified type', async () => {
            const { component } = await setup();
            expect(component.simpleType()).toBe('TestConnector');
        });

        it('should handle type without package prefix', async () => {
            const connector = createMockConnector({
                component: {
                    ...createMockConnector().component,
                    type: 'SimpleType'
                }
            });
            const { component } = await setup({ connector });
            expect(component.simpleType()).toBe('SimpleType');
        });

        it('should handle empty type', async () => {
            const connector = createMockConnector({
                component: {
                    ...createMockConnector().component,
                    type: ''
                }
            });
            const { component } = await setup({ connector });
            expect(component.simpleType()).toBe('');
        });
    });

    describe('condensed mode', () => {
        it('should render h3 in default mode', async () => {
            const { fixture } = await setup();
            const h3 = fixture.nativeElement.querySelector('h3[data-qa="connector-name"]');
            expect(h3).toBeTruthy();
        });

        it('should render span instead of h3 in condensed mode', async () => {
            const { fixture } = await setup({ condensed: true });
            const h3 = fixture.nativeElement.querySelector('h3[data-qa="connector-name"]');
            const span = fixture.nativeElement.querySelector('span[data-qa="connector-name"]');
            expect(h3).toBeNull();
            expect(span).toBeTruthy();
        });

        it('should apply text-sm to type in condensed mode', async () => {
            const { fixture } = await setup({ condensed: true });
            const typeEl = fixture.nativeElement.querySelector('[data-qa="connector-type"]');
            expect(typeEl.classList.contains('text-sm')).toBe(true);
        });

        it('should render badges on a separate line in condensed mode', async () => {
            const { fixture } = await setup({ condensed: true });
            const badgesRow = fixture.nativeElement.querySelector('[data-qa="connector-badges"]');
            expect(badgesRow).toBeTruthy();

            const stateBadge = badgesRow.querySelector('[data-qa="connector-state"]');
            expect(stateBadge).toBeTruthy();
        });

        it('should not have a separate badges row in default mode', async () => {
            const { fixture } = await setup();
            const badgesRow = fixture.nativeElement.querySelector('[data-qa="connector-badges"]');
            expect(badgesRow).toBeNull();
        });

        it('should show edits-not-applied badge in badges row in condensed mode', async () => {
            const connector = createMockConnector({
                component: {
                    ...createMockConnector().component,
                    availableActions: [{ name: 'DISCARD_WORKING_CONFIGURATION', description: 'Discard', allowed: true }]
                }
            });
            const { fixture } = await setup({ connector, condensed: true });

            const badgesRow = fixture.nativeElement.querySelector('[data-qa="connector-badges"]');
            const editsBadge = badgesRow.querySelector('[data-qa="edits-not-applied-badge"]');
            expect(editsBadge).toBeTruthy();
        });
    });
});
