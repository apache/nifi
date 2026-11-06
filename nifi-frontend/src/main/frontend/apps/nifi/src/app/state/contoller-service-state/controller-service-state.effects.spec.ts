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
import { ReplaySubject } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { provideMockActions } from '@ngrx/effects/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { Action } from '@ngrx/store';
import { ControllerServiceReferencingComponentEntity, Permissions } from '@nifi/shared';

import { ControllerServiceStateEffects } from './controller-service-state.effects';
import { ControllerServiceStateService } from '../../service/controller-service-state.service';
import { ErrorHelper } from '../../service/error-helper.service';
import { ControllerServiceEntity } from '@nifi/shared';
import { SetEnableRequest, SetEnableStep } from './index';

describe('ControllerServiceStateEffects', () => {
    const authorizedPermissions: Permissions = { canRead: true, canWrite: true };
    const unauthorizedPermissions: Permissions = { canRead: true, canWrite: false };

    function createService(
        id: string,
        state: string,
        children: ControllerServiceReferencingComponentEntity[] = [],
        options: {
            referenceCycle?: boolean;
            permissions?: Permissions;
            omitReferencingComponents?: boolean;
        } = {}
    ): ControllerServiceReferencingComponentEntity {
        const component: ControllerServiceReferencingComponentEntity['component'] = {
            groupId: 'group-1',
            id,
            name: id,
            type: 'org.apache.nifi.TestService',
            state,
            properties: {},
            descriptors: {},
            validationErrors: [],
            referenceType: 'ControllerService',
            referenceCycle: options.referenceCycle ?? false,
            ...(options.omitReferencingComponents ? {} : { referencingComponents: children })
        };

        return {
            id,
            revision: { version: 1 },
            permissions: options.permissions ?? authorizedPermissions,
            bulletins: [],
            operatePermissions: authorizedPermissions,
            component
        };
    }

    function createSchedulable(
        id: string,
        referenceType: 'Processor' | 'ReportingTask',
        state: string,
        activeThreadCount?: number
    ): ControllerServiceReferencingComponentEntity {
        return {
            id,
            revision: { version: 1 },
            permissions: authorizedPermissions,
            bulletins: [],
            operatePermissions: authorizedPermissions,
            component: {
                groupId: 'group-1',
                id,
                name: id,
                type: `org.apache.nifi.${referenceType}`,
                state,
                properties: {},
                descriptors: {},
                validationErrors: [],
                referenceType,
                referenceCycle: false,
                referencingComponents: [],
                ...(activeThreadCount === undefined ? {} : { activeThreadCount })
            }
        };
    }

    function createControllerServiceEntity(
        referencingComponents: ControllerServiceReferencingComponentEntity[],
        runStatus = 'ENABLED'
    ): ControllerServiceEntity {
        return {
            id: 'root-service',
            uri: '/controller-services/root-service',
            permissions: authorizedPermissions,
            revision: { version: 1 },
            bulletins: [],
            status: { runStatus, validationStatus: 'VALID' },
            component: {
                id: 'root-service',
                name: 'Root Service',
                type: 'org.apache.nifi.RootService',
                state: runStatus,
                referencingComponents
            }
        } as unknown as ControllerServiceEntity;
    }

    function createSetEnableRequest(
        currentStep: SetEnableStep,
        enable: boolean,
        scope: SetEnableRequest['scope'] = 'SERVICE_AND_REFERENCING_COMPONENTS'
    ): SetEnableRequest {
        return {
            enable,
            currentStep,
            scope
        };
    }

    async function setup() {
        const actions$ = new ReplaySubject<Action>(1);

        await TestBed.configureTestingModule({
            providers: [
                ControllerServiceStateEffects,
                provideMockActions(() => actions$),
                provideMockStore(),
                {
                    provide: ControllerServiceStateService,
                    useValue: {
                        setEnable: vi.fn(),
                        getControllerService: vi.fn(),
                        updateReferencingServices: vi.fn(),
                        updateReferencingSchedulableComponents: vi.fn()
                    }
                },
                {
                    provide: ErrorHelper,
                    useValue: {
                        getErrorString: vi.fn()
                    }
                },
                {
                    provide: MatDialog,
                    useValue: {
                        open: vi.fn()
                    }
                }
            ]
        }).compileComponents();

        const effects = TestBed.inject(ControllerServiceStateEffects);

        return { effects, actions$ };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('isReferencingServicesActionComplete / getNextStep enable', () => {
        it('stays at EnableReferencingServices when one sibling remains DISABLED', async () => {
            const { effects } = await setup();
            const controllerService = createControllerServiceEntity([
                createService('s1', 'ENABLED'),
                createService('s2', 'DISABLED')
            ]);

            const nextStep = effects.getNextStep(
                createSetEnableRequest(SetEnableStep.EnableReferencingServices, true),
                controllerService
            );

            expect(nextStep).toBe(SetEnableStep.EnableReferencingServices);
        });

        it('advances to StartReferencingComponents when every sibling is ENABLING or ENABLED', async () => {
            const { effects } = await setup();
            const controllerService = createControllerServiceEntity([
                createService('s1', 'ENABLING'),
                createService('s2', 'ENABLED')
            ]);

            const nextStep = effects.getNextStep(
                createSetEnableRequest(SetEnableStep.EnableReferencingServices, true),
                controllerService
            );

            expect(nextStep).toBe(SetEnableStep.StartReferencingComponents);
        });

        it('stays at EnableReferencingServices when a nested service remains DISABLED', async () => {
            const { effects } = await setup();
            const controllerService = createControllerServiceEntity([
                createService('s1', 'ENABLED', [createService('s1a', 'DISABLED')]),
                createService('s2', 'ENABLED')
            ]);

            const nextStep = effects.getNextStep(
                createSetEnableRequest(SetEnableStep.EnableReferencingServices, true),
                controllerService
            );

            expect(nextStep).toBe(SetEnableStep.EnableReferencingServices);
        });
    });

    describe('isReferencingServicesActionComplete / getNextStep disable', () => {
        it('stays at DisableReferencingServices when one sibling remains DISABLING', async () => {
            const { effects } = await setup();
            const controllerService = createControllerServiceEntity([
                createService('s1', 'DISABLED'),
                createService('s2', 'DISABLING')
            ]);

            const nextStep = effects.getNextStep(
                createSetEnableRequest(SetEnableStep.DisableReferencingServices, false),
                controllerService
            );

            expect(nextStep).toBe(SetEnableStep.DisableReferencingServices);
        });

        it('advances to DisableService when every sibling and nested service is DISABLED', async () => {
            const { effects } = await setup();
            const controllerService = createControllerServiceEntity([
                createService('s1', 'DISABLED', [createService('s1a', 'DISABLED')]),
                createService('s2', 'DISABLED')
            ]);

            const nextStep = effects.getNextStep(
                createSetEnableRequest(SetEnableStep.DisableReferencingServices, false),
                controllerService
            );

            expect(nextStep).toBe(SetEnableStep.DisableService);
        });
    });

    describe('areReferencingComponentsStopped / getNextStep stop', () => {
        it('stays at StopReferencingComponents when one nested branch still has a RUNNING processor', async () => {
            const { effects } = await setup();
            const controllerService = createControllerServiceEntity([
                createService('s1', 'ENABLED', [createSchedulable('p1', 'Processor', 'STOPPED')]),
                createService('s2', 'ENABLED', [createSchedulable('p2', 'Processor', 'RUNNING')])
            ]);

            const nextStep = effects.getNextStep(
                createSetEnableRequest(SetEnableStep.StopReferencingComponents, false),
                controllerService
            );

            expect(nextStep).toBe(SetEnableStep.StopReferencingComponents);
        });

        it('stays at StopReferencingComponents when a nested reporting task has active threads', async () => {
            const { effects } = await setup();
            const controllerService = createControllerServiceEntity([
                createService('s1', 'ENABLED', [createSchedulable('rt1', 'ReportingTask', 'STOPPED', 2)])
            ]);

            const nextStep = effects.getNextStep(
                createSetEnableRequest(SetEnableStep.StopReferencingComponents, false),
                controllerService
            );

            expect(nextStep).toBe(SetEnableStep.StopReferencingComponents);
        });

        it('advances to DisableReferencingServices when every nested branch is stopped with zero threads', async () => {
            const { effects } = await setup();
            const controllerService = createControllerServiceEntity([
                createService('s1', 'ENABLED', [createSchedulable('p1', 'Processor', 'STOPPED', 0)]),
                createService('s2', 'ENABLED', [createSchedulable('rt1', 'ReportingTask', 'STOPPED')])
            ]);

            const nextStep = effects.getNextStep(
                createSetEnableRequest(SetEnableStep.StopReferencingComponents, false),
                controllerService
            );

            expect(nextStep).toBe(SetEnableStep.DisableReferencingServices);
        });

        it('stays at StopReferencingComponents when a direct processor is RUNNING', async () => {
            const { effects } = await setup();
            const controllerService = createControllerServiceEntity([
                createSchedulable('p1', 'Processor', 'RUNNING'),
                createService('s1', 'ENABLED', [createSchedulable('p2', 'Processor', 'STOPPED')])
            ]);

            const nextStep = effects.getNextStep(
                createSetEnableRequest(SetEnableStep.StopReferencingComponents, false),
                controllerService
            );

            expect(nextStep).toBe(SetEnableStep.StopReferencingComponents);
        });
    });

    describe('empty references and reference cycles', () => {
        it('treats empty referencing components as complete for enable and stop checks', async () => {
            const { effects } = await setup();

            expect(effects.isReferencingServicesActionComplete([], ['ENABLED', 'ENABLING'])).toBe(true);
            expect(effects.isReferencingServicesActionComplete(undefined, ['DISABLED'])).toBe(true);
            expect(effects.areReferencingComponentsStopped([])).toBe(true);
            expect(effects.areReferencingComponentsStopped(undefined)).toBe(true);
            expect(effects.hasUnauthorizedReferences([])).toBe(false);
            expect(effects.hasUnauthorizedReferences(undefined)).toBe(false);
        });

        it('treats a leaf service with an empty referencingComponents list as complete', async () => {
            const { effects } = await setup();
            const leaf = [createService('s1', 'ENABLED', [])];

            expect(effects.isReferencingServicesActionComplete(leaf, ['ENABLED', 'ENABLING'])).toBe(true);
            expect(effects.areReferencingComponentsStopped(leaf)).toBe(true);
        });

        it('does not throw on cycle nodes and treats an accepted cycle sibling as complete', async () => {
            const { effects } = await setup();
            const referencingComponents = [
                createService('s1', 'ENABLED'),
                createService('s-cycle', 'ENABLED', [], {
                    referenceCycle: true,
                    omitReferencingComponents: true
                })
            ];

            expect(() =>
                effects.isReferencingServicesActionComplete(referencingComponents, ['ENABLED', 'ENABLING'])
            ).not.toThrow();
            expect(effects.isReferencingServicesActionComplete(referencingComponents, ['ENABLED', 'ENABLING'])).toBe(
                true
            );

            expect(() => effects.areReferencingComponentsStopped(referencingComponents)).not.toThrow();
            expect(effects.areReferencingComponentsStopped(referencingComponents)).toBe(true);

            expect(() => effects.hasUnauthorizedReferences(referencingComponents)).not.toThrow();
            expect(effects.hasUnauthorizedReferences(referencingComponents)).toBe(false);
        });

        it('returns false when a cycle node is outside the accepted state', async () => {
            const { effects } = await setup();
            const referencingComponents = [
                createService('s1', 'ENABLED'),
                createService('s-cycle', 'DISABLED', [], {
                    referenceCycle: true,
                    omitReferencingComponents: true
                })
            ];

            expect(effects.isReferencingServicesActionComplete(referencingComponents, ['ENABLED', 'ENABLING'])).toBe(
                false
            );
        });

        it('keeps waiting when a service is expanded under one sibling and flagged under another', async () => {
            const { effects } = await setup();
            const expanded = createService('shared', 'ENABLED', [createService('child', 'DISABLED')]);
            const flagged = createService('shared', 'ENABLED', [], {
                referenceCycle: true,
                omitReferencingComponents: true
            });

            const nextStep = effects.getNextStep(
                createSetEnableRequest(SetEnableStep.EnableReferencingServices, true),
                createControllerServiceEntity([
                    createService('s1', 'ENABLED', [expanded]),
                    createService('s2', 'ENABLED', [flagged])
                ])
            );

            expect(nextStep).toBe(SetEnableStep.EnableReferencingServices);
        });

        it('detects unauthorized permissions without recursing into cycle nodes', async () => {
            const { effects } = await setup();
            const referencingComponents = [
                createService('s1', 'ENABLED', [], { permissions: unauthorizedPermissions }),
                createService('s-cycle', 'ENABLED', [], {
                    referenceCycle: true,
                    omitReferencingComponents: true
                })
            ];

            expect(() => effects.hasUnauthorizedReferences(referencingComponents)).not.toThrow();
            expect(effects.hasUnauthorizedReferences(referencingComponents)).toBe(true);
        });

        it('detects unauthorized permissions in a nested descendant', async () => {
            const { effects } = await setup();
            const referencingComponents = [
                createService('s1', 'ENABLED', [
                    createService('s1a', 'ENABLED', [], { permissions: unauthorizedPermissions })
                ])
            ];

            expect(effects.hasUnauthorizedReferences(referencingComponents)).toBe(true);
        });
    });
});
