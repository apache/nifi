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
import { provideMockActions } from '@ngrx/effects/testing';
import { Action } from '@ngrx/store';
import { provideMockStore } from '@ngrx/store/testing';
import { Router } from '@angular/router';
import { MatDialog } from '@angular/material/dialog';
import { firstValueFrom, Observable, Subject, of, throwError } from 'rxjs';
import { ConnectorControllerServicesEffects } from './connector-controller-services.effects';
import {
    loadConnectorControllerServices,
    loadConnectorControllerServicesFailure,
    loadConnectorControllerServicesSuccess,
    openViewControllerServiceDialog,
    selectConnectorControllerService
} from './connector-controller-services.actions';
import { selectConnectorParameterContext } from '../connector-canvas/connector-canvas.selectors';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ErrorContextKey } from '../../../../state/error';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ControllerServiceEntity, ParameterContextEntity } from '../../../../state/shared';
import { createParameterContextFixture } from '../../testing/parameter-context-fixture';
import { EditControllerService } from '../../../../ui/common/controller-service/edit-controller-service/edit-controller-service.component';

function buildService(overrides: Partial<ControllerServiceEntity> = {}): ControllerServiceEntity {
    return {
        id: 'svc-1',
        permissions: { canRead: true, canWrite: true },
        component: { id: 'svc-1', name: 'My Service', state: 'DISABLED' },
        ...overrides
    } as unknown as ControllerServiceEntity;
}

describe('ConnectorControllerServicesEffects', () => {
    interface SetupOptions {
        parameterContext?: ParameterContextEntity | null;
    }

    async function setup(options: SetupOptions = {}) {
        let actions$: Observable<Action>;

        const mockConnectorService = {
            getConnectorControllerServices: vi.fn(),
            getConnectorFlow: vi.fn()
        };

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue('Error message')
        };

        const mockRouter = {
            navigate: vi.fn().mockResolvedValue(true)
        };

        const afterClosed$ = new Subject<any>();
        const mockDialogInstance = {
            createNewService: undefined as any,
            goToParameter: undefined as any,
            convertToParameter: undefined as any,
            goToService: undefined as any,
            parameterContext: undefined as any,
            supportsParameters: true as any
        };
        const mockDialog = {
            open: vi.fn().mockReturnValue({
                componentInstance: mockDialogInstance,
                afterClosed: () => afterClosed$.asObservable()
            })
        };

        await TestBed.configureTestingModule({
            providers: [
                ConnectorControllerServicesEffects,
                provideMockActions(() => actions$),
                provideMockStore({
                    selectors: [
                        {
                            selector: selectConnectorParameterContext,
                            value: options.parameterContext ?? null
                        }
                    ]
                }),
                { provide: ConnectorService, useValue: mockConnectorService },
                { provide: ErrorHelper, useValue: mockErrorHelper },
                { provide: Router, useValue: mockRouter },
                { provide: MatDialog, useValue: mockDialog }
            ]
        }).compileComponents();

        return {
            effects: TestBed.inject(ConnectorControllerServicesEffects),
            mockConnectorService,
            mockRouter,
            mockDialog,
            mockDialogInstance,
            afterClosed$,
            actions$: (stream: Observable<Action>) => {
                actions$ = stream;
            }
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('loadConnectorControllerServices$', () => {
        it('should combine controller services and flow responses into a load success', async () => {
            const { effects, actions$, mockConnectorService } = await setup();
            const breadcrumb = { id: 'pg-1' } as any;
            const service = buildService();
            mockConnectorService.getConnectorControllerServices.mockReturnValue(
                of({ controllerServices: [service], currentTime: '2023-04-01T00:00:00Z' })
            );
            mockConnectorService.getConnectorFlow.mockReturnValue(of({ processGroupFlow: { breadcrumb } }));
            actions$(
                of(
                    loadConnectorControllerServices({
                        request: { connectorId: 'conn-1', processGroupId: 'pg-1' }
                    })
                )
            );

            const result = await firstValueFrom(effects.loadConnectorControllerServices$);

            expect(result).toEqual(
                loadConnectorControllerServicesSuccess({
                    response: {
                        connectorId: 'conn-1',
                        processGroupId: 'pg-1',
                        breadcrumb,
                        controllerServices: [service],
                        loadedTimestamp: '2023-04-01T00:00:00Z'
                    }
                })
            );
        });

        it('should emit a load failure with the controller-services error context when the API call fails', async () => {
            const { effects, actions$, mockConnectorService } = await setup();
            mockConnectorService.getConnectorControllerServices.mockReturnValue(throwError(() => new Error('boom')));
            mockConnectorService.getConnectorFlow.mockReturnValue(of({ processGroupFlow: { breadcrumb: null } }));
            actions$(
                of(
                    loadConnectorControllerServices({
                        request: { connectorId: 'conn-1', processGroupId: 'pg-1' }
                    })
                )
            );

            const result = await firstValueFrom(effects.loadConnectorControllerServices$);

            expect(result).toEqual(
                loadConnectorControllerServicesFailure({
                    errorContext: {
                        errors: ['Error message'],
                        context: ErrorContextKey.CONTROLLER_SERVICES
                    }
                })
            );
        });
    });

    describe('loadConnectorControllerServicesFailure$', () => {
        it('should map a load failure into a banner error', async () => {
            const { effects, actions$ } = await setup();
            const errorContext = { errors: ['boom'], context: ErrorContextKey.CONTROLLER_SERVICES };
            actions$(of(loadConnectorControllerServicesFailure({ errorContext })));

            const result = await firstValueFrom(effects.loadConnectorControllerServicesFailure$);

            expect(result).toEqual(ErrorActions.addBannerError({ errorContext }));
        });
    });

    describe('selectConnectorControllerService$', () => {
        it('should navigate to the controller service deep link with replaceUrl', async () => {
            const { effects, actions$, mockRouter } = await setup();
            actions$(
                of(
                    selectConnectorControllerService({
                        request: { connectorId: 'conn-1', processGroupId: 'pg-1', serviceId: 'svc-1' }
                    })
                )
            );

            await firstValueFrom(effects.selectConnectorControllerService$);

            expect(mockRouter.navigate).toHaveBeenCalledWith(
                ['/connectors', 'conn-1', 'canvas', 'pg-1', 'controller-services', 'svc-1'],
                { replaceUrl: true }
            );
        });
    });

    describe('openViewControllerServiceDialog$', () => {
        it('should open the EditControllerService dialog with the readonly flag set and the entity passed through unchanged', async () => {
            const { effects, actions$, mockDialog } = await setup();
            const service = buildService();
            actions$(of(openViewControllerServiceDialog({ controllerService: service })));

            await firstValueFrom(effects.openViewControllerServiceDialog$);

            expect(mockDialog.open).toHaveBeenCalledTimes(1);
            const [component, config] = mockDialog.open.mock.calls[0];
            expect(component).toBe(EditControllerService);
            expect(config.id).toBe('svc-1');
            expect(config.data.id).toBe('svc-1');
            expect(config.data.readonly).toBe(true);
            expect(config.data.controllerService).toBe(service);
            expect(config.data.controllerService.permissions.canWrite).toBe(true);
        });

        it('should wire the bound parameter context onto the EditControllerService dialog instance', async () => {
            const parameterContext = createParameterContextFixture({
                component: { id: 'pc-1', name: 'Bound PC', parameters: [] }
            });
            const { effects, actions$, mockDialogInstance } = await setup({ parameterContext });

            actions$(of(openViewControllerServiceDialog({ controllerService: buildService() })));

            await firstValueFrom(effects.openViewControllerServiceDialog$);

            expect(mockDialogInstance.parameterContext).toBe(parameterContext);
            expect(mockDialogInstance.supportsParameters).toBe(true);
            // Read-only mode should not wire a navigation callback. The property table hides
            // "Go to Parameter" when this is undefined, while still rendering parameter values.
            expect(mockDialogInstance.goToParameter).toBeUndefined();
        });

        it('should set supportsParameters to false when no parameter context is bound', async () => {
            const { effects, actions$, mockDialogInstance } = await setup({ parameterContext: null });

            actions$(of(openViewControllerServiceDialog({ controllerService: buildService() })));

            await firstValueFrom(effects.openViewControllerServiceDialog$);

            expect(mockDialogInstance.parameterContext).toBeUndefined();
            expect(mockDialogInstance.supportsParameters).toBe(false);
            expect(mockDialogInstance.goToParameter).toBeUndefined();
        });
    });
});
