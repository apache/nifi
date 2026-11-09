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

import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { provideRouter } from '@angular/router';
import { ComponentType, ControllerServiceEntity, selectRouteParams } from '@nifi/shared';

import { ConnectorControllerServicesComponent } from './connector-controller-services.component';
import {
    selectConnectorControllerServicesState,
    selectControllerServiceIdFromRoute
} from '../../state/connector-controller-services/connector-controller-services.selectors';
import {
    clearConnectorControllerServiceBulletins,
    loadConnectorControllerServices,
    navigateToAdvancedConnectorControllerServiceUi,
    navigateToConnectorControllerService,
    openChangeConnectorControllerServiceVersionDialog,
    openConfigureConnectorControllerServiceDialog,
    openDisableConnectorControllerServiceDialog,
    openEnableConnectorControllerServiceDialog,
    openNewConnectorControllerServiceDialog,
    openViewControllerServiceDialog,
    promptConnectorControllerServiceDeletion,
    resetConnectorControllerServicesState,
    selectConnectorControllerService,
    startConnectorControllerServicesEntityPolling,
    stopConnectorControllerServicesEntityPolling
} from '../../state/connector-controller-services/connector-controller-services.actions';
import { loadConnectorEntity } from '../../state/connector-canvas-entity/connector-canvas-entity.actions';
import {
    ConnectorControllerServicesState,
    initialConnectorControllerServicesState
} from '../../state/connector-controller-services';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';
import { navigateToComponentDocumentation } from '../../../../state/documentation/documentation.actions';
import { BreadcrumbEntity } from '../../../../state/shared';
import { selectConnectorCanvasEntity } from '../../state/connector-canvas-entity/connector-canvas-entity.selectors';
const CONNECTOR_ID = 'connector-1';
const PROCESS_GROUP_ID = 'pg-current';

function buildBreadcrumb(
    overrides: Partial<BreadcrumbEntity> = {},
    parentBreadcrumb: BreadcrumbEntity | undefined = undefined
): BreadcrumbEntity {
    return {
        id: PROCESS_GROUP_ID,
        permissions: { canRead: true, canWrite: true },
        breadcrumb: { id: PROCESS_GROUP_ID, name: 'Current Group' },
        parentBreadcrumb,
        ...overrides
    } as BreadcrumbEntity;
}

function buildControllerService(overrides: Partial<ControllerServiceEntity> = {}): ControllerServiceEntity {
    return {
        id: 'svc-1',
        permissions: { canRead: true, canWrite: false },
        operatePermissions: { canRead: true, canWrite: true },
        revision: { version: 1 },
        uri: `/controller-services/${overrides.id ?? 'svc-1'}`,
        bulletins: [],
        parentGroupId: PROCESS_GROUP_ID,
        status: { runStatus: 'DISABLED', validationStatus: 'VALID' },
        component: {
            id: 'svc-1',
            parentGroupId: PROCESS_GROUP_ID,
            name: 'My Service',
            type: 'org.apache.nifi.MyService',
            bundle: { group: 'org.apache.nifi', artifact: 'nifi-services-nar', version: '2.0.0' },
            state: 'DISABLED',
            persistsState: true
        } as any,
        ...overrides
    } as ControllerServiceEntity;
}

interface SetupOptions {
    state?: Partial<ConnectorControllerServicesState>;
    routeParams?: Record<string, string>;
    selectedServiceId?: string | null;
    inTroubleshooting?: boolean;
}

function buildState(overrides: Partial<ConnectorControllerServicesState> = {}): ConnectorControllerServicesState {
    return {
        ...initialConnectorControllerServicesState,
        connectorId: CONNECTOR_ID,
        processGroupId: PROCESS_GROUP_ID,
        breadcrumb: buildBreadcrumb(),
        controllerServices: [buildControllerService()],
        loadedTimestamp: '12:00:00 EST',
        status: 'success',
        ...overrides
    };
}

function setup(options: SetupOptions = {}): {
    fixture: ComponentFixture<ConnectorControllerServicesComponent>;
    component: ConnectorControllerServicesComponent;
    dispatchSpy: ReturnType<typeof vi.spyOn>;
    store: MockStore;
} {
    const routeParams =
        options.routeParams !== undefined
            ? options.routeParams
            : { id: CONNECTOR_ID, processGroupId: PROCESS_GROUP_ID };

    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
        declarations: [ConnectorControllerServicesComponent],
        imports: [NoopAnimationsModule],
        providers: [
            provideRouter([]),
            provideMockStore({
                initialState: {},
                selectors: [
                    {
                        selector: selectConnectorControllerServicesState,
                        value: buildState(options.state)
                    },
                    { selector: selectControllerServiceIdFromRoute, value: options.selectedServiceId ?? null },
                    { selector: selectRouteParams, value: routeParams },
                    {
                        selector: selectConnectorCanvasEntity,
                        value:
                            options.inTroubleshooting === undefined
                                ? null
                                : {
                                      id: CONNECTOR_ID,
                                      component: {
                                          state: options.inTroubleshooting ? 'TROUBLESHOOTING' : 'STOPPED'
                                      }
                                  }
                    },
                    { selector: selectCurrentUser, value: null },
                    { selector: selectFlowConfiguration, value: null }
                ]
            })
        ]
    }).overrideComponent(ConnectorControllerServicesComponent, {
        set: { template: '' }
    });

    const store = TestBed.inject(MockStore);
    const dispatchSpy = vi.spyOn(store, 'dispatch');
    const fixture = TestBed.createComponent(ConnectorControllerServicesComponent);
    return { fixture, component: fixture.componentInstance, dispatchSpy, store };
}

describe('ConnectorControllerServicesComponent', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Initialization', () => {
        it('should dispatch loadConnectorControllerServices when route params are available', fakeAsync(() => {
            const { dispatchSpy } = setup();
            tick();

            expect(dispatchSpy).toHaveBeenCalledWith(
                loadConnectorControllerServices({
                    request: { connectorId: CONNECTOR_ID, processGroupId: PROCESS_GROUP_ID }
                })
            );
        }));

        it('should load the connector entity and start entity polling when route params are available', fakeAsync(() => {
            const { dispatchSpy } = setup();
            tick();

            expect(dispatchSpy).toHaveBeenCalledWith(loadConnectorEntity({ connectorId: CONNECTOR_ID }));
            expect(dispatchSpy).toHaveBeenCalledWith(
                startConnectorControllerServicesEntityPolling({ connectorId: CONNECTOR_ID })
            );
        }));

        it('should not dispatch loadConnectorControllerServices when connectorId is missing from route', fakeAsync(() => {
            const { dispatchSpy } = setup({ routeParams: { processGroupId: PROCESS_GROUP_ID } });
            tick();

            const loadDispatches = dispatchSpy.mock.calls.filter(
                (call: unknown[]) => (call[0] as { type: string }).type === loadConnectorControllerServices.type
            );
            expect(loadDispatches).toHaveLength(0);
        }));
    });

    describe('Destruction', () => {
        it('should stop entity polling and reset state on destroy', () => {
            const { fixture, dispatchSpy } = setup();
            dispatchSpy.mockClear();

            fixture.destroy();

            expect(dispatchSpy).toHaveBeenCalledWith(stopConnectorControllerServicesEntityPolling());
            expect(dispatchSpy).toHaveBeenCalledWith(resetConnectorControllerServicesState());
        });
    });

    describe('refreshControllerServiceListing', () => {
        it('should dispatch loadConnectorControllerServices using the captured route ids', fakeAsync(() => {
            const { component, dispatchSpy } = setup();
            tick();
            dispatchSpy.mockClear();

            component.refreshControllerServiceListing();

            expect(dispatchSpy).toHaveBeenCalledWith(
                loadConnectorControllerServices({
                    request: { connectorId: CONNECTOR_ID, processGroupId: PROCESS_GROUP_ID }
                })
            );
        }));
    });

    describe('Action dispatchers', () => {
        it('selectControllerService should dispatch with the connector and process group context', fakeAsync(() => {
            const { component, dispatchSpy } = setup();
            tick();
            dispatchSpy.mockClear();

            component.selectControllerService(buildControllerService({ id: 'svc-42' } as any));

            expect(dispatchSpy).toHaveBeenCalledWith(
                selectConnectorControllerService({
                    request: {
                        connectorId: CONNECTOR_ID,
                        processGroupId: PROCESS_GROUP_ID,
                        serviceId: 'svc-42'
                    }
                })
            );
        }));

        it('goToControllerService should navigate to the service owning process group', fakeAsync(() => {
            const { component, dispatchSpy } = setup();
            tick();
            dispatchSpy.mockClear();

            component.goToControllerService(
                buildControllerService({ id: 'svc-inherited', parentGroupId: 'pg-parent' })
            );

            expect(dispatchSpy).toHaveBeenCalledWith(
                navigateToConnectorControllerService({
                    processGroupId: 'pg-parent',
                    serviceId: 'svc-inherited'
                })
            );
        }));

        it('configureControllerService should dispatch openViewControllerServiceDialog', fakeAsync(() => {
            const { component, dispatchSpy } = setup();
            tick();
            dispatchSpy.mockClear();

            const entity = buildControllerService();
            component.configureControllerService(entity);

            expect(dispatchSpy).toHaveBeenCalledWith(openViewControllerServiceDialog({ controllerService: entity }));
        }));

        it('configureControllerService should dispatch the editable dialog in troubleshooting', fakeAsync(() => {
            const { component, dispatchSpy } = setup({ inTroubleshooting: true });
            tick();
            dispatchSpy.mockClear();
            const entity = buildControllerService();

            component.configureControllerService(entity);

            expect(dispatchSpy).toHaveBeenCalledWith(
                openConfigureConnectorControllerServiceDialog({
                    request: { id: entity.id, controllerService: entity, readonly: false }
                })
            );
        }));

        it('should not dispatch Controller Service operations for an unreadable entity', fakeAsync(() => {
            const { component, dispatchSpy } = setup({ inTroubleshooting: true });
            tick();
            dispatchSpy.mockClear();
            const entity = buildControllerService({
                permissions: { canRead: false, canWrite: false },
                component: undefined
            });

            component.viewControllerServiceDocumentation(entity);
            component.configureControllerService(entity);
            component.enableControllerService(entity);
            component.disableControllerService(entity);
            component.changeControllerServiceVersion(entity);
            component.deleteControllerService(entity);

            expect(dispatchSpy).not.toHaveBeenCalled();
        }));

        it('should dispatch the troubleshooting operations for a readable entity', fakeAsync(() => {
            const { component, dispatchSpy } = setup({ inTroubleshooting: true });
            tick();
            dispatchSpy.mockClear();
            const entity = buildControllerService();

            component.openNewControllerServiceDialog();
            component.enableControllerService(entity);
            component.disableControllerService(entity);
            component.changeControllerServiceVersion(entity);
            component.deleteControllerService(entity);
            component.openAdvancedUi(entity);

            expect(dispatchSpy.mock.calls.map(([action]: unknown[]) => action)).toEqual([
                openNewConnectorControllerServiceDialog(),
                openEnableConnectorControllerServiceDialog({ request: { id: entity.id, controllerService: entity } }),
                openDisableConnectorControllerServiceDialog({ request: { id: entity.id, controllerService: entity } }),
                openChangeConnectorControllerServiceVersionDialog({
                    request: {
                        id: entity.id,
                        uri: entity.uri,
                        revision: entity.revision,
                        type: 'org.apache.nifi.MyService',
                        bundle: { group: 'org.apache.nifi', artifact: 'nifi-services-nar', version: '2.0.0' }
                    }
                }),
                promptConnectorControllerServiceDeletion({ request: { controllerService: entity } }),
                navigateToAdvancedConnectorControllerServiceUi({ id: entity.id })
            ]);
        }));

        it('clearBulletinsControllerService should clear from the most recent bulletin timestamp', fakeAsync(() => {
            const { component, dispatchSpy } = setup({ inTroubleshooting: true });
            tick();
            dispatchSpy.mockClear();
            const entity = buildControllerService({
                bulletins: [
                    { id: 2, canRead: true, timestamp: '12:00:00 UTC', timestampIso: '2026-01-01T12:00:00.000Z' },
                    { id: 1, canRead: true, timestamp: '11:00:00 UTC', timestampIso: '2026-01-01T11:00:00.000Z' }
                ] as ControllerServiceEntity['bulletins']
            });

            component.clearBulletinsControllerService(entity);
            component.clearBulletinsControllerService(buildControllerService({ bulletins: [] }));

            expect(dispatchSpy.mock.calls.map(([action]: unknown[]) => action)).toEqual([
                clearConnectorControllerServiceBulletins({
                    request: {
                        uri: entity.uri,
                        componentId: entity.id,
                        componentType: ComponentType.ControllerService,
                        fromTimestamp: '2026-01-01T12:00:00.000Z'
                    }
                })
            ]);
        }));

        it('viewStateControllerService should dispatch with connectorId and canClear true for DISABLED state', fakeAsync(() => {
            const { component, dispatchSpy } = setup({ inTroubleshooting: true });
            tick();
            dispatchSpy.mockClear();

            const entity = buildControllerService({
                id: 'svc-state',
                component: {
                    parentGroupId: PROCESS_GROUP_ID,
                    name: 'Stateful Service',
                    type: 'org.apache.nifi.MyService',
                    bundle: { group: 'g', artifact: 'a', version: 'v' },
                    state: 'DISABLED',
                    persistsState: true
                } as any
            } as any);

            component.viewStateControllerService(entity);

            expect(dispatchSpy).toHaveBeenCalledWith(
                getComponentStateAndOpenDialog({
                    request: {
                        componentName: 'Stateful Service',
                        componentId: 'svc-state',
                        componentType: ComponentType.ControllerService,
                        canClear: true,
                        connectorId: CONNECTOR_ID
                    }
                })
            );
        }));

        it('viewStateControllerService should set canClear false when the service is not DISABLED', fakeAsync(() => {
            const { component, dispatchSpy } = setup();
            tick();
            dispatchSpy.mockClear();

            const entity = buildControllerService({
                id: 'svc-state',
                status: { runStatus: 'ENABLED', validationStatus: 'VALID' },
                component: {
                    parentGroupId: PROCESS_GROUP_ID,
                    name: 'Enabled Service',
                    type: 'org.apache.nifi.MyService',
                    bundle: { group: 'g', artifact: 'a', version: 'v' },
                    state: 'ENABLED',
                    persistsState: true
                } as any
            } as any);

            component.viewStateControllerService(entity);

            expect(dispatchSpy).toHaveBeenCalledWith(
                getComponentStateAndOpenDialog({
                    request: {
                        componentName: 'Enabled Service',
                        componentId: 'svc-state',
                        componentType: ComponentType.ControllerService,
                        canClear: false,
                        connectorId: CONNECTOR_ID
                    }
                })
            );
        }));

        it('viewControllerServiceDocumentation should dispatch navigateToComponentDocumentation with back navigation route', fakeAsync(() => {
            const { component, dispatchSpy } = setup();
            tick();
            dispatchSpy.mockClear();

            const entity = buildControllerService({
                id: 'svc-doc',
                component: {
                    parentGroupId: PROCESS_GROUP_ID,
                    name: 'Doc Service',
                    type: 'org.apache.nifi.DocService',
                    bundle: { group: 'org.apache.nifi', artifact: 'nifi-doc-nar', version: '2.0.0' },
                    state: 'DISABLED',
                    persistsState: false
                } as any
            } as any);

            component.viewControllerServiceDocumentation(entity);

            expect(dispatchSpy).toHaveBeenCalledWith(
                navigateToComponentDocumentation({
                    request: {
                        parameters: {
                            componentType: ComponentType.ControllerService,
                            type: 'org.apache.nifi.DocService',
                            group: 'org.apache.nifi',
                            artifact: 'nifi-doc-nar',
                            version: '2.0.0'
                        },
                        backNavigation: {
                            route: [
                                '/connectors',
                                CONNECTOR_ID,
                                'canvas',
                                PROCESS_GROUP_ID,
                                'controller-services',
                                'svc-doc'
                            ],
                            routeBoundary: ['/documentation'],
                            context: 'Controller Service'
                        }
                    }
                })
            );
        }));
    });

    describe('Filter and scope helpers', () => {
        it('shouldShowFilter is false when the breadcrumb has no parent', fakeAsync(() => {
            const { component } = setup({
                state: { breadcrumb: buildBreadcrumb() }
            });
            tick();

            expect(component.shouldShowFilter()).toBe(false);
        }));

        it('shouldShowFilter is true when the breadcrumb has a parent breadcrumb', fakeAsync(() => {
            const parent = buildBreadcrumb({
                id: 'pg-parent',
                breadcrumb: { id: 'pg-parent', name: 'Parent Group' }
            });
            const { component } = setup({
                state: { breadcrumb: buildBreadcrumb({}, parent) }
            });
            tick();

            expect(component.shouldShowFilter()).toBe(true);
        }));

        it('definedByCurrentGroup returns true only for services defined in the current group', () => {
            const { component } = setup();
            const breadcrumb = buildBreadcrumb();

            const filter = component.definedByCurrentGroup(breadcrumb);

            expect(filter(buildControllerService({ parentGroupId: PROCESS_GROUP_ID }))).toBe(true);
            expect(filter(buildControllerService({ parentGroupId: 'pg-other' }))).toBe(false);
        });

        it('definedByCurrentGroup returns true for all services when breadcrumb is null', () => {
            const { component } = setup();
            const filter = component.definedByCurrentGroup(null);

            expect(filter(buildControllerService({ parentGroupId: 'anything' }))).toBe(true);
        });

        it('formatScope returns the readable breadcrumb name for the parent group', () => {
            const { component } = setup();
            const parent = buildBreadcrumb({
                id: 'pg-parent',
                breadcrumb: { id: 'pg-parent', name: 'Parent Group' }
            });
            const breadcrumb = buildBreadcrumb({}, parent);

            const formatter = component.formatScope(breadcrumb);

            expect(formatter(buildControllerService({ parentGroupId: PROCESS_GROUP_ID }))).toBe('Current Group');
            expect(formatter(buildControllerService({ parentGroupId: 'pg-parent' }))).toBe('Parent Group');
        });

        it('formatScope returns the breadcrumb id when permissions deny read', () => {
            const { component } = setup();
            const breadcrumb = buildBreadcrumb({
                permissions: { canRead: false, canWrite: false }
            });

            const formatter = component.formatScope(breadcrumb);

            expect(formatter(buildControllerService({ parentGroupId: PROCESS_GROUP_ID }))).toBe(PROCESS_GROUP_ID);
        });

        it('formatScope returns an empty string when the entity is not in any breadcrumb', () => {
            const { component } = setup();
            const breadcrumb = buildBreadcrumb();
            const formatter = component.formatScope(breadcrumb);

            expect(formatter(buildControllerService({ parentGroupId: 'unknown' }))).toBe('');
        });

        it('formatScope returns an empty-string formatter when no breadcrumb is provided', () => {
            const { component } = setup();
            const formatter = component.formatScope(null);

            expect(formatter(buildControllerService())).toBe('');
        });

        it('toggleFilter inverts showCurrentScopeOnly', () => {
            const { component } = setup();
            expect(component.showCurrentScopeOnly()).toBe(false);

            component.toggleFilter();
            expect(component.showCurrentScopeOnly()).toBe(true);

            component.toggleFilter();
            expect(component.showCurrentScopeOnly()).toBe(false);
        });

        it('filteredControllerServices returns all services when the scope filter is off', fakeAsync(() => {
            const services = [
                buildControllerService({ id: 's1', parentGroupId: PROCESS_GROUP_ID }),
                buildControllerService({ id: 's2', parentGroupId: 'pg-other' })
            ];
            const { component } = setup({
                state: { controllerServices: services }
            });
            tick();

            expect(component.filteredControllerServices().map((s) => s.id)).toEqual(['s1', 's2']);
        }));

        it('filteredControllerServices restricts to current group when the scope filter is on', fakeAsync(() => {
            const services = [
                buildControllerService({ id: 's1', parentGroupId: PROCESS_GROUP_ID }),
                buildControllerService({ id: 's2', parentGroupId: 'pg-other' })
            ];
            const { component } = setup({
                state: { controllerServices: services }
            });
            tick();

            component.toggleFilter();

            expect(component.filteredControllerServices().map((s) => s.id)).toEqual(['s1']);
        }));

        it('getFilterTooltip uses the breadcrumb name when readable', () => {
            const { component } = setup();
            const tooltip = component.getFilterTooltip(buildBreadcrumb());

            expect(tooltip).toContain("'Current Group'");
        });

        it('getFilterTooltip falls back to a generic label when breadcrumb is null', () => {
            const { component } = setup();
            const tooltip = component.getFilterTooltip(null);

            expect(tooltip).toContain("'this group'");
        });
    });

    describe('Loading state', () => {
        it('isInitialLoading is true while loading on the very first request', fakeAsync(() => {
            const { component } = setup({
                state: { status: 'loading', hasAttemptedLoad: false }
            });
            tick();

            expect(component.isInitialLoading()).toBe(true);
        }));

        it('isInitialLoading is false on subsequent loads after data has arrived', fakeAsync(() => {
            const { component } = setup({
                state: { status: 'loading', hasAttemptedLoad: true }
            });
            tick();

            expect(component.isInitialLoading()).toBe(false);
        }));

        it('isInitialLoading is false on retry after a failed first load', fakeAsync(() => {
            const { component } = setup({
                state: {
                    status: 'loading',
                    hasAttemptedLoad: true,
                    controllerServices: [],
                    loadedTimestamp: ''
                }
            });
            tick();

            expect(component.isInitialLoading()).toBe(false);
        }));
    });

    describe('canModifyParent', () => {
        it('returns false outside troubleshooting', () => {
            const { component } = setup();
            const predicate = component.canModifyParent();

            expect(predicate(buildControllerService())).toBe(false);
        });

        it('uses the actual owning parent breadcrumb permission in troubleshooting', fakeAsync(() => {
            const parent = buildBreadcrumb({
                id: 'pg-parent',
                permissions: { canRead: true, canWrite: false },
                breadcrumb: { id: 'pg-parent', name: 'Parent' }
            });
            const { component } = setup({
                inTroubleshooting: true,
                state: { breadcrumb: buildBreadcrumb({}, parent) }
            });
            tick();
            const predicate = component.canModifyParent();

            expect(predicate(buildControllerService({ parentGroupId: PROCESS_GROUP_ID }))).toBe(true);
            expect(predicate(buildControllerService({ parentGroupId: 'pg-parent' }))).toBe(false);
        }));
    });

    describe('Breadcrumb route generator', () => {
        it('produces a connector-canvas-rooted route for the supplied process group id', fakeAsync(() => {
            const { component } = setup();
            tick();

            const generator = component.getBreadcrumbRouteGenerator();

            expect(generator('pg-target')).toEqual(['/connectors', CONNECTOR_ID, 'canvas', 'pg-target']);
        }));
    });
});
