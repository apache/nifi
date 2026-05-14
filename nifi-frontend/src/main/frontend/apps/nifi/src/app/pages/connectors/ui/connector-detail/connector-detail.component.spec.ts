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
import { ConnectorDetail } from './connector-detail.component';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import {
    SystemTokensService,
    ConnectorEntity,
    ConnectorConfigurationService
} from '@nifi/shared';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import { ConnectorDetailsContent } from '../connector-details-content/connector-details-content.component';
import { of, throwError } from 'rxjs';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { provideRouter, Router } from '@angular/router';
import { selectConnectorIdFromRoute } from '../../state/connectors-listing/connectors-listing.selectors';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { errorFeatureKey } from '../../../../state/error';
import { initialState as errorInitialState } from '../../../../state/error/error.reducer';
import { selectBackNavigation } from '../../../../state/navigation/navigation.selectors';
import { ConnectorMessageHost } from '../../service/connector-message-host.service';
import { currentUserFeatureKey } from '../../../../state/current-user';
import * as fromCurrentUser from '../../../../state/current-user/current-user.reducer';
import { navigationFeatureKey } from '../../../../state/navigation';
import { initialState as navigationInitialState } from '../../../../state/navigation/navigation.reducer';
import { flowConfigurationFeatureKey } from '../../../../state/flow-configuration';
import * as fromFlowConfiguration from '../../../../state/flow-configuration/flow-configuration.reducer';
import { clusterSummaryFeatureKey } from '../../../../state/cluster-summary';
import { initialState as clusterSummaryInitialState } from '../../../../state/cluster-summary/cluster-summary.reducer';
import { loginConfigurationFeatureKey } from '../../../../state/login-configuration';
import { initialState as loginConfigurationInitialState } from '../../../../state/login-configuration/login-configuration.reducer';
import { aboutFeatureKey } from '../../../../state/about';
import { initialState as aboutInitialState } from '../../../../state/about/about.reducer';
import { MatIconTestingModule } from '@angular/material/icon/testing';
import type { Mocked } from 'vitest';

describe('ConnectorDetail', () => {
    interface SetupOptions {
        connectorId?: string;
        connectorResponse?: ConnectorEntity;
        errorResponse?: HttpErrorResponse;
        errorMessage?: string;
        backNavigation?: { route: string[]; routeBoundary: string[]; context: string } | null;
    }

    function createMockConnector(overrides: Partial<ConnectorEntity> = {}): ConnectorEntity {
        return {
            id: 'test-connector-1',
            uri: 'http://localhost:4200/nifi-api/connectors/test-connector-1',
            permissions: { canRead: true, canWrite: true },
            bulletins: [],
            status: {
                runStatus: 'RUNNING',
                validationStatus: 'VALID'
            },
            component: {
                id: 'test-connector-1',
                name: 'Test Connector',
                type: 'TestConnector',
                state: 'RUNNING',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'test-connector',
                    version: '1.0.0'
                },
                managedProcessGroupId: 'pg-1',
                availableActions: [
                    { name: 'START', description: 'Start action', allowed: true },
                    { name: 'STOP', description: 'Stop action', allowed: true },
                    { name: 'CONFIGURE', description: 'Configure action', allowed: true },
                    { name: 'DELETE', description: 'Delete action', allowed: true }
                ]
            },
            revision: {
                version: 1
            },
            ...overrides
        };
    }

    async function setup(options: SetupOptions = {}) {
        const {
            connectorId = 'test-connector-1',
            connectorResponse = createMockConnector(),
            errorResponse,
            errorMessage = 'Connector not found'
        } = options;

        const mockConnectorConfigurationService = {
            getConnector: vi.fn()
        };

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue(errorMessage)
        };

        const mockConnectorMessageHost = {
            startListening: vi.fn()
        };

        await TestBed.configureTestingModule({
            imports: [
                ConnectorDetail,
                MockComponent(Navigation),
                MockComponent(ConnectorDetailsContent),
                MatIconTestingModule
            ],
            providers: [
                provideRouter([]),
                provideMockStore({
                    initialState: {
                        [currentUserFeatureKey]: fromCurrentUser.initialState,
                        [navigationFeatureKey]: navigationInitialState,
                        [flowConfigurationFeatureKey]: fromFlowConfiguration.initialState,
                        [errorFeatureKey]: errorInitialState,
                        [clusterSummaryFeatureKey]: clusterSummaryInitialState,
                        [loginConfigurationFeatureKey]: loginConfigurationInitialState,
                        [aboutFeatureKey]: aboutInitialState
                    },
                    selectors: [
                        { selector: selectConnectorIdFromRoute, value: connectorId },
                        { selector: selectBackNavigation, value: options.backNavigation ?? null }
                    ]
                }),
                {
                    provide: SystemTokensService,
                    useValue: {
                        appendStyleSheet: vi.fn()
                    }
                },
                {
                    provide: ConnectorConfigurationService,
                    useValue: mockConnectorConfigurationService
                },
                {
                    provide: ErrorHelper,
                    useValue: mockErrorHelper
                },
                {
                    provide: ConnectorMessageHost,
                    useValue: mockConnectorMessageHost
                }
            ],
            schemas: [NO_ERRORS_SCHEMA]
        }).compileComponents();

        const store = TestBed.inject(MockStore);
        const router = TestBed.inject(Router);
        const errorHelper = TestBed.inject(ErrorHelper) as Mocked<ErrorHelper>;

        if (errorResponse) {
            mockConnectorConfigurationService.getConnector.mockReturnValue(throwError(() => errorResponse));
        } else {
            mockConnectorConfigurationService.getConnector.mockReturnValue(of(connectorResponse));
        }

        const fixture = TestBed.createComponent(ConnectorDetail);
        const component = fixture.componentInstance;
        const routerNavigateSpy = vi.spyOn(router, 'navigate');
        const connectorConfigurationService = TestBed.inject(
            ConnectorConfigurationService
        ) as Mocked<ConnectorConfigurationService>;
        const connectorMessageHost = TestBed.inject(ConnectorMessageHost) as Mocked<ConnectorMessageHost>;

        return {
            fixture,
            component,
            store,
            router,
            connectorConfigurationService,
            errorHelper,
            routerNavigateSpy,
            connectorMessageHost
        };
    }

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component, fixture } = await setup();
            fixture.detectChanges();

            expect(component).toBeTruthy();
        });

        it('should set loading to true initially', async () => {
            const { component } = await setup();

            expect(component.loading).toBe(true);
        });

        it('should set loading to false after connector loads', async () => {
            const { component } = await setup();
            component.ngOnInit();

            expect(component.loading).toBe(false);
        });
    });

    describe('Custom details URL rendering', () => {
        it('should set frameSource with sanitized URL and connectorId when detailsUrl is present', async () => {
            const connectorWithDetailsUrl = createMockConnector({
                component: {
                    ...createMockConnector().component,
                    detailsUrl: 'http://localhost:4200/custom-details'
                }
            });

            const { component } = await setup({ connectorResponse: connectorWithDetailsUrl });
            component.ngOnInit();

            expect(component.frameSource).toBeTruthy();
            expect((component.frameSource as any).changingThisBreaksApplicationSecurity).toContain(
                'http://localhost:4200/custom-details?connectorId=test-connector-1'
            );
            expect(component.connector).toEqual(connectorWithDetailsUrl);
            expect(component.loading).toBe(false);
        });

        it('should set frameSource to null when detailsUrl is absent', async () => {
            const { component } = await setup();
            component.ngOnInit();

            expect(component.frameSource).toBeNull();
            expect(component.loading).toBe(false);
        });
    });

    describe('URL sanitization', () => {
        it('should sanitize and trust valid URLs with connectorId appended', async () => {
            const connectorWithDetailsUrl = createMockConnector({
                component: {
                    ...createMockConnector().component,
                    detailsUrl: 'http://localhost:4200/custom-details'
                }
            });

            const { component } = await setup({ connectorResponse: connectorWithDetailsUrl });
            component.ngOnInit();

            expect(component.frameSource).toBeTruthy();
            expect((component.frameSource as any).changingThisBreaksApplicationSecurity).toContain(
                'http://localhost:4200/custom-details?connectorId=test-connector-1'
            );
        });

        it('should mark invalid URLs as unsafe', async () => {
            const consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

            try {
                const { component } = await setup();
                component.connector = createMockConnector();
                const invalidUrl = 'javascript:alert("xss")';
                const result = (component as any).getFrameSource(invalidUrl);

                expect(result).toBeTruthy();
                expect(result.changingThisBreaksApplicationSecurity).toContain('unsafe:');
            } finally {
                consoleWarnSpy.mockRestore();
            }
        });
    });

    describe('Permission handling', () => {
        it('should set errorMessage when canRead is false', async () => {
            const connectorWithoutReadPermission = createMockConnector({
                permissions: { canRead: false, canWrite: false }
            });

            const { component } = await setup({ connectorResponse: connectorWithoutReadPermission });
            component.ngOnInit();

            expect(component.errorMessage).toBe('Insufficient permissions to view this connector.');
            expect(component.frameSource).toBeNull();
        });

        it('should not set errorMessage when canRead is true', async () => {
            const { component } = await setup();
            component.ngOnInit();

            expect(component.errorMessage).toBeNull();
        });
    });

    describe('Error handling', () => {
        it('should set errorMessage on fetch error', async () => {
            const errorResponse = new HttpErrorResponse({
                error: 'Connector not found',
                status: 404,
                statusText: 'Not Found'
            });
            const errorMessage = 'Connector not found';

            const { component, errorHelper } = await setup({
                errorResponse,
                errorMessage
            });

            component.ngOnInit();

            expect(errorHelper.getErrorString).toHaveBeenCalledWith(errorResponse);
            expect(component.errorMessage).toBe(errorMessage);
            expect(component.loading).toBe(false);
        });
    });

    describe('Route parameter changes', () => {
        it('should reload connector when route parameter changes', async () => {
            const firstConnector = createMockConnector();
            const secondConnector = createMockConnector({
                id: 'test-connector-2',
                component: {
                    ...createMockConnector().component,
                    detailsUrl: 'http://localhost:4200/custom-details'
                }
            });

            const { component, connectorConfigurationService, store } = await setup({
                connectorResponse: firstConnector
            });

            component.ngOnInit();
            expect(component.connector).toEqual(firstConnector);

            connectorConfigurationService.getConnector.mockReturnValue(of(secondConnector));
            store.overrideSelector(selectConnectorIdFromRoute, 'test-connector-2');
            store.refreshState();

            expect(connectorConfigurationService.getConnector).toHaveBeenCalledTimes(2);
        });
    });

    describe('navigation', () => {
        it('should navigate to connector listing when returnToConnectorListing is called', async () => {
            const { component, routerNavigateSpy } = await setup();
            component.ngOnInit();

            component.returnToConnectorListing();

            expect(routerNavigateSpy).toHaveBeenCalledWith(['/connectors']);
        });
    });

    describe('fallback back link', () => {
        it('should show fallback back link when no backNavigation, not loading, and no error', async () => {
            const { fixture } = await setup({ backNavigation: null });
            fixture.detectChanges();

            const link = fixture.nativeElement.querySelector('[data-qa="fallback-back-link"]');
            expect(link).toBeTruthy();
            expect(link.textContent).toContain('Installed connectors');
        });

        it('should navigate to connector listing when fallback back link is clicked', async () => {
            const { fixture, routerNavigateSpy } = await setup({ backNavigation: null });
            fixture.detectChanges();

            const link = fixture.nativeElement.querySelector('[data-qa="fallback-back-link"]');
            link.click();

            expect(routerNavigateSpy).toHaveBeenCalledWith(['/connectors']);
        });

        it('should hide fallback back link when backNavigation exists', async () => {
            const { fixture } = await setup({
                backNavigation: {
                    route: ['/connectors', 'test-connector-1'],
                    routeBoundary: ['/connectors', 'test-connector-1', 'detail'],
                    context: 'connectors'
                }
            });
            fixture.detectChanges();

            const link = fixture.nativeElement.querySelector('[data-qa="fallback-back-link"]');
            expect(link).toBeNull();
        });

        it('should hide fallback back link while loading', async () => {
            const { component, fixture } = await setup({ backNavigation: null, connectorId: null as any });
            fixture.detectChanges();

            expect(component.loading).toBe(true);
            const link = fixture.nativeElement.querySelector('[data-qa="fallback-back-link"]');
            expect(link).toBeNull();
        });

        it('should hide fallback back link when there is an error', async () => {
            const errorResponse = new HttpErrorResponse({
                error: 'Not found',
                status: 404,
                statusText: 'Not Found'
            });
            const { fixture } = await setup({ backNavigation: null, errorResponse });
            fixture.detectChanges();

            const link = fixture.nativeElement.querySelector('[data-qa="fallback-back-link"]');
            expect(link).toBeNull();
        });
    });

    describe('postMessage host', () => {
        it('should start listening when connector has custom details URL', async () => {
            const connectorWithDetailsUrl = createMockConnector({
                component: {
                    ...createMockConnector().component,
                    detailsUrl: 'http://localhost:4200/custom-details'
                }
            });

            const { component, connectorMessageHost } = await setup({
                connectorResponse: connectorWithDetailsUrl
            });
            component.ngOnInit();

            expect(connectorMessageHost.startListening).toHaveBeenCalledTimes(1);
            expect(connectorMessageHost.startListening).toHaveBeenCalledWith(
                expect.objectContaining({
                    expectedOrigin: 'http://localhost:4200',
                    iframeElement: expect.any(Function)
                })
            );
        });

        it('should not start listening when no custom details URL', async () => {
            const { component, connectorMessageHost } = await setup();
            component.ngOnInit();

            expect(connectorMessageHost.startListening).not.toHaveBeenCalled();
        });

        it('should not start listening when connector lacks permissions', async () => {
            const noPermissionsConnector = createMockConnector({
                permissions: { canRead: false, canWrite: false }
            });

            const { component, connectorMessageHost } = await setup({
                connectorResponse: noPermissionsConnector
            });
            component.ngOnInit();

            expect(connectorMessageHost.startListening).not.toHaveBeenCalled();
        });
    });
});
