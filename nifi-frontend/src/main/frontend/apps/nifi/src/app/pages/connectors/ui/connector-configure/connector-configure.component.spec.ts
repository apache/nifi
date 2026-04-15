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
import { ConnectorConfigure } from './connector-configure.component';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { DomSanitizer } from '@angular/platform-browser';
import { ConnectorConfigurationService, ConnectorEntity, ConnectorWizard, SystemTokensService } from '@nifi/shared';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import { of, throwError } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { provideRouter, Router } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { selectConnectorIdFromRoute } from '../../state/connectors-listing/connectors-listing.selectors';
import { selectDisconnectionAcknowledged } from '../../../../state/cluster-summary/cluster-summary.selectors';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { MatIconTestingModule } from '@angular/material/icon/testing';
import { ConnectorMessageHost } from '../../service/connector-message-host.service';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ContextErrorBanner } from '../../../../ui/common/context-error-banner/context-error-banner.component';
import { currentUserFeatureKey, CurrentUser } from '../../../../state/current-user';
import * as fromCurrentUser from '../../../../state/current-user/current-user.reducer';
import { navigationFeatureKey } from '../../../../state/navigation';
import { initialState as navigationInitialState } from '../../../../state/navigation/navigation.reducer';
import { flowConfigurationFeatureKey } from '../../../../state/flow-configuration';
import * as fromFlowConfiguration from '../../../../state/flow-configuration/flow-configuration.reducer';
import { errorFeatureKey } from '../../../../state/error';
import { initialState as errorInitialState } from '../../../../state/error/error.reducer';
import { clusterSummaryFeatureKey } from '../../../../state/cluster-summary';
import { initialState as clusterSummaryInitialState } from '../../../../state/cluster-summary/cluster-summary.reducer';
import { loginConfigurationFeatureKey } from '../../../../state/login-configuration';
import { initialState as loginConfigurationInitialState } from '../../../../state/login-configuration/login-configuration.reducer';
import { aboutFeatureKey } from '../../../../state/about';
import { initialState as aboutInitialState } from '../../../../state/about/about.reducer';
import type { Mocked } from 'vitest';

function createMockCurrentUser(): CurrentUser {
    return {
        identity: 'admin',
        anonymous: false,
        provenancePermissions: { canRead: true, canWrite: true },
        countersPermissions: { canRead: true, canWrite: true },
        tenantsPermissions: { canRead: true, canWrite: true },
        controllerPermissions: { canRead: true, canWrite: true },
        policiesPermissions: { canRead: true, canWrite: true },
        systemPermissions: { canRead: true, canWrite: true },
        parameterContextPermissions: { canRead: true, canWrite: true },
        restrictedComponentsPermissions: { canRead: true, canWrite: true },
        connectorsPermissions: { canRead: true, canWrite: true },
        componentRestrictionPermissions: [],
        canVersionFlows: true,
        logoutSupported: true
    };
}

describe('ConnectorConfigure', () => {
    let component: ConnectorConfigure;
    let fixture: ComponentFixture<ConnectorConfigure>;
    let store: MockStore;
    let domSanitizer: DomSanitizer;
    let connectorConfigurationService: Mocked<ConnectorConfigurationService>;
    let connectorMessageHost: Mocked<ConnectorMessageHost>;

    const mockConnectorWithCustomUrl: ConnectorEntity = {
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
            configurationUrl: 'http://localhost:4200/custom-config',
            availableActions: [
                { name: 'START', description: 'Start action', allowed: true },
                { name: 'STOP', description: 'Stop action', allowed: true },
                { name: 'CONFIGURE', description: 'Configure action', allowed: true },
                { name: 'DELETE', description: 'Delete action', allowed: true }
            ]
        },
        revision: {
            version: 1
        }
    };

    const mockConnectorWithoutCustomUrl: ConnectorEntity = {
        id: 'test-connector-2',
        uri: 'http://localhost:4200/nifi-api/connectors/test-connector-2',
        permissions: { canRead: true, canWrite: true },
        bulletins: [],
        status: {
            runStatus: 'RUNNING',
            validationStatus: 'VALID'
        },
        component: {
            id: 'test-connector-2',
            name: 'Generic Connector',
            type: 'GenericConnector',
            state: 'RUNNING',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'generic-connector',
                version: '1.0.0'
            },
            managedProcessGroupId: 'pg-2',
            availableActions: [
                { name: 'START', description: 'Start action', allowed: true },
                { name: 'STOP', description: 'Stop action', allowed: true },
                { name: 'CONFIGURE', description: 'Configure action', allowed: true },
                { name: 'DELETE', description: 'Delete action', allowed: true }
            ]
        },
        revision: {
            version: 1
        }
    };

    beforeEach(async () => {
        vi.spyOn(console, 'warn').mockImplementation(() => {
            // noop
        });

        const mockConnectorConfigurationService = {
            getConnector: vi.fn(),
            getSecrets: vi.fn().mockReturnValue(of({ secrets: [] }))
        };

        const mockConnectorMessageHost = {
            startListening: vi.fn()
        };

        const mockClusterConnectionService = {
            isDisconnectionAcknowledged: vi.fn().mockReturnValue(false)
        };

        const mockErrorHelper = {
            getErrorString: vi.fn().mockReturnValue('Request failed')
        };

        await TestBed.configureTestingModule({
            imports: [
                ConnectorConfigure,
                MockComponent(Navigation),
                MockComponent(ContextErrorBanner),
                MockComponent(ConnectorWizard),
                MatIconTestingModule
            ],
            providers: [
                provideHttpClient(),
                provideHttpClientTesting(),
                provideRouter([]),
                provideMockStore({
                    initialState: {
                        [currentUserFeatureKey]: {
                            ...fromCurrentUser.initialState,
                            user: createMockCurrentUser(),
                            status: 'success' as const
                        },
                        [navigationFeatureKey]: navigationInitialState,
                        [flowConfigurationFeatureKey]: fromFlowConfiguration.initialState,
                        [errorFeatureKey]: errorInitialState,
                        [clusterSummaryFeatureKey]: clusterSummaryInitialState,
                        [loginConfigurationFeatureKey]: loginConfigurationInitialState,
                        [aboutFeatureKey]: aboutInitialState
                    }
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
                    provide: ConnectorMessageHost,
                    useValue: mockConnectorMessageHost
                },
                {
                    provide: ClusterConnectionService,
                    useValue: mockClusterConnectionService
                },
                {
                    provide: ErrorHelper,
                    useValue: mockErrorHelper
                }
            ],
            schemas: [NO_ERRORS_SCHEMA]
        }).compileComponents();

        store = TestBed.inject(MockStore);
        domSanitizer = TestBed.inject(DomSanitizer);
        connectorConfigurationService = TestBed.inject(
            ConnectorConfigurationService
        ) as Mocked<ConnectorConfigurationService>;
        connectorMessageHost = TestBed.inject(ConnectorMessageHost) as Mocked<ConnectorMessageHost>;

        store.overrideSelector(selectConnectorIdFromRoute, 'test-connector-1');
        store.overrideSelector(selectDisconnectionAcknowledged, false);
        store.refreshState();
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('should create', () => {
        connectorConfigurationService.getConnector.mockReturnValue(of(mockConnectorWithoutCustomUrl));
        fixture = TestBed.createComponent(ConnectorConfigure);
        component = fixture.componentInstance;
        fixture.detectChanges();

        expect(component).toBeTruthy();
    });

    describe('iframe rendering with custom configuration URL', () => {
        beforeEach(() => {
            connectorConfigurationService.getConnector.mockReturnValue(of(mockConnectorWithCustomUrl));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;
        });

        it('should set frameSource with sanitized URL when configurationUrl is present', () => {
            component.ngOnInit();

            expect(component.frameSource).toBeTruthy();
            expect(component.connector).toEqual(mockConnectorWithCustomUrl);
            expect(component.loading).toBe(false);
        });

        it('should not set connector when configurationUrl is absent', () => {
            connectorConfigurationService.getConnector.mockReturnValue(of(mockConnectorWithoutCustomUrl));
            component.ngOnInit();

            expect(component.frameSource).toBeNull();
            expect(component.connector).toEqual(mockConnectorWithoutCustomUrl);
        });
    });

    describe('wizard rendering without custom configuration URL', () => {
        beforeEach(() => {
            connectorConfigurationService.getConnector.mockReturnValue(of(mockConnectorWithoutCustomUrl));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;
        });

        it('should set frameSource to null when configurationUrl is absent', () => {
            component.ngOnInit();

            expect(component.frameSource).toBeNull();
            expect(component.connector).toEqual(mockConnectorWithoutCustomUrl);
            expect(component.loading).toBe(false);
        });
    });

    describe('URL sanitization', () => {
        beforeEach(() => {
            connectorConfigurationService.getConnector.mockReturnValue(of(mockConnectorWithCustomUrl));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;
        });

        it('should sanitize and trust valid URLs', () => {
            const validUrl = 'http://localhost:4200/custom-config';
            const result = (component as unknown as { getFrameSource(url: string): unknown }).getFrameSource(validUrl);

            expect(result).toBeTruthy();
        });

        it('should return null when sanitizer returns null', () => {
            vi.spyOn(domSanitizer, 'sanitize').mockReturnValue(null);

            const url = 'some-invalid-url';
            const result = (component as unknown as { getFrameSource(url: string): unknown }).getFrameSource(url);

            expect(result).toBeNull();
        });

        it('should use two-step sanitization process', () => {
            const sanitizeSpy = vi.spyOn(domSanitizer, 'sanitize');
            const bypassSpy = vi.spyOn(domSanitizer, 'bypassSecurityTrustResourceUrl');

            const url = 'http://localhost:4200/custom-config';
            (component as unknown as { getFrameSource(url: string): void }).getFrameSource(url);

            expect(sanitizeSpy).toHaveBeenCalled();
            expect(bypassSpy).toHaveBeenCalled();
        });
    });

    describe('permission checking', () => {
        it('should show permission error when connector lacks canRead permission', () => {
            const noReadPermissionConnector: ConnectorEntity = {
                ...mockConnectorWithoutCustomUrl,
                permissions: { canRead: false, canWrite: true }
            };
            connectorConfigurationService.getConnector.mockReturnValue(of(noReadPermissionConnector));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;

            component.ngOnInit();

            expect(component.errorMessage).toBe('Insufficient permissions to configure this connector.');
            expect(component.loading).toBe(false);
            expect(component.frameSource).toBeNull();
        });

        it('should show permission error when connector lacks canWrite permission', () => {
            const noWritePermissionConnector: ConnectorEntity = {
                ...mockConnectorWithoutCustomUrl,
                permissions: { canRead: true, canWrite: false }
            };
            connectorConfigurationService.getConnector.mockReturnValue(of(noWritePermissionConnector));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;

            component.ngOnInit();

            expect(component.errorMessage).toBe('Insufficient permissions to configure this connector.');
            expect(component.loading).toBe(false);
            expect(component.frameSource).toBeNull();
        });

        it('should show permission error when connector lacks both permissions', () => {
            const noPermissionsConnector: ConnectorEntity = {
                ...mockConnectorWithoutCustomUrl,
                permissions: { canRead: false, canWrite: false }
            };
            connectorConfigurationService.getConnector.mockReturnValue(of(noPermissionsConnector));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;

            component.ngOnInit();

            expect(component.errorMessage).toBe('Insufficient permissions to configure this connector.');
            expect(component.loading).toBe(false);
            expect(component.frameSource).toBeNull();
        });

        it('should render wizard when connector has sufficient permissions and no custom URL', () => {
            connectorConfigurationService.getConnector.mockReturnValue(of(mockConnectorWithoutCustomUrl));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;

            component.ngOnInit();

            expect(component.errorMessage).toBeNull();
            expect(component.connector).toEqual(mockConnectorWithoutCustomUrl);
            expect(component.frameSource).toBeNull();
            expect(component.loading).toBe(false);
        });

        it('should render iframe when connector has sufficient permissions and custom URL', () => {
            connectorConfigurationService.getConnector.mockReturnValue(of(mockConnectorWithCustomUrl));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;

            component.ngOnInit();

            expect(component.errorMessage).toBeNull();
            expect(component.connector).toEqual(mockConnectorWithCustomUrl);
            expect(component.frameSource).toBeTruthy();
            expect(component.loading).toBe(false);
        });

        it('should display permission error message in template', () => {
            const noPermissionsConnector: ConnectorEntity = {
                ...mockConnectorWithoutCustomUrl,
                permissions: { canRead: false, canWrite: false }
            };
            connectorConfigurationService.getConnector.mockReturnValue(of(noPermissionsConnector));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;
            fixture.detectChanges();

            const compiled = fixture.nativeElement;
            const errorContainer = compiled.querySelector('[data-qa="error-container"]');
            const errorMessage = compiled.querySelector('[data-qa="error-message"]');
            const returnButton = compiled.querySelector('[data-qa="return-button"]');

            expect(errorContainer).toBeTruthy();
            expect(errorMessage?.textContent).toContain('Insufficient permissions to configure this connector.');
            expect(returnButton).toBeTruthy();
            expect(returnButton?.textContent).toContain('Return to Connectors');
        });

        it('should not render wizard or iframe when permission error exists', () => {
            const noPermissionsConnector: ConnectorEntity = {
                ...mockConnectorWithoutCustomUrl,
                permissions: { canRead: false, canWrite: false }
            };
            connectorConfigurationService.getConnector.mockReturnValue(of(noPermissionsConnector));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;
            fixture.detectChanges();

            const compiled = fixture.nativeElement;
            const wizard = compiled.querySelector('connector-wizard');
            const iframe = compiled.querySelector('iframe');

            expect(wizard).toBeNull();
            expect(iframe).toBeNull();
        });
    });

    describe('error handling', () => {
        it('should set errorMessage when getConnector returns an HTTP error', () => {
            connectorConfigurationService.getConnector.mockReturnValue(
                throwError(() => new HttpErrorResponse({ status: 500, error: 'Internal Server Error' }))
            );
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;

            component.ngOnInit();

            expect(component.errorMessage).toBeTruthy();
            expect(component.loading).toBe(false);
            expect(component.frameSource).toBeNull();
        });

        it('should not render wizard or iframe when getConnector fails', () => {
            connectorConfigurationService.getConnector.mockReturnValue(
                throwError(() => new HttpErrorResponse({ status: 404, error: 'Not Found' }))
            );
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;
            fixture.detectChanges();

            const wizard = fixture.nativeElement.querySelector('connector-wizard');
            const iframe = fixture.nativeElement.querySelector('iframe');

            expect(wizard).toBeNull();
            expect(iframe).toBeNull();
        });
    });

    describe('navigation', () => {
        it('should navigate to connector listing when returnToConnectorListing is called', () => {
            connectorConfigurationService.getConnector.mockReturnValue(of(mockConnectorWithoutCustomUrl));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;
            fixture.detectChanges();

            const router = TestBed.inject(Router);
            const navigateSpy = vi.spyOn(router, 'navigate').mockResolvedValue(true);
            component.returnToConnectorListing();

            expect(navigateSpy).toHaveBeenCalledWith(['/connectors']);
        });
    });

    describe('postMessage host', () => {
        it('should start listening for postMessage events when custom URL is present', () => {
            connectorConfigurationService.getConnector.mockReturnValue(of(mockConnectorWithCustomUrl));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;

            component.ngOnInit();

            expect(connectorMessageHost.startListening).toHaveBeenCalledTimes(1);
            expect(connectorMessageHost.startListening).toHaveBeenCalledWith(
                expect.objectContaining({
                    expectedOrigin: 'http://localhost:4200',
                    iframeElement: expect.any(Function)
                })
            );
        });

        it('should not start listening when no custom URL is present', () => {
            connectorConfigurationService.getConnector.mockReturnValue(of(mockConnectorWithoutCustomUrl));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;

            component.ngOnInit();

            expect(connectorMessageHost.startListening).not.toHaveBeenCalled();
        });

        it('should not start listening when connector lacks permissions', () => {
            const noPermissionsConnector: ConnectorEntity = {
                ...mockConnectorWithCustomUrl,
                permissions: { canRead: false, canWrite: false }
            };
            connectorConfigurationService.getConnector.mockReturnValue(of(noPermissionsConnector));
            fixture = TestBed.createComponent(ConnectorConfigure);
            component = fixture.componentInstance;

            component.ngOnInit();

            expect(connectorMessageHost.startListening).not.toHaveBeenCalled();
        });
    });
});
