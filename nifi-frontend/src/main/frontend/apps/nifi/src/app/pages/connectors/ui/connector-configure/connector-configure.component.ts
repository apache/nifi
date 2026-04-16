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

import { Component, DestroyRef, ElementRef, OnInit, SecurityContext, viewChild, inject } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import {
    CONNECTOR_MESSAGE_NAMESPACE,
    ConnectorConfigurationService,
    ConnectorEntity,
    ConnectorWizardConfig,
    ConnectorWizard,
    StandardConnectorWizardStore,
    ConnectorWizardStore,
    CONNECTOR_WIZARD_CONFIG,
    ParentToConnectorMessage,
    SystemTokensService,
    NifiSpinnerDirective
} from '@nifi/shared';
import { catchError, distinctUntilChanged, filter, switchMap } from 'rxjs/operators';
import { of } from 'rxjs';
import { selectConnectorIdFromRoute } from '../../state/connectors-listing/connectors-listing.selectors';
import { selectDisconnectionAcknowledged } from '../../../../state/cluster-summary/cluster-summary.selectors';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { MatButton } from '@angular/material/button';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ContextErrorBanner } from '../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorContextKey } from '../../../../state/error';
import { ConnectorMessageHost } from '../../service/connector-message-host.service';

function connectorWizardConfigFactory(): ConnectorWizardConfig {
    const router = inject(Router);
    const clusterConnectionService = inject(ClusterConnectionService);
    return {
        getDisconnectedNodeAcknowledged: () => clusterConnectionService.isDisconnectionAcknowledged(),
        onApplySuccess: (connectorId) => {
            if (connectorId) {
                router.navigate(['/connectors', connectorId]);
            } else {
                router.navigate(['/connectors']);
            }
        },
        onNavigateBack: (connectorId: string | null) => {
            if (connectorId) {
                router.navigate(['/connectors', connectorId]);
            } else {
                router.navigate(['/connectors']);
            }
        }
    };
}

@Component({
    selector: 'connector-configure',
    standalone: true,
    imports: [Navigation, ConnectorWizard, MatButton, ContextErrorBanner, NifiSpinnerDirective],
    templateUrl: './connector-configure.component.html',
    styleUrls: ['./connector-configure.component.scss'],
    providers: [
        StandardConnectorWizardStore,
        { provide: ConnectorWizardStore, useExisting: StandardConnectorWizardStore },
        { provide: CONNECTOR_WIZARD_CONFIG, useFactory: connectorWizardConfigFactory }
    ],
    host: {
        class: 'block h-full'
    }
})
export class ConnectorConfigure implements OnInit {
    private store = inject<Store<NiFiState>>(Store);
    private router = inject(Router);
    private domSanitizer = inject(DomSanitizer);
    private connectorConfigurationService = inject(ConnectorConfigurationService);
    private clusterConnectionService = inject(ClusterConnectionService);
    private errorHelper = inject(ErrorHelper);
    private destroyRef = inject(DestroyRef);
    protected systemTokensService = inject(SystemTokensService);
    private connectorMessageHost = inject(ConnectorMessageHost);

    readonly iframeRef = viewChild<ElementRef<HTMLIFrameElement>>('iframeRef');

    frameSource: SafeResourceUrl | null = null;
    connector: ConnectorEntity | null = null;
    loading = true;
    errorMessage: string | null = null;

    private childConnectorUiReady = false;

    ngOnInit(): void {
        this.store
            .select(selectDisconnectionAcknowledged)
            .pipe(distinctUntilChanged(), takeUntilDestroyed(this.destroyRef))
            .subscribe(() => {
                if (this.childConnectorUiReady && this.connector?.component?.configurationUrl) {
                    this.postDisconnectedNodeAcknowledgmentToChild();
                }
            });

        this.store
            .select(selectConnectorIdFromRoute)
            .pipe(
                filter((connectorId) => connectorId != null),
                switchMap((connectorId) => {
                    this.loading = true;
                    this.frameSource = null;
                    this.connector = null;
                    this.errorMessage = null;
                    this.childConnectorUiReady = false;

                    return this.connectorConfigurationService.getConnector(connectorId!).pipe(
                        catchError((errorResponse: HttpErrorResponse) => {
                            this.loading = false;
                            this.errorMessage = this.errorHelper.getErrorString(errorResponse);
                            return of(null);
                        })
                    );
                }),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe((connector: ConnectorEntity | null) => {
                if (!connector) {
                    return;
                }

                this.connector = connector;
                this.loading = false;
                this.errorMessage = null;

                if (!connector.permissions.canRead || !connector.permissions.canWrite) {
                    this.errorMessage = 'Insufficient permissions to configure this connector.';
                    return;
                }

                if (connector.component?.configurationUrl) {
                    this.frameSource = this.getFrameSource(connector.component.configurationUrl);

                    this.connectorMessageHost.startListening({
                        destroyRef: this.destroyRef,
                        expectedOrigin: ConnectorMessageHost.extractOrigin(connector.component.configurationUrl),
                        iframeElement: () => this.iframeRef()?.nativeElement,
                        onConnectorUiReady: () => {
                            this.childConnectorUiReady = true;
                            this.postDisconnectedNodeAcknowledgmentToChild();
                        }
                    });
                }
            });
    }

    private postDisconnectedNodeAcknowledgmentToChild(): void {
        const iframe = this.iframeRef()?.nativeElement;
        const configurationUrl = this.connector?.component?.configurationUrl;
        if (!iframe?.contentWindow || !configurationUrl) {
            return;
        }
        const targetOrigin = ConnectorMessageHost.extractOrigin(configurationUrl);
        if (!targetOrigin) {
            return;
        }
        const message: ParentToConnectorMessage = {
            namespace: CONNECTOR_MESSAGE_NAMESPACE,
            type: 'disconnected-node-acknowledgment',
            payload: {
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged()
            }
        };
        iframe.contentWindow.postMessage(message, targetOrigin);
    }

    private getFrameSource(configurationUrl: string): SafeResourceUrl | null {
        const connectorId = this.connector?.id;
        const urlWithParams = connectorId ? `${configurationUrl}?connectorId=${connectorId}` : configurationUrl;

        const sanitizedUrl = this.domSanitizer.sanitize(SecurityContext.URL, urlWithParams);

        if (sanitizedUrl) {
            return this.domSanitizer.bypassSecurityTrustResourceUrl(sanitizedUrl);
        }

        return null;
    }

    returnToConnectorListing(): void {
        this.router.navigate(['/connectors']);
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
