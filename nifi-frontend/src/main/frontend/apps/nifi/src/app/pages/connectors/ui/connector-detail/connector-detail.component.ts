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

import { Component, DestroyRef, ElementRef, OnInit, viewChild, inject } from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import {
    SystemTokensService,
    ConnectorEntity,
    ConnectorConfigurationService,
    NifiSpinnerDirective
} from '@nifi/shared';
import { ConnectorDetailsContent } from '../connector-details-content/connector-details-content.component';
import { catchError, filter, switchMap } from 'rxjs/operators';
import { selectConnectorIdFromRoute } from '../../state/connectors-listing/connectors-listing.selectors';
import { MatButton } from '@angular/material/button';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ContextErrorBanner } from '../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorContextKey } from '../../../../state/error';
import { selectBackNavigation } from '../../../../state/navigation/navigation.selectors';
import { of } from 'rxjs';
import { ConnectorMessageHost } from '../../service/connector-message-host.service';

@Component({
    selector: 'connector-detail',
    imports: [Navigation, ConnectorDetailsContent, MatButton, NifiSpinnerDirective, ContextErrorBanner],
    templateUrl: './connector-detail.component.html',
    styleUrls: ['./connector-detail.component.scss']
})
export class ConnectorDetail implements OnInit {
    private store = inject<Store<NiFiState>>(Store);
    private router = inject(Router);
    private domSanitizer = inject(DomSanitizer);
    private connectorConfigurationService = inject(ConnectorConfigurationService);
    private destroyRef = inject(DestroyRef);
    private errorHelper = inject(ErrorHelper);
    protected systemTokensService = inject(SystemTokensService);
    private connectorMessageHost = inject(ConnectorMessageHost);

    readonly iframeRef = viewChild<ElementRef<HTMLIFrameElement>>('iframeRef');

    backNavigation = this.store.selectSignal(selectBackNavigation);
    connectorIdFromRoute = this.store.selectSignal(selectConnectorIdFromRoute);
    frameSource: SafeResourceUrl | null = null;
    connector: ConnectorEntity | null = null;
    loading = true;
    errorMessage: string | null = null;

    protected readonly ErrorContextKey = ErrorContextKey;

    ngOnInit(): void {
        this.store
            .select(selectConnectorIdFromRoute)
            .pipe(
                filter((connectorId) => connectorId != null),
                switchMap((connectorId) => {
                    this.connectorMessageHost.stopListening();
                    this.loading = true;
                    this.frameSource = null;
                    this.connector = null;
                    this.errorMessage = null;

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

                if (!connector.permissions.canRead) {
                    this.errorMessage = 'Insufficient permissions to view this connector.';
                    return;
                }

                if (connector.component?.detailsUrl) {
                    this.frameSource = this.getFrameSource(connector.component.detailsUrl);

                    this.connectorMessageHost.startListening({
                        destroyRef: this.destroyRef,
                        expectedOrigin: ConnectorMessageHost.extractOrigin(connector.component.detailsUrl),
                        iframeElement: () => this.iframeRef()?.nativeElement
                    });
                }
            });
    }

    private getFrameSource(detailsUrl: string): SafeResourceUrl | null {
        const connectorId = this.connector?.id;
        const urlWithParams = connectorId ? `${detailsUrl}?connectorId=${connectorId}` : detailsUrl;

        try {
            const parsed = new URL(urlWithParams);
            if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
                return null;
            }
        } catch {
            return null;
        }

        return this.domSanitizer.bypassSecurityTrustResourceUrl(urlWithParams);
    }

    returnToConnectorListing(): void {
        this.router.navigate(['/connectors']);
    }
}
