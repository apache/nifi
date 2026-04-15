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

import { DestroyRef, Injectable, inject } from '@angular/core';
import { Router } from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Subscription, fromEvent } from 'rxjs';
import { filter } from 'rxjs/operators';
import { ConnectorMessage, ConnectorUiReadyMessage, isConnectorMessage } from '@nifi/shared';

export interface ConnectorMessageHostOptions {
    /** Angular DestroyRef for automatic subscription cleanup. */
    destroyRef: DestroyRef;

    /**
     * The expected origin of the iframe (e.g. `'https://connector-ui.example.com'`).
     * Messages whose `event.origin` does not match are silently dropped.
     */
    expectedOrigin: string;

    /**
     * Optional lazy getter for the HTMLIFrameElement hosting the custom UI.
     * When provided, the host verifies that `event.source` matches the
     * iframe's `contentWindow`, preventing spoofing from other windows
     * on the same origin.
     *
     * A getter is used because the iframe may not yet be in the DOM when
     * `startListening()` is called -- Angular renders the element after
     * the next change-detection cycle.
     */
    iframeElement?: () => HTMLIFrameElement | undefined;

    /**
     * Invoked when the iframe sends `connector-ui-ready` (listener is up).
     * Parent should respond with the initial `disconnected-node-acknowledgment` message.
     */
    onConnectorUiReady?: (event: MessageEvent) => void;
}

/**
 * Parent-side service that listens for postMessage events from embedded
 * custom connector UIs and routes them to the appropriate handler.
 *
 * Navigation messages use the Angular Router.
 *
 * **Security:** Messages are validated by origin, source (when an iframe
 * reference is available), namespace, and message-type before processing.
 *
 * Lifecycle is managed by the consuming component -- call `startListening()`
 * in ngOnInit and rely on `DestroyRef` for automatic cleanup.
 */
@Injectable({
    providedIn: 'root'
})
export class ConnectorMessageHost {
    private router = inject(Router);
    private activeSubscription: Subscription | null = null;

    /**
     * Extract the origin from a URL string for use with `expectedOrigin`.
     * Returns an empty string when the URL cannot be parsed, which will
     * cause all origin checks to fail-closed (no messages accepted).
     */
    static extractOrigin(url: string): string {
        try {
            return new URL(url).origin;
        } catch {
            return '';
        }
    }

    /**
     * Begin listening for postMessage events.
     *
     * If a previous listener is active it is torn down first, so callers
     * can safely call this method again when the route changes without
     * accumulating duplicate subscriptions.
     *
     * Only messages that pass all of the following checks are processed:
     * 1. `event.origin` matches `expectedOrigin`
     * 2. `event.source` matches the iframe's `contentWindow` (when `iframeElement` is provided)
     * 3. `isConnectorMessage()` type guard passes (namespace + type validation)
     *
     * The subscription is automatically torn down when the provided
     * DestroyRef fires (component destroy).
     */
    startListening(options: ConnectorMessageHostOptions): void {
        this.stopListening();

        this.activeSubscription = fromEvent<MessageEvent>(window, 'message')
            .pipe(
                filter((event) => this.isAllowedSource(event, options)),
                filter((event) => isConnectorMessage(event.data)),
                takeUntilDestroyed(options.destroyRef)
            )
            .subscribe((event) => {
                const data = event.data as ConnectorMessage;
                if (data.type === 'connector-ui-ready') {
                    options.onConnectorUiReady?.(event);
                    return;
                }
                this.handleMessage(data);
            });
    }

    /**
     * Tear down the active message listener, if any.
     */
    stopListening(): void {
        if (this.activeSubscription) {
            this.activeSubscription.unsubscribe();
            this.activeSubscription = null;
        }
    }

    /**
     * Validate that the MessageEvent originates from the expected iframe.
     */
    private isAllowedSource(event: MessageEvent, options: ConnectorMessageHostOptions): boolean {
        if (event.origin !== options.expectedOrigin) {
            return false;
        }

        if (options.iframeElement) {
            const iframe = options.iframeElement();
            if (iframe && event.source !== iframe.contentWindow) {
                return false;
            }
        }

        return true;
    }

    private handleMessage(message: Exclude<ConnectorMessage, ConnectorUiReadyMessage>): void {
        switch (message.type) {
            case 'navigate-to-connector-listing': {
                const connectorId = message.payload.connectorId;
                if (connectorId) {
                    this.router.navigate(['/connectors', connectorId]);
                } else {
                    this.router.navigate(['/connectors']);
                }
                break;
            }

            // Event messages are acknowledged but not acted on. These are
            // extensibility points for future telemetry or logging integrations.
            case 'step-saved':
            case 'step-save-error':
            case 'config-applied':
            case 'config-apply-error':
            case 'verify-started':
            case 'verify-success':
            case 'verify-error':
                break;

            default: {
                const _exhaustive: never = message;
                return _exhaustive;
            }
        }
    }
}
