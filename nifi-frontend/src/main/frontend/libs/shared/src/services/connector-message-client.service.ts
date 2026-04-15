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

import { Injectable } from '@angular/core';
import { Observable, fromEvent } from 'rxjs';
import { distinctUntilChanged, filter, map } from 'rxjs/operators';
import {
    CONNECTOR_MESSAGE_NAMESPACE,
    ConnectorMessage,
    ConnectorUiReadyPayload,
    NavigateToListingPayload,
    ParentToConnectorMessage,
    StepSavedPayload,
    StepSaveErrorPayload,
    ConfigAppliedPayload,
    ConfigApplyErrorPayload,
    VerifyStartedPayload,
    VerifySuccessPayload,
    VerifyErrorPayload,
    isParentToConnectorMessage
} from '../types/connector-message.types';

/**
 * Lightweight client service for custom connector UIs to communicate
 * actions back to the parent runtime application via postMessage.
 *
 * Provides action-specific typed methods that mirror the parent-side
 * ConnectorConfigTelemetryService API, making the available actions
 * fully discoverable via autocomplete.
 *
 * The parent decides what to do with each message -- telemetry tracking,
 * navigation, state updates, etc. Custom UIs remain agnostic to those
 * concerns.
 *
 * Usage:
 * ```typescript
 * private messageClient = inject(ConnectorMessageClient);
 *
 * onSave(): void {
 *     this.messageClient.trackStepSaved({
 *         connectorId: this.connectorId,
 *         stepName: 'Connection Details',
 *         stepIndex: 0
 *     });
 * }
 *
 * onCancel(): void {
 *     this.messageClient.navigateToListing({ connectorId: this.connectorId });
 * }
 * ```
 */
@Injectable({
    providedIn: 'root'
})
export class ConnectorMessageClient {
    // ========================
    // Navigation
    // ========================

    /** Request the parent to navigate back to the connector listing. */
    navigateToListing(payload: NavigateToListingPayload = {}): void {
        this.send({ namespace: CONNECTOR_MESSAGE_NAMESPACE, type: 'navigate-to-connector-listing', payload });
    }

    // ========================
    // Events
    // ========================

    /** Report that a configuration step was successfully saved. */
    trackStepSaved(payload: StepSavedPayload): void {
        this.send({ namespace: CONNECTOR_MESSAGE_NAMESPACE, type: 'step-saved', payload });
    }

    /** Report that a configuration step save failed. */
    trackStepSaveError(payload: StepSaveErrorPayload): void {
        this.send({ namespace: CONNECTOR_MESSAGE_NAMESPACE, type: 'step-save-error', payload });
    }

    /** Report that the full configuration was applied successfully. */
    trackConfigApplied(payload: ConfigAppliedPayload): void {
        this.send({ namespace: CONNECTOR_MESSAGE_NAMESPACE, type: 'config-applied', payload });
    }

    /** Report that applying the configuration failed. */
    trackConfigApplyError(payload: ConfigApplyErrorPayload): void {
        this.send({ namespace: CONNECTOR_MESSAGE_NAMESPACE, type: 'config-apply-error', payload });
    }

    /** Report that verification was initiated. */
    trackVerifyStarted(payload: VerifyStartedPayload): void {
        this.send({ namespace: CONNECTOR_MESSAGE_NAMESPACE, type: 'verify-started', payload });
    }

    /** Report that verification completed successfully. */
    trackVerifySuccess(payload: VerifySuccessPayload): void {
        this.send({ namespace: CONNECTOR_MESSAGE_NAMESPACE, type: 'verify-success', payload });
    }

    /** Report that verification failed due to a network/API error. */
    trackVerifyError(payload: VerifyErrorPayload): void {
        this.send({ namespace: CONNECTOR_MESSAGE_NAMESPACE, type: 'verify-error', payload });
    }

    /**
     * Notify the parent runtime that this connector UI is ready to receive
     * parent→child messages (e.g. disconnected-node acknowledgment).
     * Call after installing listeners for {@link disconnectedNodeAcknowledgment$}.
     *
     * No-ops when `document.referrer` cannot be parsed to a target origin, so we
     * never post with a wildcard `targetOrigin`.
     */
    notifyConnectorUiReady(payload: ConnectorUiReadyPayload = {}): void {
        if (!this.isEmbedded()) {
            return;
        }
        let targetOrigin: string;
        try {
            targetOrigin = new URL(document.referrer).origin;
        } catch {
            return;
        }
        const message = {
            namespace: CONNECTOR_MESSAGE_NAMESPACE,
            type: 'connector-ui-ready' as const,
            payload
        };
        window.parent.postMessage(message, targetOrigin);
    }

    /**
     * Stream of `disconnectedNodeAcknowledged` values from the parent runtime.
     * Only emits when the message passes origin, payload, and source (`window.parent`) checks.
     */
    disconnectedNodeAcknowledgment$(): Observable<boolean> {
        return fromEvent<MessageEvent>(window, 'message').pipe(
            filter((event: MessageEvent) => {
                let expectedOrigin: string;
                try {
                    expectedOrigin = new URL(document.referrer).origin;
                } catch {
                    return false;
                }
                return (
                    event.origin === expectedOrigin &&
                    event.source === window.parent &&
                    isParentToConnectorMessage(event.data)
                );
            }),
            map((event: MessageEvent) => (event.data as ParentToConnectorMessage).payload.disconnectedNodeAcknowledged),
            distinctUntilChanged()
        );
    }

    // ========================
    // Utilities
    // ========================

    /**
     * Check whether the application is running embedded (e.g. inside an
     * iframe). When running standalone during development the service
     * gracefully no-ops all sends.
     */
    isEmbedded(): boolean {
        try {
            return window.self !== window.parent;
        } catch {
            // Cross-origin restriction -- we are embedded
            return true;
        }
    }

    // ========================
    // Internal
    // ========================

    private send(message: ConnectorMessage): void {
        if (!this.isEmbedded()) {
            return;
        }
        let targetOrigin: string;
        try {
            targetOrigin = new URL(document.referrer).origin;
        } catch {
            return;
        }
        window.parent.postMessage(message, targetOrigin);
    }
}
