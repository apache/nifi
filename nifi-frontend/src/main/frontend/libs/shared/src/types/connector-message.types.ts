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

// ========================
// Namespace constant
// ========================

export const CONNECTOR_MESSAGE_NAMESPACE = 'nifi-connector' as const;

// ========================
// Payload interfaces
// Single source of truth for each action's data shape, used by the
// ConnectorMessageClient (child), ConnectorMessageHost (parent),
// ConnectorConfigTelemetryService, and generic wizard effects.
// ========================

export interface NavigateToListingPayload {
    connectorId?: string;
}

/**
 * connectorId is `string | null` to stay compatible with NgRx selectors
 * on the parent side, which derive connectorId from route params.
 */
export interface StepSavedPayload {
    connectorId: string | null;
    stepName: string;
    stepIndex: number | null;
}

export interface StepSaveErrorPayload {
    connectorId: string | null;
    stepName: string;
    error: string;
}

export interface ConfigAppliedPayload {
    connectorId: string | null;
}

export interface ConfigApplyErrorPayload {
    connectorId: string | null;
    error: string;
}

export interface VerifyStartedPayload {
    connectorId: string | null;
    stepName: string;
}

export interface VerifyResultSummary {
    verificationStepName: string;
    outcome: string;
    subject: string | null;
}

export interface VerifySuccessPayload {
    connectorId: string | null;
    stepName: string;
    results: VerifyResultSummary[];
}

export interface VerifyErrorPayload {
    connectorId: string | null;
    stepName: string;
    error: string;
}

export interface DetailViewStartedPayload {
    connectorId: string | null;
    connectorType?: string;
}

/** Child notifies parent that the connector UI is ready to receive parent→child messages. */
export interface ConnectorUiReadyPayload {
    connectorId?: string;
}

/** Parent notifies child of cluster disconnected-node acknowledgment state (mutable NiFi requests). */
export interface DisconnectedNodeAcknowledgmentPayload {
    disconnectedNodeAcknowledged: boolean;
}

// ========================
// Message types
// Each message has a unique `type` discriminant and references one
// of the named payload interfaces above.
// ========================

interface ConnectorMessageBase {
    namespace: typeof CONNECTOR_MESSAGE_NAMESPACE;
}

export interface NavigateToListingMessage extends ConnectorMessageBase {
    type: 'navigate-to-connector-listing';
    payload: NavigateToListingPayload;
}

export interface StepSavedMessage extends ConnectorMessageBase {
    type: 'step-saved';
    payload: StepSavedPayload;
}

export interface StepSaveErrorMessage extends ConnectorMessageBase {
    type: 'step-save-error';
    payload: StepSaveErrorPayload;
}

export interface ConfigAppliedMessage extends ConnectorMessageBase {
    type: 'config-applied';
    payload: ConfigAppliedPayload;
}

export interface ConfigApplyErrorMessage extends ConnectorMessageBase {
    type: 'config-apply-error';
    payload: ConfigApplyErrorPayload;
}

export interface VerifyStartedMessage extends ConnectorMessageBase {
    type: 'verify-started';
    payload: VerifyStartedPayload;
}

export interface VerifySuccessMessage extends ConnectorMessageBase {
    type: 'verify-success';
    payload: VerifySuccessPayload;
}

export interface VerifyErrorMessage extends ConnectorMessageBase {
    type: 'verify-error';
    payload: VerifyErrorPayload;
}

export interface ConnectorUiReadyMessage extends ConnectorMessageBase {
    type: 'connector-ui-ready';
    payload: ConnectorUiReadyPayload;
}

// ========================
// Discriminated union
// ========================

export type ConnectorMessage =
    | NavigateToListingMessage
    | StepSavedMessage
    | StepSaveErrorMessage
    | ConfigAppliedMessage
    | ConfigApplyErrorMessage
    | VerifyStartedMessage
    | VerifySuccessMessage
    | VerifyErrorMessage
    | ConnectorUiReadyMessage;

/** Parent → iframe: current disconnected-node acknowledgment for NiFi mutating requests. */
export interface DisconnectedNodeAcknowledgmentMessage extends ConnectorMessageBase {
    type: 'disconnected-node-acknowledgment';
    payload: DisconnectedNodeAcknowledgmentPayload;
}

export type ParentToConnectorMessage = DisconnectedNodeAcknowledgmentMessage;

/** All valid `type` discriminant values for the ConnectorMessage union. */
const CONNECTOR_MESSAGE_TYPES: ReadonlySet<string> = new Set<ConnectorMessage['type']>([
    'navigate-to-connector-listing',
    'step-saved',
    'step-save-error',
    'config-applied',
    'config-apply-error',
    'verify-started',
    'verify-success',
    'verify-error',
    'connector-ui-ready'
]);

// ========================
// Type guard
// ========================

/**
 * Runtime type guard that validates an unknown postMessage payload
 * is a well-formed ConnectorMessage.
 */
export function isConnectorMessage(data: unknown): data is ConnectorMessage {
    if (data == null || typeof data !== 'object') {
        return false;
    }
    const msg = data as Record<string, unknown>;
    return (
        msg['namespace'] === CONNECTOR_MESSAGE_NAMESPACE &&
        typeof msg['type'] === 'string' &&
        CONNECTOR_MESSAGE_TYPES.has(msg['type'])
    );
}

const PARENT_TO_CONNECTOR_TYPES: ReadonlySet<string> = new Set<ParentToConnectorMessage['type']>([
    'disconnected-node-acknowledgment'
]);

/**
 * Validates parent → iframe messages (embedded connector UI).
 */
export function isParentToConnectorMessage(data: unknown): data is ParentToConnectorMessage {
    if (data == null || typeof data !== 'object') {
        return false;
    }
    const msg = data as Record<string, unknown>;
    if (
        msg['namespace'] !== CONNECTOR_MESSAGE_NAMESPACE ||
        typeof msg['type'] !== 'string' ||
        !PARENT_TO_CONNECTOR_TYPES.has(msg['type'])
    ) {
        return false;
    }
    const payload = msg['payload'];
    if (msg['type'] === 'disconnected-node-acknowledgment') {
        return (
            payload != null &&
            typeof payload === 'object' &&
            typeof (payload as DisconnectedNodeAcknowledgmentPayload).disconnectedNodeAcknowledged === 'boolean'
        );
    }
    return false;
}
