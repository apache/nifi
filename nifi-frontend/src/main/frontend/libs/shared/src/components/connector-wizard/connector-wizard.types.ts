/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { InjectionToken } from '@angular/core';
import {
    ConnectorEntity,
    ConnectorPropertyFormValue,
    Revision,
    Secret,
    StepDocumentationState,
    ConfigurationStepConfiguration,
    ConnectorConfigStepVerificationRequest,
    ConfigVerificationResult,
    AssetInfo,
    PropertyAllowableValuesState
} from '../../types';

/**
 * Creates a unique key for identifying a property in the dynamic allowable values map.
 * @param stepName The configuration step name
 * @param propertyGroupName The property group name
 * @param propertyName The property name
 * @returns A unique key string
 */
export function createPropertyKey(stepName: string, propertyGroupName: string, propertyName: string): string {
    return `${stepName}::${propertyGroupName}::${propertyName}`;
}

/**
 * State for the connector configuration wizard
 */
export interface ConnectorWizardState {
    // Core connector data
    connectorId: string | null;
    connector: ConnectorEntity | null;

    // Parent connector revision (updated after each step save)
    parentConnectorRevision: Revision | null;

    // Configuration metadata
    stepNames: string[];

    // Individual step configurations (loaded from workingConfiguration on init)
    stepConfigurations: Record<string, ConfigurationStepConfiguration>;

    // Currently active step
    currentStepIndex: number;

    // Saving states for individual steps
    savingSteps: Record<string, boolean>;

    // Completed steps (tracks which steps have been successfully saved)
    completedSteps: Record<string, boolean>;

    // Dirty states for individual steps (tracks unsaved changes)
    dirtySteps: Record<string, boolean>;

    // Unsaved form values per step (preserves user input across navigation)
    unsavedStepValues: Record<string, Record<string, ConnectorPropertyFormValue>>;

    // Pending step advancement (set by store effects, consumed by component)
    pendingStepAdvancement: boolean;

    // Generic loading flag for async operations (connector fetch, etc.)
    loading: boolean;

    // Overall wizard status
    status: 'pending' | 'configuring' | 'saving' | 'complete' | 'error';
    error: string | null;

    // Available secrets for SECRET type properties
    // Loaded once when the wizard opens
    availableSecrets: Secret[] | null;
    secretsLoading: boolean;
    secretsError: string | null;

    // Step documentation panel state (per step)
    /** Keyed by step name. Use documentationForStep(stepName) to get a signal for a single step. */
    stepDocumentation: Record<string, StepDocumentationState>;

    // Whether the documentation panel is open
    documentationPanelOpen: boolean;

    // Active verification request (for async polling)
    activeVerificationRequest: ConnectorConfigStepVerificationRequest | null;

    // Verification state
    verifying: boolean;

    // Subject verification errors (errors with a subject field indicating the property)
    // Map of property name → error message for inline display
    subjectVerificationErrors: Record<string, string>;

    allStepsVerification: {
        verifying: boolean;
        passed: boolean | null;
        currentStepName: string | null;
        failedStepName: string | null;
        error: string | null;
    };

    stepVerificationResults: Record<string, ConfigVerificationResult[]>;

    // Dynamic allowable values per property (for allowableValuesFetchable properties)
    // Key format: `${stepName}::${propertyGroupName}::${propertyName}`
    dynamicAllowableValues: Record<string, PropertyAllowableValuesState>;

    bannerErrors: string[];
    assetsByProperty: Record<string, AssetInfo[]>;
    processedUploadIds: number[];

    /** Set via setDisconnectedNodeAcknowledged(); combined with config.getDisconnectedNodeAcknowledged in saveStep. */
    disconnectedNodeAcknowledged: boolean;
}

export const initialConnectorWizardState: ConnectorWizardState = {
    connectorId: null,
    connector: null,
    parentConnectorRevision: null,
    stepNames: [],
    stepConfigurations: {},
    currentStepIndex: 0,
    savingSteps: {},
    completedSteps: {},
    dirtySteps: {},
    unsavedStepValues: {},
    pendingStepAdvancement: false,
    loading: false,
    status: 'pending',
    error: null,
    availableSecrets: null,
    secretsLoading: false,
    secretsError: null,
    stepDocumentation: {},
    documentationPanelOpen: true,
    activeVerificationRequest: null,
    verifying: false,
    subjectVerificationErrors: {},
    allStepsVerification: {
        verifying: false,
        passed: null,
        currentStepName: null,
        failedStepName: null,
        error: null
    },
    stepVerificationResults: {},
    dynamicAllowableValues: {},
    bannerErrors: [],
    assetsByProperty: {},
    processedUploadIds: [],
    disconnectedNodeAcknowledged: false
};

export interface CustomStepRegistration {
    stepName: string;
    label?: string;
}

export interface ConnectorWizardConfig {
    customSteps?: CustomStepRegistration[];
    getDisconnectedNodeAcknowledged?: () => boolean;
    onApplySuccess?: (connectorId: string | null) => void;
    onNavigateBack?: (connectorId: string | null) => void;
    connectorMetadata?: {
        group: string;
        artifact: string;
        version: string;
        type: string;
    };
}

export const CONNECTOR_WIZARD_CONFIG = new InjectionToken<ConnectorWizardConfig>('CONNECTOR_WIZARD_CONFIG');
