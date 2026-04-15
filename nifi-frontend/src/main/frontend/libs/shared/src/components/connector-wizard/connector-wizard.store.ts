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

import { Signal } from '@angular/core';
import { signalStore } from '@ngrx/signals';
import {
    ConfigurationStepConfiguration,
    ConfigVerificationResult,
    ConnectorEntity,
    ConnectorPropertyFormValue,
    AssetInfo,
    StepDocumentationState,
    ConnectorConfiguration,
    Secret,
    PropertyAllowableValuesState
} from '../../types';
import { withConnectorWizard } from './with-connector-wizard.feature';

// =============================
// Abstract class — the DI token shared components inject
// =============================

/**
 * Abstract base class that serves as the Angular DI token for the connector wizard store.
 * Inject this in shared wizard components. Concrete implementations are provided via
 * `{ provide: ConnectorWizardStore, useExisting: ConnectorWizardSignalStore }` (or a
 * connector-specific store) in the host component's `providers` array.
 */
export abstract class ConnectorWizardStore {
    // --------------- State signals ---------------
    abstract readonly connectorId: Signal<string | null>;
    abstract readonly connector: Signal<ConnectorEntity | null>;
    abstract readonly pendingStepAdvancement: Signal<boolean>;
    abstract readonly loading: Signal<boolean>;
    abstract readonly status: Signal<'pending' | 'configuring' | 'saving' | 'complete' | 'error'>;
    abstract readonly error: Signal<string | null>;
    abstract readonly verifying: Signal<boolean>;
    abstract readonly subjectVerificationErrors: Signal<Record<string, string>>;
    abstract readonly dynamicAllowableValues: Signal<Record<string, PropertyAllowableValuesState>>;
    abstract readonly bannerErrors: Signal<string[]>;
    abstract readonly assetsByProperty: Signal<Record<string, AssetInfo[]>>;
    abstract readonly documentationPanelOpen: Signal<boolean>;
    abstract readonly availableSecrets: Signal<Secret[] | null>;
    abstract readonly secretsLoading: Signal<boolean>;
    abstract readonly secretsError: Signal<string | null>;
    abstract readonly stepVerificationResults: Signal<Record<string, ConfigVerificationResult[]>>;
    abstract readonly completedSteps: Signal<Record<string, boolean>>;
    abstract readonly unsavedStepValues: Signal<Record<string, Record<string, ConnectorPropertyFormValue>>>;

    // --------------- Computed signals ---------------
    abstract readonly visibleStepNames: Signal<string[]>;
    abstract readonly workingConfiguration: Signal<ConnectorConfiguration | null>;
    abstract readonly applying: Signal<boolean>;
    abstract readonly allStepsVerifying: Signal<boolean>;
    abstract readonly verificationPassed: Signal<boolean | null>;
    abstract readonly currentVerifyingStepName: Signal<string | null>;
    abstract readonly verifyAllError: Signal<string | null>;

    // --------------- Per-step signal factories ---------------
    abstract stepConfiguration(stepName: string): Signal<ConfigurationStepConfiguration | null>;
    abstract stepSaving(stepName: string): Signal<boolean>;
    abstract stepCompleted(stepName: string): Signal<boolean>;
    abstract documentationForStep(stepName: string): Signal<StepDocumentationState | null>;
    abstract generalVerificationErrorsForStep(stepName: string): Signal<string[]>;

    // --------------- State mutation methods ---------------
    abstract initializeWithConnector(connector: ConnectorEntity): void;
    abstract updateConnector(connector: ConnectorEntity): void;
    abstract resetState(): void;
    abstract changeStep(stepIndex: number): void;
    abstract clearPendingStepAdvancement(): void;
    abstract markStepDirty(data: { stepName: string; isDirty: boolean }): void;
    abstract updateUnsavedStepValues(data: {
        stepName: string;
        values: Record<string, ConnectorPropertyFormValue>;
    }): void;
    abstract clearUnsavedStepValues(stepName: string): void;
    abstract setLoading(loading: boolean): void;
    abstract toggleDocumentationPanel(open: boolean): void;
    abstract clearBannerErrors(): void;
    abstract clearSubjectVerificationError(propertyName: string): void;
    abstract setStepVerificationResults(data: { stepName: string; results: ConfigVerificationResult[] }): void;
    abstract enterSummaryStep(): void;
    abstract addAsset(data: { propertyName: string; asset: AssetInfo; uploadId: number }): void;
    abstract removeAsset(data: { propertyName: string; assetId: string }): void;
    abstract clearAssetsForProperty(propertyName: string): void;
    abstract setDisconnectedNodeAcknowledged(ack: boolean): void;
    abstract initializeAssets(propertyAssets: Record<string, AssetInfo[]>): void;
    abstract addBannerError(message: string): void;

    // --------------- Effect methods (rxMethod-based) ---------------
    abstract loadConnector(connectorId: string): void;
    abstract loadSecrets(connectorId: string): void;
    abstract saveStep(data: { stepName: string; stepConfiguration: ConfigurationStepConfiguration }): void;
    abstract saveAndClose(data: { stepName: string; stepConfiguration: ConfigurationStepConfiguration | null }): void;
    abstract advanceWithoutSaving(stepName: string): void;
    abstract verifyStep(data: { stepName: string; configuration: ConfigurationStepConfiguration }): void;
    abstract verifyAllSteps(): void;
    abstract applyConfiguration(): void;
    abstract refreshConnectorForSummary(): void;
    abstract loadStepDocumentation(stepName: string): void;
    abstract fetchPropertyAllowableValues(data: {
        stepName: string;
        propertyGroupName: string;
        propertyName: string;
        filter?: string;
    }): void;
}

// =============================
// Concrete SignalStore (default implementation)
// =============================

/**
 * Standard concrete implementation of the connector wizard store. Suitable for any
 * connector that requires no custom step visibility or lifecycle logic.
 * Provide alongside `{ provide: ConnectorWizardStore, useExisting: StandardConnectorWizardStore }` in the
 * host component's `providers` array. For app-specific step visibility or lifecycle logic, compose
 * `withConnectorWizard()` directly in a custom store instead.
 */
export const StandardConnectorWizardStore = signalStore(withConnectorWizard());

export type StandardConnectorWizardStore = InstanceType<typeof StandardConnectorWizardStore>;
