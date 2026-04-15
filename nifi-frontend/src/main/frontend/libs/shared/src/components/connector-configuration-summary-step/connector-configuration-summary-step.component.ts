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

import { Component, computed, input, output } from '@angular/core';

import { MatButton } from '@angular/material/button';
import { MatCard } from '@angular/material/card';
import { MatTooltip } from '@angular/material/tooltip';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { NifiSpinnerDirective } from '../../directives/spinner/nifi-spinner.directive';
import { PropertyGroupCard } from '../property-group-card/property-group-card.component';
import { ErrorBanner } from '../error-banner/error-banner.component';
import { ConnectorConfiguration, ConnectorPropertyDescriptor, ConfigVerificationResult } from '../../types';
import {
    hasPropertyValue,
    filterPropertyVisibility,
    assetReferencesHaveMissingContent
} from '../../utils/connector-validation.utils';

@Component({
    selector: 'connector-configuration-summary-step',
    standalone: true,
    imports: [
        MatButton,
        MatCard,
        MatTooltip,
        NgxSkeletonLoaderModule,
        NifiSpinnerDirective,
        PropertyGroupCard,
        ErrorBanner
    ],
    templateUrl: './connector-configuration-summary-step.component.html',
    styleUrl: './connector-configuration-summary-step.component.scss'
})
export class ConnectorConfigurationSummaryStep {
    // Signal inputs
    workingConfiguration = input.required<ConnectorConfiguration | null>();
    visibleStepNames = input<string[]>([]);
    loading = input(false);
    applying = input(false);
    verifying = input(false);
    verificationPassed = input<boolean | null>(null);
    currentVerifyingStepName = input<string | null>(null);
    stepVerificationResults = input<{ [stepName: string]: ConfigVerificationResult[] }>({});
    stepNameMapping = input<{ [displayName: string]: string[] }>({});
    verifyAllError = input<string | null>(null);

    // Signal outputs
    confirm = output<void>();
    dismiss = output<void>();
    previous = output<void>();
    verify = output<void>();

    applyDisabled = computed(() => {
        return this.loading() || this.applying() || this.verifying() || this.verificationPassed() !== true;
    });

    applyTooltip = computed(() => {
        if (this.verificationPassed() !== true) {
            return 'Run verification before applying';
        }
        return '';
    });

    // Skeleton iteration helpers
    readonly skeletonSteps = [1, 2, 3];
    readonly skeletonProperties = [1, 2, 3, 4, 5, 6];

    /**
     * Computed signal that returns only visible steps based on visibleStepNames.
     * If visibleStepNames is empty, all steps are shown (backwards compatibility).
     */
    visibleSteps = computed(() => {
        const config = this.workingConfiguration();
        const visibleNames = this.visibleStepNames();

        if (!config?.configurationStepConfigurations) {
            return [];
        }

        // If no visible step names provided, show all steps
        if (!visibleNames || visibleNames.length === 0) {
            return config.configurationStepConfigurations;
        }

        // Filter to only visible steps, preserving order from visibleStepNames
        const stepsByName = new Map(
            config.configurationStepConfigurations.map((step) => [step.configurationStepName, step])
        );

        return visibleNames.map((name) => stepsByName.get(name)).filter((step) => step !== undefined);
    });

    /**
     * Computed signal that returns visible steps with property groups filtered to only visible properties.
     * Property visibility is scoped to each step (dependencies can only reference properties within the same step).
     * This is what the template should iterate over for rendering.
     */
    filteredVisibleSteps = computed(() => {
        const steps = this.visibleSteps();
        if (!steps || steps.length === 0) {
            return [];
        }
        return filterPropertyVisibility(steps);
    });

    /**
     * Computed signal that checks if any required fields are missing values.
     * Only checks visible steps and properties that are visible based on their dependencies.
     * Only recalculates when workingConfiguration or visibleStepNames changes.
     */
    hasMissingRequiredFields = computed(() => {
        const steps = this.filteredVisibleSteps();
        if (!steps || steps.length === 0) {
            return false;
        }

        for (const step of steps) {
            for (const group of step.propertyGroupConfigurations) {
                for (const [name, descriptor] of Object.entries(group.propertyDescriptors)) {
                    const typedDescriptor = descriptor as ConnectorPropertyDescriptor;
                    if (typedDescriptor.required) {
                        const valueRef = group.propertyValues?.[name];
                        if (!hasPropertyValue(valueRef, typedDescriptor.type, typedDescriptor.defaultValue)) {
                            return true;
                        }
                        if (assetReferencesHaveMissingContent(valueRef, typedDescriptor.type)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    });

    private resolveStepNames(stepName: string): string[] {
        return this.stepNameMapping()[stepName] ?? [stepName];
    }

    getStepVerificationStatus(stepName: string): 'verifying' | 'passed' | 'failed' | 'skipped' | null {
        const sourceNames = this.resolveStepNames(stepName);

        const currentStep = this.currentVerifyingStepName();

        if (this.verifying() && currentStep && sourceNames.includes(currentStep)) {
            return 'verifying';
        }

        const results = this.stepVerificationResults();
        const allResults = sourceNames.flatMap((name) => results[name] ?? []);
        const anyDefined = sourceNames.some((name) => results[name] !== undefined);

        if (!anyDefined) {
            return this.verificationPassed() !== null ? 'skipped' : null;
        }

        return allResults.length === 0 ? 'passed' : 'failed';
    }

    getStepErrors(stepName: string): ConfigVerificationResult[] {
        const results = this.stepVerificationResults();
        return this.resolveStepNames(stepName).flatMap((name) => results[name] ?? []);
    }

    getGeneralErrorMessages(stepName: string): string[] {
        const results = this.getStepErrors(stepName);
        const steps = this.filteredVisibleSteps();
        const step = steps.find((s) => s.configurationStepName === stepName);
        const allPropertyNames = new Set(
            step?.propertyGroupConfigurations.flatMap((g) => Object.keys(g.propertyDescriptors || {})) ?? []
        );

        return results
            .filter((r) => r.outcome === 'FAILED' && (!r.subject || !allPropertyNames.has(r.subject)))
            .map((r) => r.explanation || 'Verification failed');
    }

    onVerify(): void {
        this.verify.emit();
    }

    onConfirm(): void {
        this.confirm.emit();
    }

    onDismiss(): void {
        this.dismiss.emit();
    }

    onPrevious(): void {
        this.previous.emit();
    }
}
