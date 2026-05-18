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

import {
    Component,
    computed,
    contentChildren,
    DestroyRef,
    input,
    Injector,
    OnDestroy,
    OnInit,
    output,
    TemplateRef,
    viewChild,
    ChangeDetectorRef,
    inject
} from '@angular/core';
import { takeUntilDestroyed, toObservable } from '@angular/core/rxjs-interop';
import { NgTemplateOutlet } from '@angular/common';
import { StepperSelectionEvent } from '@angular/cdk/stepper';
import { MatButton } from '@angular/material/button';
import { MatStep, MatStepLabel, MatStepContent, MatStepperIcon } from '@angular/material/stepper';
import { filter } from 'rxjs/operators';
import { Wizard, BeforeStepChangeEvent } from '../wizard/wizard.component';
import { ActiveStepService } from '../../services/active-step.service';
import { ConnectorConfigurationSummaryStep } from '../connector-configuration-summary-step/connector-configuration-summary-step.component';
import { ConfigurationStepConfiguration, ConnectorConfiguration, ConnectorEntity } from '../../types';
import { ConnectorWizardStore } from './connector-wizard.store';
import { WizardCustomStepDirective } from './wizard-custom-step.directive';
import { SharedConnectorConfigurationStep } from './connector-configuration-step/connector-configuration-step.component';

@Component({
    selector: 'connector-wizard',
    standalone: true,
    imports: [
        NgTemplateOutlet,
        MatButton,
        MatStep,
        MatStepLabel,
        MatStepContent,
        MatStepperIcon,
        Wizard,
        SharedConnectorConfigurationStep,
        ConnectorConfigurationSummaryStep
    ],
    providers: [ActiveStepService],
    templateUrl: './connector-wizard.component.html',
    styleUrl: './connector-wizard.component.scss',
    host: {
        class: 'w-full h-full'
    }
})
export class ConnectorWizard implements OnInit, OnDestroy {
    wizardStore = inject(ConnectorWizardStore);
    private cdr = inject(ChangeDetectorRef);
    private destroyRef = inject(DestroyRef);
    private injector = inject(Injector);
    private activeStepService = inject(ActiveStepService);

    /**
     * Tracks the target step index for deferred navigation after a save completes.
     * When null, the default behavior is to advance forward (wizard.next()).
     * When set, navigation goes to the specific index (for back button and header clicks).
     * Reset to null after each navigation is handled.
     */
    private pendingTargetIndex: number | null = null;

    readonly connector = input<ConnectorEntity | null>(null);
    readonly summaryLabel = input('Review & Apply');
    readonly summaryConfiguration = input<ConnectorConfiguration | null>(null);
    readonly stepNameMapping = input<{ [displayName: string]: string[] }>({});
    readonly preWizardContent = input<TemplateRef<unknown> | null>(null);

    readonly summaryVisibleStepNames = computed(() => {
        const curated = this.summaryConfiguration();
        if (curated) {
            return curated.configurationStepConfigurations.map((s) => s.configurationStepName);
        }
        return this.wizardStore.visibleStepNames();
    });

    readonly navigateBack = output<void>();

    readonly wizard = viewChild<Wizard>('wizard');
    readonly customStepDirectives = contentChildren(WizardCustomStepDirective);

    private customStepMap = computed(() => {
        const map = new Map<string, TemplateRef<{ $implicit: string }>>();
        for (const dir of this.customStepDirectives()) {
            map.set(dir.stepName(), dir.templateRef);
        }
        return map;
    });

    ngOnInit(): void {
        const connector = this.connector();
        if (connector) {
            this.wizardStore.initializeWithConnector(connector);
            this.wizardStore.loadSecrets(connector.id);
        }

        toObservable(this.wizardStore.pendingStepAdvancement, { injector: this.injector })
            .pipe(
                filter((pending) => pending === true),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe(() => {
                this.cdr.detectChanges();

                const wiz = this.wizard();
                if (wiz) {
                    if (this.pendingTargetIndex !== null) {
                        wiz.goToStep(this.pendingTargetIndex);
                    } else {
                        wiz.next();
                    }
                }

                this.pendingTargetIndex = null;
                this.wizardStore.clearPendingStepAdvancement();
            });

        toObservable(this.wizardStore.bannerErrors, { injector: this.injector })
            .pipe(
                filter((errors) => errors.length > 0),
                takeUntilDestroyed(this.destroyRef)
            )
            .subscribe(() => {
                this.pendingTargetIndex = null;
            });
    }

    ngOnDestroy(): void {
        this.wizardStore.resetState();
    }

    getCustomStepTemplate(stepName: string): TemplateRef<{ $implicit: string }> | null {
        return this.customStepMap().get(stepName) ?? null;
    }

    onStepChange(event: StepperSelectionEvent, stepNames: string[]): void {
        this.wizardStore.changeStep(event.selectedIndex);

        if (event.selectedIndex === stepNames.length) {
            this.wizardStore.enterSummaryStep();
            this.wizardStore.refreshConnectorForSummary();
        }
    }

    onBeforeStepChange(event: BeforeStepChangeEvent): void {
        const activeStep = this.activeStepService.activeStep;
        if (activeStep) {
            const { configuration, isDirty } = activeStep.getConfigurationForSave();
            if (isDirty && configuration) {
                this.pendingTargetIndex = event.targetIndex;
                this.wizardStore.saveStep({
                    stepName: activeStep.stepName(),
                    stepConfiguration: configuration
                });
                return;
            }
        }

        this.wizard()?.goToStep(event.targetIndex);
    }

    onPreviousStep(): void {
        this.wizard()?.previous();
    }

    onVerifyAllSteps(): void {
        this.wizardStore.verifyAllSteps();
    }

    onApplyConfiguration(): void {
        this.wizardStore.applyConfiguration();
    }

    onBackStep(data: { stepName: string; configuration: ConfigurationStepConfiguration | null }): void {
        if (data.configuration) {
            const wiz = this.wizard();
            this.pendingTargetIndex = wiz ? wiz.selectedIndex - 1 : null;
            this.wizardStore.saveStep({
                stepName: data.stepName,
                stepConfiguration: data.configuration
            });
        } else {
            this.wizard()?.previous();
        }
    }
}
