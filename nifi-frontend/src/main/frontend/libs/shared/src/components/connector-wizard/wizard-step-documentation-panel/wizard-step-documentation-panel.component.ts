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

import { Component, computed, effect, input, output, inject } from '@angular/core';
import { MarkdownComponent } from 'ngx-markdown';
import { MatIconButton } from '@angular/material/button';
import { MatProgressSpinner } from '@angular/material/progress-spinner';
import { StepDocumentationState } from '../../../types';
import { ConnectorWizardStore } from '../connector-wizard.store';

/**
 * Step documentation panel that reads from ConnectorWizardStore and renders markdown via ngx-markdown.
 */
@Component({
    selector: 'wizard-step-documentation-panel',
    standalone: true,
    imports: [MarkdownComponent, MatIconButton, MatProgressSpinner],
    templateUrl: './wizard-step-documentation-panel.component.html',
    styleUrl: './wizard-step-documentation-panel.component.scss'
})
export class WizardStepDocumentationPanel {
    private wizardStore = inject(ConnectorWizardStore);

    readonly stepName = input.required<string>();
    readonly isOpen = input(false);
    readonly panelTitle = input<string | undefined>();

    readonly closePanel = output<void>();

    private subscribedStepName: string | null = null;

    readonly resolvedPanelTitle = computed(() => this.panelTitle() ?? 'Documentation');

    readonly documentationState = computed((): StepDocumentationState | null => {
        const name = this.stepName();
        if (!name) return null;
        return this.wizardStore.documentationForStep(name)();
    });

    constructor() {
        effect(() => {
            const name = this.stepName();
            const open = this.isOpen();
            if (name && open) {
                this.loadIfNeeded(name);
            }
        });
    }

    private loadIfNeeded(stepName: string): void {
        const current = this.wizardStore.documentationForStep(stepName)();
        const alreadyHandled = this.subscribedStepName === stepName;

        if (!alreadyHandled) {
            this.subscribedStepName = stepName;
        }

        if (!current?.stepDocumentation && !current?.loading && !current?.loaded) {
            this.wizardStore.loadStepDocumentation(stepName);
        }
    }

    onClose(): void {
        this.wizardStore.toggleDocumentationPanel(false);
        this.closePanel.emit();
    }

    isLoadingDoc(): boolean {
        return this.documentationState()?.loading ?? false;
    }

    getErrorMessage(): string | null {
        return this.documentationState()?.error ?? null;
    }

    getStepDocumentation(): string | null {
        return this.documentationState()?.stepDocumentation ?? null;
    }
}
