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

import { Component, effect, input, output, signal } from '@angular/core';
import { MatButton } from '@angular/material/button';
import { NifiSpinnerDirective } from '../../directives/spinner/nifi-spinner.directive';

export type StepActionTrigger = 'next' | 'back' | 'saveAndClose';

@Component({
    selector: 'connector-step-actions',
    standalone: true,
    imports: [MatButton, NifiSpinnerDirective],
    templateUrl: './connector-step-actions.component.html',
    styleUrl: './connector-step-actions.component.scss'
})
export class ConnectorStepActions {
    isSaving = input(false);

    showNext = input(true);
    showBack = input(true);
    showSaveAndClose = input(true);

    nextDisabled = input(false);
    backDisabled = input(false);
    saveAndCloseDisabled = input(false);

    nextLabel = input<string | null>(null);
    backLabel = input<string | null>(null);
    saveAndCloseLabel = input<string | null>(null);
    savingLabel = input<string | null>(null);

    nextActionName = input<string | null>(null);
    backActionName = input<string | null>(null);
    saveAndCloseActionName = input<string | null>(null);

    next = output<void>();
    back = output<void>();
    saveAndClose = output<void>();

    saveTriggeredBy = signal<StepActionTrigger | null>(null);

    constructor() {
        effect(() => {
            if (!this.isSaving()) {
                this.saveTriggeredBy.set(null);
            }
        });
    }

    onNextClick(): void {
        this.saveTriggeredBy.set('next');
        this.next.emit();
    }

    onBackClick(): void {
        this.saveTriggeredBy.set('back');
        this.back.emit();
    }

    onSaveAndCloseClick(): void {
        this.saveTriggeredBy.set('saveAndClose');
        this.saveAndClose.emit();
    }

    isButtonSaving(trigger: StepActionTrigger): boolean {
        return this.isSaving() && this.saveTriggeredBy() === trigger;
    }
}
