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

import { Component, EventEmitter, Output, ViewEncapsulation } from '@angular/core';
import { STEPPER_GLOBAL_OPTIONS } from '@angular/cdk/stepper';
import { MatStep, MatStepHeader, MatStepper } from '@angular/material/stepper';
import { StepState } from '@angular/cdk/stepper';
import { NgTemplateOutlet } from '@angular/common';
import { CdkStepper } from '@angular/cdk/stepper';

export interface BeforeStepChangeEvent {
    currentIndex: number;
    targetIndex: number;
}

@Component({
    selector: 'wizard',
    imports: [NgTemplateOutlet, MatStepHeader],
    templateUrl: 'wizard.component.html',
    styleUrl: 'wizard.component.scss',
    providers: [
        { provide: CdkStepper, useExisting: Wizard },
        { provide: STEPPER_GLOBAL_OPTIONS, useValue: { showError: true, displayDefaultIndicatorType: true } }
    ],
    encapsulation: ViewEncapsulation.None
})
export class Wizard extends MatStepper {
    /**
     * Emitted when a step header is clicked, before any navigation occurs.
     * If a consumer subscribes to this output, the wizard will NOT automatically
     * call step.select(). The consumer is responsible for calling goToStep()
     * to complete the navigation.
     *
     * If no consumer subscribes, the wizard falls back to the default behavior
     * of calling step.select() immediately (backward compatible).
     */
    @Output() beforeStepChange = new EventEmitter<BeforeStepChangeEvent>();

    constructor() {
        super();
        this.orientation = 'vertical';
    }

    stepIsDisabled(step: MatStep): boolean {
        if (!this.linear) {
            return false;
        }
        // Defer to the CDK/Material navigability logic to ensure parity with MatStepper
        return !step.isNavigable();
    }

    onHeaderClick(step: MatStep): void {
        if (this.stepIsDisabled(step)) {
            return;
        }

        if (this.beforeStepChange.observed) {
            const targetIndex = this.steps.toArray().indexOf(step);
            this.beforeStepChange.emit({
                currentIndex: this.selectedIndex,
                targetIndex
            });
        } else {
            step.select();
        }
    }

    /**
     * Programmatically navigate to a step by index.
     * Used by consumers that handle beforeStepChange to complete deferred navigation.
     */
    goToStep(index: number): void {
        const stepsArray = this.steps.toArray();
        if (index >= 0 && index < stepsArray.length) {
            stepsArray[index].select();
        }
    }

    computeState(index: number, step: MatStep): StepState {
        if (step.hasError) {
            return this.selectedIndex === index ? 'edit' : 'error';
        }
        if (step.completed) {
            return 'done';
        }
        return this.selectedIndex === index ? 'edit' : 'number';
    }
}
