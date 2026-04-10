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

import { Component, TemplateRef, ViewChild } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { WizardCustomStepDirective } from './wizard-custom-step.directive';

@Component({
    standalone: true,
    imports: [WizardCustomStepDirective],
    template: `
        <ng-template wizardCustomStep="test-step" #tmpl>
            <div data-qa="custom-step-content">Custom content</div>
        </ng-template>
    `
})
class TestHostComponent {
    @ViewChild(WizardCustomStepDirective) directive!: WizardCustomStepDirective;
}

async function setup() {
    await TestBed.configureTestingModule({
        imports: [TestHostComponent]
    }).compileComponents();

    const fixture = TestBed.createComponent(TestHostComponent);
    fixture.detectChanges();

    return { fixture, component: fixture.componentInstance };
}

describe('WizardCustomStepDirective', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should create', async () => {
        const { component } = await setup();
        expect(component.directive).toBeTruthy();
    });

    it('should capture the step name', async () => {
        const { component } = await setup();
        expect(component.directive.stepName()).toBe('test-step');
    });

    it('should provide a template ref', async () => {
        const { component } = await setup();
        expect(component.directive.templateRef).toBeInstanceOf(TemplateRef);
    });
});
