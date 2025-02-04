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

import { Component, Input, viewChild } from '@angular/core';
import { MatAccordion, MatExpansionModule } from '@angular/material/expansion';
import { InputRequirement, UseCase } from '../../../state/processor-definition';
import { MatIconButton } from '@angular/material/button';
import { InputRequirementComponent } from '../input-requirement/input-requirement.component';

@Component({
    selector: 'use-case',
    templateUrl: './use-case.component.html',
    imports: [MatAccordion, MatExpansionModule, MatIconButton, InputRequirementComponent],
    styleUrl: './use-case.component.scss'
})
export class UseCaseComponent {
    @Input() useCases: UseCase[] | null = null;

    useCaseAccordion = viewChild.required(MatAccordion);

    expand(): void {
        this.useCaseAccordion().openAll();
    }

    collapse(): void {
        this.useCaseAccordion().closeAll();
    }

    formatKeywords(keywords: string[]): string {
        return keywords.join(', ');
    }

    protected readonly InputRequirement = InputRequirement;
}
