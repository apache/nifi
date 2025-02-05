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
import { ComponentType, NiFiCommon } from '@nifi/shared';
import { DocumentedType } from '../../../../../state/shared';
import { Observable } from 'rxjs';
import { AsyncPipe } from '@angular/common';
import { MatAccordion, MatExpansionModule } from '@angular/material/expansion';
import { NiFiState } from '../../../../../state';
import { Store } from '@ngrx/store';
import { selectProcessorFromType } from '../../../../../state/extension-types/extension-types.selectors';
import { RouterLink } from '@angular/router';
import { MultiProcessorUseCase } from '../../../state/processor-definition';
import { MatIconButton } from '@angular/material/button';

@Component({
    selector: 'multi-processor-use-case',
    templateUrl: './multi-processor-use-case.component.html',
    imports: [MatAccordion, MatExpansionModule, MatIconButton, AsyncPipe, RouterLink],
    styleUrl: './multi-processor-use-case.component.scss'
})
export class MultiProcessorUseCaseComponent {
    @Input() multiProcessorUseCases: MultiProcessorUseCase[] | null = null;

    useCaseAccordion = viewChild.required(MatAccordion);

    constructor(
        private store: Store<NiFiState>,
        private nifiCommon: NiFiCommon
    ) {}

    expand(): void {
        this.useCaseAccordion().openAll();
    }

    collapse(): void {
        this.useCaseAccordion().closeAll();
    }

    formatKeywords(keywords: string[]): string {
        return keywords.join(', ');
    }

    getProcessorFromType(processorType: string): Observable<DocumentedType | undefined> {
        return this.store.select(selectProcessorFromType(processorType));
    }

    formatProcessorName(processorType: string): string {
        return this.nifiCommon.getComponentTypeLabel(processorType);
    }

    protected readonly ComponentType = ComponentType;
}
