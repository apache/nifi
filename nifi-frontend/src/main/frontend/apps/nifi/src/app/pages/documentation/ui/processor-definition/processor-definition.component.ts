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

import { Component, OnDestroy } from '@angular/core';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { selectProcessorDefinitionState } from '../../state/processor-definition/processor-definition.selectors';
import { ProcessorDefinitionState } from '../../state/processor-definition';
import { ComponentType, isDefinedAndNotNull } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
    loadProcessorDefinition,
    resetProcessorDefinitionState
} from '../../state/processor-definition/processor-definition.actions';
import { ConfigurableExtensionDefinitionComponent } from '../common/configurable-extension-definition/configurable-extension-definition.component';
import { RelationshipsDefinitionComponent } from '../common/relationships-definition/relationships-definition.component';
import { AttributesDefinitionComponent } from '../common/attributes-definition/attributes-definition.component';
import { InputRequirementComponent } from '../common/input-requirement/input-requirement.component';
import { UseCaseComponent } from '../common/use-case/use-case.component';
import { MultiProcessorUseCaseComponent } from '../common/multi-processor-use-case/multi-processor-use-case.component';
import { selectDefinitionCoordinatesFromRouteForComponentType } from '../../state/documentation/documentation.selectors';
import { SeeAlsoComponent } from '../common/see-also/see-also.component';
import { distinctUntilChanged } from 'rxjs';

@Component({
    selector: 'processor-definition',
    imports: [
        NgxSkeletonLoaderModule,
        ConfigurableExtensionDefinitionComponent,
        RelationshipsDefinitionComponent,
        RelationshipsDefinitionComponent,
        AttributesDefinitionComponent,
        InputRequirementComponent,
        UseCaseComponent,
        MultiProcessorUseCaseComponent,
        SeeAlsoComponent
    ],
    templateUrl: './processor-definition.component.html',
    styleUrl: './processor-definition.component.scss'
})
export class ProcessorDefinition implements OnDestroy {
    processorDefinitionState: ProcessorDefinitionState | null = null;

    constructor(private store: Store<NiFiState>) {
        this.store
            .select(selectDefinitionCoordinatesFromRouteForComponentType(ComponentType.Processor))
            .pipe(
                isDefinedAndNotNull(),
                distinctUntilChanged(
                    (a, b) =>
                        a.group === b.group && a.artifact === b.artifact && a.version === b.version && a.type === b.type
                ),
                takeUntilDestroyed()
            )
            .subscribe((coordinates) => {
                this.store.dispatch(resetProcessorDefinitionState());

                this.store.dispatch(
                    loadProcessorDefinition({
                        coordinates
                    })
                );
            });

        this.store
            .select(selectProcessorDefinitionState)
            .pipe(takeUntilDestroyed())
            .subscribe((processorDefinitionState) => {
                this.processorDefinitionState = processorDefinitionState;

                if (processorDefinitionState.status === 'loading') {
                    window.scrollTo({ top: 0, left: 0 });
                }
            });
    }

    isInitialLoading(state: ProcessorDefinitionState): boolean {
        return state.processorDefinition === null && state.error === null;
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetProcessorDefinitionState());
    }
}
