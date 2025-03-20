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
import { ComponentType, isDefinedAndNotNull } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ConfigurableExtensionDefinitionComponent } from '../common/configurable-extension-definition/configurable-extension-definition.component';
import { selectDefinitionCoordinatesFromRouteForComponentType } from '../../state/documentation/documentation.selectors';
import { distinctUntilChanged } from 'rxjs';
import { FlowAnalysisRuleDefinitionState } from '../../state/flow-analysis-rule-definition';
import {
    loadFlowAnalysisRuleDefinition,
    resetFlowAnalysisRuleDefinitionState
} from '../../state/flow-analysis-rule-definition/flow-analysis-rule-definition.actions';
import { selectFlowAnalysisRuleDefinitionState } from '../../state/flow-analysis-rule-definition/flow-analysis-rule-definition.selectors';

@Component({
    selector: 'flow-analysis-rule-definition',
    imports: [NgxSkeletonLoaderModule, ConfigurableExtensionDefinitionComponent],
    templateUrl: './flow-analysis-rule-definition.component.html',
    styleUrl: './flow-analysis-rule-definition.component.scss'
})
export class FlowAnalysisRuleDefinition implements OnDestroy {
    flowAnalysisRuleDefinitionState: FlowAnalysisRuleDefinitionState | null = null;

    constructor(private store: Store<NiFiState>) {
        this.store
            .select(selectDefinitionCoordinatesFromRouteForComponentType(ComponentType.FlowAnalysisRule))
            .pipe(
                isDefinedAndNotNull(),
                distinctUntilChanged(
                    (a, b) =>
                        a.group === b.group && a.artifact === b.artifact && a.version === b.version && a.type === b.type
                ),
                takeUntilDestroyed()
            )
            .subscribe((coordinates) => {
                this.store.dispatch(
                    loadFlowAnalysisRuleDefinition({
                        coordinates
                    })
                );
            });

        this.store
            .select(selectFlowAnalysisRuleDefinitionState)
            .pipe(takeUntilDestroyed())
            .subscribe((flowAnalysisRuleDefinitionState) => {
                this.flowAnalysisRuleDefinitionState = flowAnalysisRuleDefinitionState;

                if (flowAnalysisRuleDefinitionState.status === 'loading') {
                    window.scrollTo({ top: 0, left: 0 });
                }
            });
    }

    isInitialLoading(state: FlowAnalysisRuleDefinitionState): boolean {
        return state.flowAnalysisRuleDefinition === null && state.error === null;
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetFlowAnalysisRuleDefinitionState());
    }
}
