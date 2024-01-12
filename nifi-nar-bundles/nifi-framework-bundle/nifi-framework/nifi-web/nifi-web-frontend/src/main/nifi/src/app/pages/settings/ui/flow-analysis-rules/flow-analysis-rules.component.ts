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

import { Component, OnDestroy, OnInit } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Store } from '@ngrx/store';
import { filter, switchMap, take } from 'rxjs';
import {
    selectFlowAnalysisRuleIdFromRoute,
    selectFlowAnalysisRulesState,
    selectSingleEditedFlowAnalysisRule,
    selectRule
} from '../../state/flow-analysis-rules/flow-analysis-rules.selectors';
import {
    loadFlowAnalysisRules,
    navigateToEditFlowAnalysisRule,
    openConfigureFlowAnalysisRuleDialog,
    openNewFlowAnalysisRuleDialog,
    promptFlowAnalysisRuleDeletion,
    resetFlowAnalysisRulesState,
    selectFlowAnalysisRule,
    enableFlowAnalysisRule,
    disableFlowAnalysisRule
} from '../../state/flow-analysis-rules/flow-analysis-rules.actions';
import { initialState } from '../../state/flow-analysis-rules/flow-analysis-rules.reducer';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { NiFiState } from '../../../../state';
import { FlowAnalysisRuleEntity, FlowAnalysisRulesState } from '../../state/flow-analysis-rules';
import { CurrentUser } from '../../../../state/current-user';

@Component({
    selector: 'flow-analysis-rules',
    templateUrl: './flow-analysis-rules.component.html',
    styleUrls: ['./flow-analysis-rules.component.scss']
})
export class FlowAnalysisRules implements OnInit, OnDestroy {
    flowAnalysisRuleState$ = this.store.select(selectFlowAnalysisRulesState);
    selectedFlowAnalysisRuleId$ = this.store.select(selectFlowAnalysisRuleIdFromRoute);
    currentUser$ = this.store.select(selectCurrentUser);

    constructor(private store: Store<NiFiState>) {
        this.store
            .select(selectSingleEditedFlowAnalysisRule)
            .pipe(
                filter((id: string) => id != null),
                switchMap((id: string) =>
                    this.store.select(selectRule(id)).pipe(
                        filter((entity) => entity != null),
                        take(1)
                    )
                ),
                takeUntilDestroyed()
            )
            .subscribe((entity) => {
                if (entity) {
                    this.store.dispatch(
                        openConfigureFlowAnalysisRuleDialog({
                            request: {
                                id: entity.id,
                                flowAnalysisRule: entity
                            }
                        })
                    );
                }
            });
    }

    ngOnInit(): void {
        this.store.dispatch(loadFlowAnalysisRules());
    }

    isInitialLoading(state: FlowAnalysisRulesState): boolean {
        // using the current timestamp to detect the initial load event
        return state.loadedTimestamp == initialState.loadedTimestamp;
    }

    canModifyParent(currentUser: CurrentUser): (entity: FlowAnalysisRuleEntity) => boolean {
        return (entity: FlowAnalysisRuleEntity) =>
            currentUser.controllerPermissions.canRead && currentUser.controllerPermissions.canWrite;
    }

    openNewFlowAnalysisRuleDialog(): void {
        this.store.dispatch(openNewFlowAnalysisRuleDialog());
    }

    refreshFlowAnalysisRuleListing(): void {
        this.store.dispatch(loadFlowAnalysisRules());
    }

    selectFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            selectFlowAnalysisRule({
                request: {
                    id: entity.id
                }
            })
        );
    }

    enableFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            enableFlowAnalysisRule({
                request: {
                    id: entity.id,
                    flowAnalysisRule: entity
                }
            })
        );
    }

    disableFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            disableFlowAnalysisRule({
                request: {
                    id: entity.id,
                    flowAnalysisRule: entity
                }
            })
        );
    }

    deleteFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            promptFlowAnalysisRuleDeletion({
                request: {
                    flowAnalysisRule: entity
                }
            })
        );
    }

    configureFlowAnalysisRule(entity: FlowAnalysisRuleEntity): void {
        this.store.dispatch(
            navigateToEditFlowAnalysisRule({
                id: entity.id
            })
        );
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetFlowAnalysisRulesState());
    }
}
