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

import { Component, model, viewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Store } from '@ngrx/store';
import { MatMenuModule } from '@angular/material/menu';
import { MatIconModule } from '@angular/material/icon';
import { MatAccordion, MatExpansionModule } from '@angular/material/expansion';
import { navigateToComponentDocumentation } from 'apps/nifi/src/app/state/documentation/documentation.actions';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { FormsModule } from '@angular/forms';
import { selectFlowAnalysisState } from '../../../../state/flow-analysis/flow-analysis.selectors';
import {
    navigateToEditFlowAnalysisRule,
    startPollingFlowAnalysis,
    openRuleDetailsDialog
} from '../../../../state/flow-analysis/flow-analysis.actions';
import { FlowAnalysisRule, FlowAnalysisRuleViolation } from '../../../../state/flow-analysis';
import { selectCurrentProcessGroupId } from '../../../../state/flow/flow.selectors';

@Component({
    selector: 'flow-analysis-drawer',
    standalone: true,
    imports: [CommonModule, MatMenuModule, MatIconModule, MatExpansionModule, MatCheckboxModule, FormsModule],
    templateUrl: './flow-analysis-drawer.component.html',
    styleUrl: './flow-analysis-drawer.component.scss'
})
export class FlowAnalysisDrawerComponent {
    accordion = viewChild.required(MatAccordion);
    violationsMap = new Map();
    warningRules: FlowAnalysisRule[] = [];
    enforcedRules: FlowAnalysisRule[] = [];
    warningViolations: FlowAnalysisRuleViolation[] = [];
    enforcedViolations: FlowAnalysisRuleViolation[] = [];
    rules: FlowAnalysisRule[] = [];
    currentProcessGroupId = '';
    readonly showEnforcedViolations = model(false);
    readonly showWarningViolations = model(false);
    flowAnalysisState$ = this.store.select(selectFlowAnalysisState);
    currentProcessGroupId$ = this.store.select(selectCurrentProcessGroupId);

    constructor(private store: Store) {
        this.store.dispatch(startPollingFlowAnalysis());
        this.flowAnalysisState$.pipe(takeUntilDestroyed()).subscribe((res) => {
            this.clearRulesTracking();
            this.rules = res.rules;

            res.rules.forEach((rule: FlowAnalysisRule) => {
                if (rule.enforcementPolicy === 'WARN') {
                    this.warningRules.push(rule);
                } else {
                    this.enforcedRules.push(rule);
                }
            });
            res.ruleViolations.forEach((violation: FlowAnalysisRuleViolation) => {
                if (this.violationsMap.has(violation.ruleId)) {
                    this.violationsMap.get(violation.ruleId).push(violation);
                } else {
                    this.violationsMap.set(violation.ruleId, [violation]);
                }
            });
            this.enforcedViolations = res.ruleViolations.filter(function (violation: FlowAnalysisRuleViolation) {
                return violation.enforcementPolicy === 'ENFORCE';
            });
            this.warningViolations = res.ruleViolations.filter(function (violation: FlowAnalysisRuleViolation) {
                return violation.enforcementPolicy === 'WARN';
            });
        });
        this.currentProcessGroupId$.subscribe((pgId) => {
            this.currentProcessGroupId = pgId;
        });
    }

    openRule(rule: FlowAnalysisRule) {
        this.store.dispatch(
            navigateToEditFlowAnalysisRule({
                id: rule.id
            })
        );
    }

    clearRulesTracking() {
        this.enforcedRules = [];
        this.warningRules = [];
        this.violationsMap.clear();
    }

    openDocumentation(rule: FlowAnalysisRule) {
        this.store.dispatch(
            navigateToComponentDocumentation({
                request: {
                    backNavigation: {
                        route: ['/process-groups', this.currentProcessGroupId],
                        routeBoundary: ['/documentation'],
                        context: 'Canvas'
                    },
                    parameters: {
                        select: rule.type,
                        group: rule.bundle.group,
                        artifact: rule.bundle.artifact,
                        version: rule.bundle.version
                    }
                }
            })
        );
    }

    viewViolationDetails(violation: FlowAnalysisRuleViolation) {
        const ruleTest: FlowAnalysisRule = this.rules.find((rule) => rule.id === violation.ruleId)!;
        this.store.dispatch(openRuleDetailsDialog({ violation, rule: ruleTest }));
    }

    goToComponent(id: string) {
        // TODO: add 'go to component' logic
        throw new Error('Method not implemented.');
    }

    getRuleName(id: string) {
        const rule = this.rules.find(function (rule: FlowAnalysisRule) {
            return rule.id === id;
        });

        return rule ? rule.name : '';
    }
}
