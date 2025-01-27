/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { afterRender, Component, ElementRef, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Store } from '@ngrx/store';
import { UpdateAttributeState } from '../../state';
import { MatSlideToggleChange, MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatFormFieldModule } from '@angular/material/form-field';
import { FormBuilder, FormGroup, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatExpansionModule } from '@angular/material/expansion';
import { CdkDrag, CdkDragDrop, CdkDropList, moveItemInArray } from '@angular/cdk/drag-drop';
import { Action, Condition, NewRule, Rule } from '../../state/rules';
import { EvaluationContext } from '../../state/evaluation-context';
import { saveEvaluationContext } from '../../state/evaluation-context/evaluation-context.actions';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { debounceTime, Observable } from 'rxjs';
import {
    createRule,
    editRule,
    populateNewRule,
    promptRuleDeletion,
    resetNewRule
} from '../../state/rules/rules.actions';
import { EditRule } from '../edit-rule/edit-rule.component';
import { isDefinedAndNotNull, NifiTooltipDirective, TextTip } from '@nifi/shared';
import { MatMenu, MatMenuItem, MatMenuModule } from '@angular/material/menu';
import { selectRule } from '../../state/rules/rules.selectors';
import { selectEvaluationContextError } from '../../state/evaluation-context/evaluation-context.selectors';

@Component({
    selector: 'rule-listing',
    imports: [
        CommonModule,
        MatSlideToggleModule,
        MatFormFieldModule,
        FormsModule,
        MatInputModule,
        ReactiveFormsModule,
        MatExpansionModule,
        MatButtonModule,
        MatMenuModule,
        CdkDropList,
        CdkDrag,
        EditRule,
        MatMenu,
        MatMenuItem,
        NifiTooltipDirective
    ],
    templateUrl: './rule-listing.component.html',
    styleUrl: './rule-listing.component.scss'
})
export class RuleListing {
    @Input() set evaluationContext(evaluationContext: EvaluationContext) {
        this.ruleOrder = evaluationContext.ruleOrder;
        this.flowFilePolicy = evaluationContext.flowFilePolicy;
        this.updateFlowFilePolicy();
    }

    @Input() set rules(rules: Rule[]) {
        this.originalRulesList = [...rules];
        this.rulesList = [...rules];
        this.filterRules();
    }
    @Input() newRule?: NewRule;
    @Input() set editable(editable: boolean) {
        this.isEditable = editable;

        if (editable) {
            this.flowFilePolicyForm.get('useOriginalFlowFilePolicy')?.enable();
        } else {
            this.flowFilePolicyForm.get('useOriginalFlowFilePolicy')?.disable();
        }
    }

    isEditable: boolean = false;

    ruleOrder: string[] = [];
    allowRuleReordering: boolean = false;
    originalRulesList: Rule[] = [];
    rulesList: Rule[] = [];
    dirtyRules: Set<string> = new Set<string>();

    filteredRulesList: Rule[] = [];
    searchForm: FormGroup;

    flowFilePolicyForm: FormGroup;
    flowFilePolicy: string = 'USE_ORIGINAL';

    scrollToNewRule: boolean = false;

    private openRuleCount: number = 0;

    constructor(
        private store: Store<UpdateAttributeState>,
        private formBuilder: FormBuilder,
        private ruleListing: ElementRef
    ) {
        this.searchForm = this.formBuilder.group({ searchRules: '' });
        this.flowFilePolicyForm = this.formBuilder.group({ useOriginalFlowFilePolicy: true });

        this.searchForm
            .get('searchRules')
            ?.valueChanges.pipe(takeUntilDestroyed(), debounceTime(500))
            .subscribe(() => {
                this.filterRules();
            });

        this.store
            .select(selectEvaluationContextError)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe(() => {
                this.updateFlowFilePolicy();

                this.rulesList = [...this.originalRulesList];
                this.filterRules();
            });

        afterRender(() => {
            if (this.scrollToNewRule) {
                const newRulePanel = this.ruleListing.nativeElement.querySelector('.new-rule');
                if (newRulePanel) {
                    window.setTimeout(function () {
                        newRulePanel.scrollIntoView({
                            block: 'center'
                        });
                    }, 0);
                    this.scrollToNewRule = false;
                }
            }
        });
    }

    private filterRules(): void {
        const filter: string | null | undefined = this.searchForm.get('searchRules')?.value;
        if (filter) {
            const filterText = filter.toLowerCase();
            this.filteredRulesList = this.rulesList.filter((rule) => this.ruleMatches(rule, filterText));
        } else {
            this.filteredRulesList = [...this.rulesList];
        }
    }

    private ruleMatches(rule: Rule, filterText: string): boolean {
        return (
            rule.id.toLowerCase().includes(filterText) ||
            rule.name.toLowerCase().includes(filterText) ||
            rule.comments.toLowerCase().includes(filterText) ||
            rule.conditions.some((condition) => this.conditionMatches(condition, filterText)) ||
            rule.actions.some((action) => this.actionMatches(action, filterText))
        );
    }

    private conditionMatches(condition: Condition, filterText: string): boolean {
        return (
            condition.id.toLowerCase().includes(filterText) || condition.expression.toLowerCase().includes(filterText)
        );
    }

    private actionMatches(action: Action, filterText: string): boolean {
        return (
            action.id.toLowerCase().includes(filterText) ||
            action.attribute.toLowerCase().includes(filterText) ||
            action.value.toLowerCase().includes(filterText)
        );
    }

    private updateFlowFilePolicy(): void {
        this.flowFilePolicyForm.get('useOriginalFlowFilePolicy')?.setValue(this.flowFilePolicy === 'USE_ORIGINAL');
    }

    reorderDisabled(): boolean {
        return this.filterApplied() || this.areAnyExpanded() || this.newRule !== undefined || !this.isEditable;
    }

    areAnyExpanded(): boolean {
        // override the expansion panel check when allowing rule reordering
        if (this.allowRuleReordering) {
            return false;
        }

        return this.openRuleCount > 0;
    }

    filterApplied(): boolean {
        return this.rulesList.length != this.filteredRulesList.length;
    }

    populateNewRule(): void {
        this.store.dispatch(populateNewRule({}));
    }

    cloneRule(rule: Rule): void {
        this.store.dispatch(populateNewRule({ rule }));
    }

    newRuleExpanded(): void {
        this.scrollToNewRule = true;
    }

    deleteRule(rule: Rule): void {
        this.store.dispatch(promptRuleDeletion({ rule }));
    }

    flowFilePolicyToggled(event: MatSlideToggleChange): void {
        this.store.dispatch(
            saveEvaluationContext({
                evaluationContext: {
                    ruleOrder: this.ruleOrder,
                    flowFilePolicy: event.checked ? 'USE_ORIGINAL' : 'USE_CLONE'
                }
            })
        );
    }

    setAllowRuleReordering(event: MatSlideToggleChange): void {
        this.allowRuleReordering = event.checked;
    }

    panelOpened(): void {
        this.openRuleCount++;
    }

    panelClosed(): void {
        this.openRuleCount--;
    }

    getExistingRuleNames(): string[] {
        return this.rulesList.map((rule) => rule.name);
    }

    getRuleUpdates(id: string): Observable<Rule | undefined> {
        return this.store.select(selectRule(id));
    }

    createRule(newRule: NewRule): void {
        this.store.dispatch(
            createRule({
                newRule
            })
        );
    }

    cancelNewRule(): void {
        this.store.dispatch(resetNewRule());
    }

    editRule(rule: Rule): void {
        this.store.dispatch(
            editRule({
                rule
            })
        );
    }

    setDirty(id: string, dirty: boolean): void {
        if (dirty) {
            this.dirtyRules.add(id);
        } else {
            this.dirtyRules.delete(id);
        }
    }

    isDirty(id: string): boolean {
        return this.dirtyRules.has(id);
    }

    reorderRules(event: CdkDragDrop<Rule[]>): void {
        if (event.previousIndex !== event.currentIndex) {
            moveItemInArray(event.container.data, event.previousIndex, event.currentIndex);

            this.store.dispatch(
                saveEvaluationContext({
                    evaluationContext: {
                        ruleOrder: event.container.data.map((rule) => rule.id),
                        flowFilePolicy: this.flowFilePolicy
                    }
                })
            );

            this.filterRules();
        }
    }

    protected readonly TextTip = TextTip;
}
