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

import { AfterViewInit, Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatFormFieldModule } from '@angular/material/form-field';
import {
    AbstractControl,
    FormBuilder,
    FormControl,
    FormGroup,
    FormsModule,
    PristineChangeEvent,
    ReactiveFormsModule,
    ValidationErrors,
    ValidatorFn,
    Validators
} from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatExpansionModule } from '@angular/material/expansion';
import { Action, Condition, NewRule, Rule } from '../../state/rules';
import { ConditionTable } from '../condition-table/condition-table.component';
import { ActionTable } from '../action-table/action-table.component';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'edit-rule',
    imports: [
        CommonModule,
        MatSlideToggleModule,
        MatFormFieldModule,
        FormsModule,
        MatInputModule,
        ReactiveFormsModule,
        MatExpansionModule,
        MatButtonModule,
        ConditionTable,
        ActionTable
    ],
    templateUrl: './edit-rule.component.html',
    styleUrl: './edit-rule.component.scss'
})
export class EditRule implements AfterViewInit {
    @Input() id?: string;
    @Input() set name(name: string) {
        this.currentName = name;
        this.nameControl.setValue(name);
    }
    @Input() set existingRuleNames(existingRuleNames: string[]) {
        this.nameControl.setValidators([Validators.required, this.existingRuleValidator(existingRuleNames)]);
    }
    @Input() set comments(comments: string) {
        this.currentComments = comments;
        this.editRuleForm.get('comments')?.setValue(comments);
    }
    @Input() set conditions(conditions: Condition[]) {
        this.currentConditions = conditions;
        this.editRuleForm.get('conditions')?.setValue(conditions);
    }
    @Input() set actions(actions: Action[]) {
        this.currentActions = actions;
        this.editRuleForm.get('actions')?.setValue(actions);
    }
    @Input() set editable(editable: boolean) {
        this.isEditable = editable;

        if (editable) {
            this.editRuleForm.get('conditions')?.enable();
            this.editRuleForm.get('actions')?.enable();
        } else {
            this.editRuleForm.get('conditions')?.disable();
            this.editRuleForm.get('actions')?.disable();
        }
    }
    @Input() saving: boolean = false;
    @Input() set ruleUpdate(ruleUpdate: Rule) {
        if (ruleUpdate && this.ruleSaved) {
            this.editRuleForm.markAsPristine();
        }
    }

    @Output() afterInit: EventEmitter<void> = new EventEmitter<void>();
    @Output() addRule: EventEmitter<NewRule> = new EventEmitter<NewRule>();
    @Output() editRule: EventEmitter<Rule> = new EventEmitter<Rule>();
    @Output() dirty: EventEmitter<boolean> = new EventEmitter<boolean>();
    @Output() close: EventEmitter<void> = new EventEmitter<void>();

    editRuleForm: FormGroup;
    isEditable: boolean = true;

    currentName: string | null = null;
    currentComments: string | null = null;
    currentConditions: Condition[] | null = null;
    currentActions: Action[] | null = null;

    nameControl: FormControl;
    conditionControl: FormControl;
    actionControl: FormControl;

    private ruleSaved: boolean = false;

    constructor(private formBuilder: FormBuilder) {
        this.nameControl = new FormControl('', Validators.required);
        this.conditionControl = new FormControl({ value: [], disabled: !this.isEditable }, [
            this.conditionsValidator()
        ]);
        this.actionControl = new FormControl({ value: [], disabled: !this.isEditable }, [this.actionsValidator()]);

        this.editRuleForm = this.formBuilder.group({
            name: this.nameControl,
            comments: new FormControl(''),
            conditions: this.conditionControl,
            actions: this.actionControl
        });

        this.editRuleForm.events.pipe(takeUntilDestroyed()).subscribe((event) => {
            if (event instanceof PristineChangeEvent) {
                this.dirty.next(!event.pristine);
            }
        });
    }

    ngAfterViewInit(): void {
        this.afterInit.next();
    }

    private existingRuleValidator(existingRuleNames: string[]): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const value = control.value;
            if (value === '') {
                return null;
            }
            if (existingRuleNames.includes(value)) {
                if (this.id) {
                    if (value !== this.currentName) {
                        return {
                            existingRuleName: true
                        };
                    }
                } else {
                    return {
                        existingRuleName: true
                    };
                }
            }
            return null;
        };
    }

    getNameErrorMessage(): string {
        if (this.nameControl.hasError('required')) {
            return 'Rule name is required.';
        }

        return this.nameControl.hasError('existingRuleName') ? 'A rule with this name already exists.' : '';
    }

    private conditionsValidator(): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const conditions: Condition[] = control.value;

            if (conditions.length === 0) {
                return {
                    conditionsRequired: true
                };
            }

            if (conditions.some((condition) => condition.expression === '')) {
                return {
                    emptyCondition: true
                };
            }

            return null;
        };
    }

    getConditionErrorMessage(): string {
        if (this.conditionControl.hasError('conditionsRequired')) {
            return 'No Conditions defined for this Rule. Create one using the Add button.';
        }

        return this.conditionControl.hasError('emptyCondition') ? 'All Conditions must have an expression.' : '';
    }

    private actionsValidator(): ValidatorFn {
        return (control: AbstractControl): ValidationErrors | null => {
            const actions: Action[] = control.value;

            if (actions.length === 0) {
                return {
                    actionsRequired: true
                };
            }

            if (actions.some((action) => action.attribute === '')) {
                return {
                    emptyAttribute: true
                };
            }

            return null;
        };
    }

    getActionErrorMessage(): string {
        if (this.actionControl.hasError('actionsRequired')) {
            return 'No Actions defined for this Rule. Create one using the Add button.';
        }

        return this.actionControl.hasError('emptyAttribute') ? 'All Actions must have an attribute.' : '';
    }

    cancelClicked(): void {
        this.editRuleForm.reset({
            name: this.currentName,
            comments: this.currentComments,
            conditions: this.currentConditions,
            actions: this.currentActions
        });

        this.dirty.next(false);
        this.close.next();
    }

    saveClicked(): void {
        const name: string = this.nameControl.value;
        const comments: string = this.editRuleForm.get('comments')?.value;
        const conditions: Condition[] = this.editRuleForm.get('conditions')?.value;
        const actions: Action[] = this.editRuleForm.get('actions')?.value;

        if (this.id) {
            this.editRule.next({
                id: this.id,
                name,
                comments,
                conditions,
                actions
            });
        } else {
            this.addRule.next({
                name,
                comments,
                conditions,
                actions
            });
        }

        this.ruleSaved = true;
    }
}
