/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the 'License'); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, OnDestroy } from '@angular/core';
import { Store } from '@ngrx/store';
import { UpdateAttributeApplicationState } from '../../../state';
import { isDefinedAndNotNull } from '@nifi/shared';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { loadRules, resetRulesState } from '../state/rules/rules.actions';
import {
    loadEvaluationContext,
    resetEvaluationContextState
} from '../state/evaluation-context/evaluation-context.actions';
import {
    selectAdvancedUiParametersFromRoute,
    selectEditable
} from '../state/advanced-ui-parameters/advanced-ui-parameters.selectors';
import {
    resetAdvancedUiParametersState,
    setAdvancedUiParameters
} from '../state/advanced-ui-parameters/advanced-ui-parameters.actions';
import { selectRulesState } from '../state/rules/rules.selectors';
import { selectEvaluationContextState } from '../state/evaluation-context/evaluation-context.selectors';

@Component({
    selector: 'update-attribute',
    templateUrl: './update-attribute.component.html',
    styleUrls: ['./update-attribute.component.scss'],
    standalone: false
})
export class UpdateAttribute implements OnDestroy {
    rulesState = this.store.selectSignal(selectRulesState);
    evaluationContextState = this.store.selectSignal(selectEvaluationContextState);
    editable = this.store.selectSignal(selectEditable);

    constructor(private store: Store<UpdateAttributeApplicationState>) {
        this.store
            .select(selectAdvancedUiParametersFromRoute)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((advancedUiParameters) => {
                this.store.dispatch(
                    setAdvancedUiParameters({
                        advancedUiParameters
                    })
                );
                this.store.dispatch(loadEvaluationContext());
                this.store.dispatch(loadRules());
            });
    }

    ngOnDestroy(): void {
        this.store.dispatch(resetRulesState());
        this.store.dispatch(resetEvaluationContextState());
        this.store.dispatch(resetAdvancedUiParametersState());
    }
}
