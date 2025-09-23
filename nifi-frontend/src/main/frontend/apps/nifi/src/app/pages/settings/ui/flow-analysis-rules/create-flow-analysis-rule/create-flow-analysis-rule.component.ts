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

import { Component, inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { ExtensionCreation } from '../../../../../ui/common/extension-creation/extension-creation.component';
import { FlowAnalysisRulesState } from '../../../state/flow-analysis-rules';
import { createFlowAnalysisRule } from '../../../state/flow-analysis-rules/flow-analysis-rules.actions';
import { Client } from '../../../../../service/client.service';
import { DocumentedType } from '../../../../../state/shared';
import { selectSaving } from '../../../state/flow-analysis-rules/flow-analysis-rules.selectors';
import { AsyncPipe } from '@angular/common';
import { CloseOnEscapeDialog } from '@nifi/shared';
import {
    selectExtensionTypesLoadingStatus,
    selectFlowAnalysisRuleTypes
} from '../../../../../state/extension-types/extension-types.selectors';

@Component({
    selector: 'create-flow-analysis-rule',
    imports: [ExtensionCreation, AsyncPipe],
    templateUrl: './create-flow-analysis-rule.component.html',
    styleUrls: ['./create-flow-analysis-rule.component.scss']
})
export class CreateFlowAnalysisRule extends CloseOnEscapeDialog {
    private store = inject<Store<FlowAnalysisRulesState>>(Store);
    private client = inject(Client);

    flowAnalysisRulesTypes$ = this.store.select(selectFlowAnalysisRuleTypes);
    flowAnalysisRulesTypesLoadingStatus$ = this.store.select(selectExtensionTypesLoadingStatus);
    saving$ = this.store.select(selectSaving);

    createFlowAnalysisRule(flowAnalysisRuleType: DocumentedType): void {
        this.store.dispatch(
            createFlowAnalysisRule({
                request: {
                    revision: {
                        clientId: this.client.getClientId(),
                        version: 0
                    },
                    flowAnalysisRuleType: flowAnalysisRuleType.type,
                    flowAnalysisRuleBundle: flowAnalysisRuleType.bundle
                }
            })
        );
    }
}
