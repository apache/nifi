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

import { Component, Inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FlowAnalysisRule, FlowAnalysisRuleViolation } from '../../../../../state/flow-analysis';
import { Store } from '@ngrx/store';
import { navigateToComponentDocumentation } from 'apps/nifi/src/app/state/documentation/documentation.actions';
import { selectCurrentProcessGroupId } from '../../../../../state/flow/flow.selectors';
import { MatButton } from '@angular/material/button';
import { ComponentType } from '@nifi/shared';

interface Data {
    violation: FlowAnalysisRuleViolation;
    rule: FlowAnalysisRule;
}

@Component({
    selector: 'app-violation-details-dialog',
    imports: [CommonModule, MatDialogModule, MatButton],
    templateUrl: './violation-details-dialog.component.html',
    styleUrl: './violation-details-dialog.component.scss'
})
export class ViolationDetailsDialogComponent {
    violation: FlowAnalysisRuleViolation;
    rule: FlowAnalysisRule;
    currentProcessGroupId$ = this.store.select(selectCurrentProcessGroupId);
    currentProcessGroupId = '';

    constructor(
        @Inject(MAT_DIALOG_DATA) public data: Data,
        private store: Store
    ) {
        this.violation = data.violation;
        this.rule = data.rule;
        this.currentProcessGroupId$.subscribe((pgId) => {
            this.currentProcessGroupId = pgId;
        });
    }

    viewDocumentation() {
        this.store.dispatch(
            navigateToComponentDocumentation({
                request: {
                    backNavigation: {
                        route: ['/process-groups', this.currentProcessGroupId],
                        routeBoundary: ['/documentation'],
                        context: 'Canvas'
                    },
                    parameters: {
                        componentType: ComponentType.FlowAnalysisRule,
                        type: this.rule.type,
                        group: this.rule.bundle.group,
                        artifact: this.rule.bundle.artifact,
                        version: this.rule.bundle.version
                    }
                }
            })
        );
    }
}
