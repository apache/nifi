import { Component, Inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FlowAnalysisRule, FlowAnalysisRuleViolation } from '../../../../../state/flow-analysis';
import { Store } from '@ngrx/store';
import { navigateToComponentDocumentation } from 'apps/nifi/src/app/state/documentation/documentation.actions';
import { selectCurrentProcessGroupId } from '../../../../../state/flow/flow.selectors';

interface Data {
    violation: FlowAnalysisRuleViolation;
    rule: FlowAnalysisRule;
}

@Component({
    selector: 'app-violation-details-dialog',
    standalone: true,
    imports: [CommonModule, MatDialogModule],
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
                        select: this.rule.type,
                        group: this.rule.bundle.group,
                        artifact: this.rule.bundle.artifact,
                        version: this.rule.bundle.version
                    }
                }
            })
        );
    }
}
