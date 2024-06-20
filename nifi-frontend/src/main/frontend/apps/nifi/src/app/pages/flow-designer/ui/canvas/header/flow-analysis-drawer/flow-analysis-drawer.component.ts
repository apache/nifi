import { Component, model, viewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FlowAnalysisService } from '../../../../service/flow-analysis.service';
import { selectProcessGroupIdFromRoute } from '../../../../state/flow/flow.selectors';
import { filter, switchMap } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Store } from '@ngrx/store';
import { MatMenuModule } from '@angular/material/menu';
import { MatIconModule } from '@angular/material/icon';
import { MatAccordion, MatExpansionModule } from '@angular/material/expansion';
import { navigateToEditFlowAnalysisRule } from 'apps/nifi/src/app/pages/settings/state/flow-analysis-rules/flow-analysis-rules.actions';
import { navigateToComponentDocumentation } from 'apps/nifi/src/app/state/documentation/documentation.actions';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { FormsModule } from '@angular/forms';

// TODO: see if backend generates typescript interfaces and replace all uses of 'any' type
// TODO: add polling for flowAnalysisService.getResults()
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
    warningRules: any = [];
    enforcedRules: any = [];
    warningViolations: any = [];
    enforcedViolations: any = [];
    warningCount = 0;
    enforcedCount = 0;
    rules: any = [];
    readonly showEnforcedViolations = model(false);
    readonly showWarningViolations = model(false);

    constructor(private flowAnalysisService: FlowAnalysisService, private store: Store) {
        this.store
            .select(selectProcessGroupIdFromRoute)
            .pipe(
                filter((processGroupId: string) => processGroupId != null),
                takeUntilDestroyed(),
                switchMap((processGroupId: string) => {
                    return this.flowAnalysisService.getResults(processGroupId);
                })
            )
            .subscribe((res) => {
                this.rules = res.rules;
                res.rules.forEach((rule: any) => {
                    if (rule.enforcementPolicy === 'WARN') {
                        this.warningRules.push(rule);
                        this.warningCount++;
                    } else {
                        this.enforcedRules.push(rule);
                        this.enforcedCount++;
                    }
                });
                res.ruleViolations.forEach((violation: any) => {

                    if (this.violationsMap.has(violation.ruleId)){
                        this.violationsMap.get(violation.ruleId).push(violation);
                        } else {
                        this.violationsMap.set(violation.ruleId, [violation]);
                        }
                });
                this.enforcedViolations = res.ruleViolations.filter(function (violation: any) {
                    return violation.enforcementPolicy === 'ENFORCE'
                });
                this.warningViolations = res.ruleViolations.filter(function (violation: any) {
                    return violation.enforcementPolicy === 'WARN'
                });
            });
        }

        openRule(rule: any) {
            this.store.dispatch(
                navigateToEditFlowAnalysisRule({
                    id: rule.id
                })
            );
        }

        openDocumentation(rule: any) {
            this.store.dispatch(
                navigateToComponentDocumentation({
                    request: {
                        backNavigation: {
                            route: ['/settings', 'flow-analysis-rules', rule.id],
                            routeBoundary: ['/documentation'],
                            context: 'Flow Analysis Rule'
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

        viewViolationDetails(id: string) {
            // TODO: add violation details modal logic
            throw new Error('Method not implemented.');
        }

        goToComponent(id: string) {
            // TODO: add 'go to component' logic
            throw new Error('Method not implemented.');
        }

        getRuleName(id: string) {
            var rule = this.rules.find(function(rule: any) {
                return rule.id === id;
            });

            return rule.name;
        }
}
