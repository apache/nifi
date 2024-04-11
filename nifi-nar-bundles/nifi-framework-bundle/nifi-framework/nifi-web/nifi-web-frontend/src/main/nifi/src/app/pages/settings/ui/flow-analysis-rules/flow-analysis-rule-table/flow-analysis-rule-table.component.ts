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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { RouterLink } from '@angular/router';
import { NgClass } from '@angular/common';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { FlowAnalysisRuleEntity } from '../../../state/flow-analysis-rules';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { BulletinsTip } from '../../../../../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { ValidationErrorsTip } from '../../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { BulletinsTipInput, TextTipInput, ValidationErrorsTipInput } from '../../../../../state/shared';
import { NifiTooltipDirective } from '../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { ReportingTaskEntity } from '../../../state/reporting-tasks';
import { CurrentUser } from '../../../../../state/current-user';

@Component({
    selector: 'flow-analysis-rule-table',
    standalone: true,
    templateUrl: './flow-analysis-rule-table.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        MatSortModule,
        NgClass,
        NifiTooltipDirective,
        RouterLink
    ],
    styleUrls: ['./flow-analysis-rule-table.component.scss']
})
export class FlowAnalysisRuleTable {
    @Input() set flowAnalysisRules(flowAnalysisRuleEntities: FlowAnalysisRuleEntity[]) {
        this.dataSource.data = this.sortFlowAnalysisRules(flowAnalysisRuleEntities, this.sort);
    }

    @Input() selectedFlowAnalysisRuleId!: string;
    @Input() currentUser!: CurrentUser;

    @Output() selectFlowAnalysisRule: EventEmitter<FlowAnalysisRuleEntity> = new EventEmitter<FlowAnalysisRuleEntity>();
    @Output() viewFlowAnalysisRuleDocumentation: EventEmitter<FlowAnalysisRuleEntity> =
        new EventEmitter<FlowAnalysisRuleEntity>();
    @Output() deleteFlowAnalysisRule: EventEmitter<FlowAnalysisRuleEntity> = new EventEmitter<FlowAnalysisRuleEntity>();
    @Output() configureFlowAnalysisRule: EventEmitter<FlowAnalysisRuleEntity> =
        new EventEmitter<FlowAnalysisRuleEntity>();
    @Output() enableFlowAnalysisRule: EventEmitter<FlowAnalysisRuleEntity> = new EventEmitter<FlowAnalysisRuleEntity>();
    @Output() viewStateFlowAnalysisRule: EventEmitter<FlowAnalysisRuleEntity> =
        new EventEmitter<FlowAnalysisRuleEntity>();
    @Output() disableFlowAnalysisRule: EventEmitter<FlowAnalysisRuleEntity> =
        new EventEmitter<FlowAnalysisRuleEntity>();

    sort: Sort = {
        active: 'name',
        direction: 'asc'
    };

    protected readonly TextTip = TextTip;
    protected readonly BulletinsTip = BulletinsTip;
    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    displayedColumns: string[] = ['moreDetails', 'name', 'type', 'bundle', 'state', 'actions'];
    dataSource: MatTableDataSource<FlowAnalysisRuleEntity> = new MatTableDataSource<FlowAnalysisRuleEntity>();

    constructor(private nifiCommon: NiFiCommon) {}

    updateSort(sort: Sort): void {
        this.sort = sort;
        this.dataSource.data = this.sortFlowAnalysisRules(this.dataSource.data, sort);
    }

    sortFlowAnalysisRules(items: FlowAnalysisRuleEntity[], sort: Sort): FlowAnalysisRuleEntity[] {
        const data: FlowAnalysisRuleEntity[] = items.slice();
        return data.sort((a, b) => {
            const isAsc = sort.direction === 'asc';

            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(a.component.name, b.component.name);
                    break;
                case 'type':
                    retVal = this.nifiCommon.compareString(this.formatType(a), this.formatType(b));
                    break;
                case 'bundle':
                    retVal = this.nifiCommon.compareString(this.formatBundle(a), this.formatBundle(b));
                    break;
                case 'state':
                    retVal = this.nifiCommon.compareString(this.formatState(a), this.formatState(b));
                    break;
            }

            return retVal * (isAsc ? 1 : -1);
        });
    }

    canRead(entity: FlowAnalysisRuleEntity): boolean {
        return entity.permissions.canRead;
    }

    canWrite(entity: FlowAnalysisRuleEntity): boolean {
        return entity.permissions.canWrite;
    }

    canOperate(entity: FlowAnalysisRuleEntity): boolean {
        if (this.canWrite(entity)) {
            return true;
        }
        return !!entity.operatePermissions?.canWrite;
    }

    viewDocumentationClicked(entity: FlowAnalysisRuleEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.viewFlowAnalysisRuleDocumentation.next(entity);
    }

    hasComments(entity: FlowAnalysisRuleEntity): boolean {
        return !this.nifiCommon.isBlank(entity.component.comments);
    }

    getCommentsTipData(entity: FlowAnalysisRuleEntity): TextTipInput {
        return {
            text: entity.component.comments
        };
    }

    hasErrors(entity: FlowAnalysisRuleEntity): boolean {
        return !this.nifiCommon.isEmpty(entity.component.validationErrors);
    }

    getValidationErrorsTipData(entity: FlowAnalysisRuleEntity): ValidationErrorsTipInput {
        return {
            isValidating: entity.status.validationStatus === 'VALIDATING',
            validationErrors: entity.component.validationErrors
        };
    }

    hasBulletins(entity: FlowAnalysisRuleEntity): boolean {
        return !this.nifiCommon.isEmpty(entity.bulletins);
    }

    getBulletinsTipData(entity: FlowAnalysisRuleEntity): BulletinsTipInput {
        return {
            bulletins: entity.bulletins
        };
    }

    getStateIcon(entity: FlowAnalysisRuleEntity): string {
        if (entity.status.validationStatus === 'VALIDATING') {
            return 'validating nifi-surface-default fa fa-spin fa-circle-o-notch';
        } else if (entity.status.validationStatus === 'INVALID') {
            return 'invalid fa fa-warning';
        } else {
            if (entity.status.runStatus === 'DISABLED') {
                return 'disabled primary-color icon icon-enable-false';
            } else if (entity.status.runStatus === 'ENABLED') {
                return 'enabled nifi-success-default fa fa-flash';
            }
        }
        return '';
    }

    formatState(entity: FlowAnalysisRuleEntity): string {
        if (entity.status.validationStatus === 'VALIDATING') {
            return 'Validating';
        } else if (entity.status.validationStatus === 'INVALID') {
            return 'Invalid';
        } else {
            if (entity.status.runStatus === 'DISABLED') {
                return 'Disabled';
            } else if (entity.status.runStatus === 'ENABLED') {
                return 'Enabled';
            }
        }
        return '';
    }

    formatType(entity: FlowAnalysisRuleEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: FlowAnalysisRuleEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    isDisabled(entity: FlowAnalysisRuleEntity): boolean {
        return entity.status.runStatus === 'DISABLED';
    }

    isEnabled(entity: FlowAnalysisRuleEntity): boolean {
        return entity.status.runStatus === 'ENABLED';
    }

    hasActiveThreads(entity: ReportingTaskEntity): boolean {
        return entity.status?.activeThreadCount > 0;
    }

    canConfigure(entity: FlowAnalysisRuleEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity) && this.isDisabled(entity);
    }

    configureClicked(entity: FlowAnalysisRuleEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.configureFlowAnalysisRule.next(entity);
    }

    canEnable(entity: FlowAnalysisRuleEntity): boolean {
        const userAuthorized: boolean = this.canRead(entity) && this.canOperate(entity);
        return userAuthorized && this.isDisabled(entity) && entity.status.validationStatus === 'VALID';
    }

    enabledClicked(entity: FlowAnalysisRuleEntity): void {
        this.enableFlowAnalysisRule.next(entity);
    }

    canDisable(entity: FlowAnalysisRuleEntity): boolean {
        const userAuthorized: boolean = this.canRead(entity) && this.canOperate(entity);
        return userAuthorized && this.isEnabled(entity);
    }

    disableClicked(entity: FlowAnalysisRuleEntity): void {
        this.disableFlowAnalysisRule.next(entity);
    }

    canChangeVersion(entity: FlowAnalysisRuleEntity): boolean {
        return (
            this.isDisabled(entity) &&
            this.canRead(entity) &&
            this.canWrite(entity) &&
            entity.component.multipleVersionsAvailable
        );
    }

    canDelete(entity: FlowAnalysisRuleEntity): boolean {
        return this.isDisabled(entity) && this.canRead(entity) && this.canWrite(entity) && this.canModifyParent();
    }

    canModifyParent(): boolean {
        return this.currentUser.controllerPermissions.canRead && this.currentUser.controllerPermissions.canWrite;
    }

    deleteClicked(entity: FlowAnalysisRuleEntity): void {
        this.deleteFlowAnalysisRule.next(entity);
    }

    canViewState(entity: FlowAnalysisRuleEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity) && entity.component.persistsState === true;
    }

    viewStateClicked(entity: FlowAnalysisRuleEntity): void {
        this.viewStateFlowAnalysisRule.next(entity);
    }

    select(entity: FlowAnalysisRuleEntity): void {
        this.selectFlowAnalysisRule.next(entity);
    }

    isSelected(entity: FlowAnalysisRuleEntity): boolean {
        if (this.selectedFlowAnalysisRuleId) {
            return entity.id == this.selectedFlowAnalysisRuleId;
        }
        return false;
    }
}
