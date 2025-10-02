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

import { Component, EventEmitter, Input, Output, inject } from '@angular/core';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { NiFiCommon, NifiTooltipDirective } from '@nifi/shared';
import { FlowConfiguration } from '../../../../../state/flow-configuration';
import { CurrentUser } from '../../../../../state/current-user';
import { BoundProcessGroup, ParameterContextEntity } from '../../../../../state/shared';
import { MatIconButton } from '@angular/material/button';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';
import { RouterLink } from '@angular/router';
import { NgClass } from '@angular/common';
import { ProcessGroupTip } from '../../../../../ui/common/tooltips/process-group-tip/process-group-tip.component';

@Component({
    selector: 'parameter-context-table',
    templateUrl: './parameter-context-table.component.html',
    styleUrls: ['./parameter-context-table.component.scss'],
    imports: [
        MatTableModule,
        MatSortModule,
        MatIconButton,
        MatMenuTrigger,
        MatMenu,
        MatMenuItem,
        RouterLink,
        NgClass,
        NifiTooltipDirective
    ]
})
export class ParameterContextTable {
    private nifiCommon = inject(NiFiCommon);

    @Input() initialSortColumn: 'name' | 'provider' | 'description' | 'process groups' = 'name';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';
    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };

    @Input() set parameterContexts(parameterContextEntities: ParameterContextEntity[]) {
        this.dataSource.data = this.sortEntities(parameterContextEntities, this.activeSort);
    }

    @Input() selectedParameterContextId!: string;
    @Input() flowConfiguration!: FlowConfiguration;
    @Input() currentUser!: CurrentUser;

    @Output() selectParameterContext: EventEmitter<ParameterContextEntity> = new EventEmitter<ParameterContextEntity>();
    @Output() editParameterContext: EventEmitter<ParameterContextEntity> = new EventEmitter<ParameterContextEntity>();
    @Output() deleteParameterContext: EventEmitter<ParameterContextEntity> = new EventEmitter<ParameterContextEntity>();
    @Output() manageAccessPolicies: EventEmitter<ParameterContextEntity> = new EventEmitter<ParameterContextEntity>();

    displayedColumns: string[] = ['name', 'provider', 'description', 'process groups', 'actions'];
    dataSource: MatTableDataSource<ParameterContextEntity> = new MatTableDataSource<ParameterContextEntity>();

    canRead(entity: ParameterContextEntity): boolean {
        return entity.permissions.canRead;
    }

    canWrite(entity: ParameterContextEntity): boolean {
        return entity.permissions.canWrite;
    }

    formatName(entity: ParameterContextEntity): string {
        return this.canRead(entity) && entity.component ? entity.component.name : entity.id;
    }

    formatProvider(entity: ParameterContextEntity): string {
        if (!this.canRead(entity) || !entity.component) {
            return '';
        }
        const paramProvider = entity.component.parameterProviderConfiguration;
        if (!paramProvider) {
            return '';
        }
        return `${paramProvider.component.parameterGroupName} from ${paramProvider.component.parameterProviderName}`;
    }

    formatDescription(entity: ParameterContextEntity): string {
        return this.canRead(entity) && entity.component ? entity.component.description : '';
    }

    formatProcessGroups(entity: ParameterContextEntity): string {
        const boundedProcessGroups = this.getBoundedProcessGroups(entity);
        return boundedProcessGroups.length <= 1
            ? this.getAllowedProcessGroupName(boundedProcessGroups)
            : boundedProcessGroups.length.toString() + ' referencing Process Groups';
    }

    hasMultipleProcessGroups(entity: ParameterContextEntity): boolean {
        const boundedProcessGroups = this.getBoundedProcessGroups(entity);
        return boundedProcessGroups.length > 1;
    }

    getBoundedProcessGroups(entity: ParameterContextEntity): BoundProcessGroup[] {
        if (!this.canRead(entity) || entity.component === undefined) {
            return [];
        }
        return entity.component.boundProcessGroups;
    }

    private getAllowedProcessGroupName(groups: BoundProcessGroup[]): string {
        if (groups.length === 0) {
            return '';
        }
        return groups[0].permissions.canRead ? groups[0].component.name : '1 referencing Process Group';
    }

    editClicked(entity: ParameterContextEntity): void {
        this.editParameterContext.next(entity);
    }

    canDelete(entity: ParameterContextEntity): boolean {
        const canModifyParameterContexts: boolean =
            this.currentUser.parameterContextPermissions.canRead &&
            this.currentUser.parameterContextPermissions.canWrite;
        return canModifyParameterContexts && this.canRead(entity) && this.canWrite(entity);
    }

    deleteClicked(entity: ParameterContextEntity): void {
        this.deleteParameterContext.next(entity);
    }

    canManageAccessPolicies(): boolean {
        return this.flowConfiguration?.supportsManagedAuthorizer && this.currentUser.tenantsPermissions.canRead;
    }

    manageAccessPoliciesClicked(entity: ParameterContextEntity): void {
        this.manageAccessPolicies.next(entity);
    }

    canGoToParameterProvider(entity: ParameterContextEntity): boolean {
        if (!this.canRead(entity) || !entity.component) {
            return false;
        }
        return !!entity.component.parameterProviderConfiguration;
    }

    getParameterProviderLink(entity: ParameterContextEntity): string[] {
        if (!this.canRead(entity) || !entity.component || !entity.component.parameterProviderConfiguration) {
            return [];
        }
        return ['/settings', 'parameter-providers', entity.component.parameterProviderConfiguration.id];
    }

    select(entity: ParameterContextEntity): void {
        this.selectParameterContext.next(entity);
    }

    isSelected(entity: ParameterContextEntity): boolean {
        if (this.selectedParameterContextId) {
            return entity.id == this.selectedParameterContextId;
        }
        return false;
    }

    sortData(sort: Sort) {
        this.activeSort = sort;
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private sortEntities(data: ParameterContextEntity[], sort: Sort): ParameterContextEntity[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;

            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(this.formatName(a), this.formatName(b));
                    break;
                case 'provider':
                    retVal = this.nifiCommon.compareString(this.formatProvider(a), this.formatProvider(b));
                    break;
                case 'description':
                    retVal = this.nifiCommon.compareString(this.formatDescription(a), this.formatDescription(b));
                    break;
                case 'process groups':
                    retVal = this.nifiCommon.compareString(this.formatProcessGroups(a), this.formatProcessGroups(b));
                    break;
                default:
                    return 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    protected readonly ProcessGroupTip = ProcessGroupTip;
}
