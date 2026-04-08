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
import { MatButtonModule } from '@angular/material/button';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatMenuModule } from '@angular/material/menu';
import { MatTooltipModule } from '@angular/material/tooltip';
import {
    ConnectorEntity,
    ConnectorActionName,
    NifiTooltipDirective,
    NiFiCommon,
    canReadConnector,
    canModifyConnector,
    canOperateConnector,
    isConnectorActionAllowed,
    getConnectorActionDisabledReason
} from '@nifi/shared';
import { ValidationErrorsTipInput } from '../../../../state/shared';
import { FlowConfiguration } from '../../../../state/flow-configuration';
import { CurrentUser } from '../../../../state/current-user';
import { ValidationErrorsTip } from '../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';

@Component({
    selector: 'connector-table',
    standalone: true,
    templateUrl: './connector-table.component.html',
    imports: [MatButtonModule, MatTableModule, MatSortModule, MatMenuModule, MatTooltipModule, NifiTooltipDirective],
    styleUrls: ['./connector-table.component.scss']
})
export class ConnectorTable {
    private nifiCommon = inject(NiFiCommon);

    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    @Input() initialSortColumn: 'name' | 'type' | 'bundle' | 'state' = 'name';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';
    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };

    @Input() set connectors(connectorEntities: ConnectorEntity[]) {
        this.dataSource.data = this.sortEntities(connectorEntities, this.activeSort);
    }

    @Input() selectedConnectorId!: string;
    @Input() flowConfiguration!: FlowConfiguration;
    @Input() currentUser!: CurrentUser;

    @Output() selectConnector = new EventEmitter<ConnectorEntity>();
    @Output() viewConnector = new EventEmitter<ConnectorEntity>();
    @Output() viewDetails = new EventEmitter<ConnectorEntity>();
    @Output() configureConnector = new EventEmitter<ConnectorEntity>();
    @Output() renameConnector = new EventEmitter<ConnectorEntity>();
    @Output() startConnector = new EventEmitter<ConnectorEntity>();
    @Output() stopConnector = new EventEmitter<ConnectorEntity>();
    @Output() deleteConnector = new EventEmitter<ConnectorEntity>();
    @Output() manageAccessPolicies = new EventEmitter<ConnectorEntity>();
    @Output() viewDocumentation = new EventEmitter<ConnectorEntity>();
    @Output() discardConnectorConfig = new EventEmitter<ConnectorEntity>();
    @Output() drainConnector = new EventEmitter<ConnectorEntity>();
    @Output() cancelDrainConnector = new EventEmitter<ConnectorEntity>();
    @Output() purgeConnector = new EventEmitter<ConnectorEntity>();

    displayedColumns: string[] = ['moreDetails', 'name', 'type', 'bundle', 'state', 'actions'];
    dataSource: MatTableDataSource<ConnectorEntity> = new MatTableDataSource<ConnectorEntity>();

    canRead(entity: ConnectorEntity): boolean {
        return canReadConnector(entity);
    }

    hasErrors(entity: ConnectorEntity): boolean {
        return !this.nifiCommon.isEmpty(entity.component.validationErrors);
    }

    getValidationErrorsTipData(entity: ConnectorEntity): ValidationErrorsTipInput {
        return {
            isValidating: entity.component.validationStatus === 'VALIDATING',
            validationErrors: entity.component.validationErrors || []
        };
    }

    canModify(entity: ConnectorEntity): boolean {
        return canModifyConnector(entity);
    }

    canOperate(entity: ConnectorEntity): boolean {
        return canOperateConnector(entity);
    }

    isActionAllowed(entity: ConnectorEntity, actionName: ConnectorActionName): boolean {
        return isConnectorActionAllowed(entity, actionName);
    }

    getActionDisabledReason(entity: ConnectorEntity, actionName: ConnectorActionName): string {
        return getConnectorActionDisabledReason(entity, actionName);
    }

    canConfigure(entity: ConnectorEntity): boolean {
        return isConnectorActionAllowed(entity, 'CONFIGURE');
    }

    canDelete(entity: ConnectorEntity): boolean {
        return isConnectorActionAllowed(entity, 'DELETE');
    }

    canStart(entity: ConnectorEntity): boolean {
        return isConnectorActionAllowed(entity, 'START');
    }

    canStop(entity: ConnectorEntity): boolean {
        return isConnectorActionAllowed(entity, 'STOP');
    }

    canDiscardConfig(entity: ConnectorEntity): boolean {
        return isConnectorActionAllowed(entity, 'DISCARD_WORKING_CONFIGURATION');
    }

    canDrain(entity: ConnectorEntity): boolean {
        return isConnectorActionAllowed(entity, 'DRAIN_FLOWFILES');
    }

    canCancelDrain(entity: ConnectorEntity): boolean {
        return isConnectorActionAllowed(entity, 'CANCEL_DRAIN_FLOWFILES');
    }

    canPurge(entity: ConnectorEntity): boolean {
        return isConnectorActionAllowed(entity, 'PURGE_FLOWFILES');
    }

    formatName(entity: ConnectorEntity): string {
        return this.canRead(entity) ? entity.component.name : entity.id;
    }

    formatType(entity: ConnectorEntity): string {
        return this.canRead(entity) ? this.nifiCommon.formatType(entity.component) : '';
    }

    formatBundle(entity: ConnectorEntity): string {
        return this.canRead(entity) ? this.nifiCommon.formatBundle(entity.component.bundle) : '';
    }

    formatState(entity: ConnectorEntity): string {
        if (!this.canRead(entity)) {
            return '';
        }
        return entity.component.state;
    }

    isSelected(entity: ConnectorEntity): boolean {
        if (this.selectedConnectorId) {
            return entity.id === this.selectedConnectorId;
        }
        return false;
    }

    select(entity: ConnectorEntity): void {
        this.selectConnector.next(entity);
    }

    viewClicked(entity: ConnectorEntity): void {
        this.viewConnector.next(entity);
    }

    viewDetailsClicked(entity: ConnectorEntity): void {
        this.viewDetails.next(entity);
    }

    configureClicked(entity: ConnectorEntity): void {
        this.configureConnector.next(entity);
    }

    renameClicked(entity: ConnectorEntity): void {
        this.renameConnector.next(entity);
    }

    startClicked(entity: ConnectorEntity): void {
        this.startConnector.next(entity);
    }

    stopClicked(entity: ConnectorEntity): void {
        this.stopConnector.next(entity);
    }

    deleteClicked(entity: ConnectorEntity): void {
        this.deleteConnector.next(entity);
    }

    discardConfigClicked(entity: ConnectorEntity): void {
        this.discardConnectorConfig.next(entity);
    }

    drainClicked(entity: ConnectorEntity): void {
        this.drainConnector.next(entity);
    }

    cancelDrainClicked(entity: ConnectorEntity): void {
        this.cancelDrainConnector.next(entity);
    }

    purgeClicked(entity: ConnectorEntity): void {
        this.purgeConnector.next(entity);
    }

    canManageAccessPolicies(): boolean {
        return this.flowConfiguration?.supportsManagedAuthorizer && this.currentUser?.tenantsPermissions?.canRead;
    }

    manageAccessPoliciesClicked(entity: ConnectorEntity): void {
        this.manageAccessPolicies.next(entity);
    }

    viewDocumentationClicked(entity: ConnectorEntity): void {
        this.viewDocumentation.next(entity);
    }

    sortData(sort: Sort): void {
        this.activeSort = sort;
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private sortEntities(data: ConnectorEntity[], sort: Sort): ConnectorEntity[] {
        if (!data) {
            return [];
        }

        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal: number;

            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(this.formatName(a), this.formatName(b));
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
                default:
                    return 0;
            }

            return retVal * (isAsc ? 1 : -1);
        });
    }
}
