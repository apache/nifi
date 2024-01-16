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

import { AfterViewInit, Component, EventEmitter, Input, Output, ViewChild } from '@angular/core';
import { MatTableDataSource } from '@angular/material/table';
import { MatSort } from '@angular/material/sort';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { ParameterContextEntity } from '../../../state/parameter-context-listing';
import { FlowConfiguration } from '../../../../../state/flow-configuration';
import { CurrentUser } from '../../../../../state/current-user';

@Component({
    selector: 'parameter-context-table',
    templateUrl: './parameter-context-table.component.html',
    styleUrls: ['./parameter-context-table.component.scss', '../../../../../../assets/styles/listing-table.scss']
})
export class ParameterContextTable implements AfterViewInit {
    @Input() set parameterContexts(parameterContextEntities: ParameterContextEntity[]) {
        this.dataSource = new MatTableDataSource<ParameterContextEntity>(parameterContextEntities);
        this.dataSource.sort = this.sort;
        this.dataSource.sortingDataAccessor = (data: ParameterContextEntity, displayColumn: string) => {
            if (this.canRead(data)) {
                if (displayColumn === 'name') {
                    return this.formatName(data);
                } else if (displayColumn === 'type') {
                    return this.formatProvider(data);
                } else if (displayColumn === 'bundle') {
                    return this.formatDescription(data);
                }
            }
            return '';
        };
    }

    @Input() selectedParameterContextId!: string;
    @Input() flowConfiguration!: FlowConfiguration;
    @Input() currentUser!: CurrentUser;

    @Output() selectParameterContext: EventEmitter<ParameterContextEntity> = new EventEmitter<ParameterContextEntity>();
    @Output() editParameterContext: EventEmitter<ParameterContextEntity> = new EventEmitter<ParameterContextEntity>();
    @Output() deleteParameterContext: EventEmitter<ParameterContextEntity> = new EventEmitter<ParameterContextEntity>();

    displayedColumns: string[] = ['moreDetails', 'name', 'provider', 'description', 'actions'];
    dataSource: MatTableDataSource<ParameterContextEntity> = new MatTableDataSource<ParameterContextEntity>();

    @ViewChild(MatSort) sort!: MatSort;

    constructor(private nifiCommon: NiFiCommon) {}

    ngAfterViewInit(): void {
        this.dataSource.sort = this.sort;
    }

    canRead(entity: ParameterContextEntity): boolean {
        return entity.permissions.canRead;
    }

    canWrite(entity: ParameterContextEntity): boolean {
        return entity.permissions.canWrite;
    }

    formatName(entity: ParameterContextEntity): string {
        return entity.component.name;
    }

    formatProvider(entity: ParameterContextEntity): string {
        return '';
    }

    formatDescription(entity: ParameterContextEntity): string {
        return entity.component.description;
    }

    editClicked(entity: ParameterContextEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.editParameterContext.next(entity);
    }

    canDelete(entity: ParameterContextEntity): boolean {
        const canModifyParameterContexts: boolean =
            this.currentUser.parameterContextPermissions.canRead &&
            this.currentUser.parameterContextPermissions.canWrite;
        return canModifyParameterContexts && this.canRead(entity) && this.canWrite(entity);
    }

    deleteClicked(entity: ParameterContextEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.deleteParameterContext.next(entity);
    }

    canManageAccessPolicies(): boolean {
        return this.flowConfiguration.supportsManagedAuthorizer && this.currentUser.tenantsPermissions.canRead;
    }

    getPolicyLink(entity: ParameterContextEntity): string[] {
        return ['/access-policies', 'read', 'component', 'parameter-contexts', entity.id];
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
}
