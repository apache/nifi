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
import { ReportingTaskEntity } from '../../../state/reporting-tasks';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { BulletinsTip } from '../../../../../ui/common/tooltips/bulletins-tip/bulletins-tip.component';
import { ValidationErrorsTip } from '../../../../../ui/common/tooltips/validation-errors-tip/validation-errors-tip.component';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { BulletinsTipInput, ValidationErrorsTipInput } from '../../../../../state/shared';
import { RegistryClientEntity } from '../../../state/registry-clients';

@Component({
    selector: 'registry-client-table',
    templateUrl: './registry-client-table.component.html',
    styleUrls: ['./registry-client-table.component.scss', '../../../../../../assets/styles/listing-table.scss']
})
export class RegistryClientTable implements AfterViewInit {
    @Input() set registryClients(registryClientEntities: RegistryClientEntity[]) {
        this.dataSource = new MatTableDataSource<RegistryClientEntity>(registryClientEntities);
        this.dataSource.sort = this.sort;
        this.dataSource.sortingDataAccessor = (data: RegistryClientEntity, displayColumn: string) => {
            if (displayColumn === 'name') {
                return this.formatType(data);
            } else if (displayColumn === 'type') {
                return this.formatType(data);
            } else if (displayColumn === 'bundle') {
                return this.formatBundle(data);
            }
            return '';
        };
    }
    @Input() selectedRegistryClientId!: string;

    @Output() selectRegistryClient: EventEmitter<RegistryClientEntity> = new EventEmitter<RegistryClientEntity>();
    @Output() configureRegistryClient: EventEmitter<RegistryClientEntity> = new EventEmitter<RegistryClientEntity>();
    @Output() deleteRegistryClient: EventEmitter<RegistryClientEntity> = new EventEmitter<RegistryClientEntity>();

    protected readonly TextTip = TextTip;
    protected readonly BulletinsTip = BulletinsTip;
    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    displayedColumns: string[] = ['moreDetails', 'name', 'description', 'type', 'bundle', 'actions'];
    dataSource: MatTableDataSource<RegistryClientEntity> = new MatTableDataSource<RegistryClientEntity>();

    @ViewChild(MatSort) sort!: MatSort;

    constructor(private nifiCommon: NiFiCommon) {}

    ngAfterViewInit(): void {
        this.dataSource.sort = this.sort;
    }

    canRead(entity: RegistryClientEntity): boolean {
        return entity.permissions.canRead;
    }

    canWrite(entity: RegistryClientEntity): boolean {
        return entity.permissions.canWrite;
    }

    hasErrors(entity: RegistryClientEntity): boolean {
        return this.canRead(entity) && !this.nifiCommon.isEmpty(entity.component.validationErrors);
    }

    getValidationErrorsTipData(entity: RegistryClientEntity): ValidationErrorsTipInput {
        return {
            isValidating: false,
            validationErrors: entity.component.validationErrors
        };
    }

    hasBulletins(entity: RegistryClientEntity): boolean {
        return this.canRead(entity) && !this.nifiCommon.isEmpty(entity.bulletins);
    }

    getBulletinsTipData(entity: RegistryClientEntity): BulletinsTipInput {
        return {
            // @ts-ignore
            bulletins: entity.bulletins
        };
    }

    formatType(entity: RegistryClientEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: RegistryClientEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    canConfigure(entity: RegistryClientEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity);
    }

    configureClicked(entity: RegistryClientEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.configureRegistryClient.next(entity);
    }

    canDelete(entity: RegistryClientEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity);
    }

    deleteClicked(entity: RegistryClientEntity, event: MouseEvent): void {
        event.stopPropagation();
        this.deleteRegistryClient.next(entity);
    }

    select(entity: ReportingTaskEntity): void {
        this.selectRegistryClient.next(entity);
    }

    isSelected(entity: ReportingTaskEntity): boolean {
        if (this.selectedRegistryClientId) {
            return entity.id == this.selectedRegistryClientId;
        }
        return false;
    }
}
