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
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { NiFiCommon } from '../../../../service/nifi-common.service';
import { MatSort, MatSortModule } from '@angular/material/sort';
import { NgClass, NgIf } from '@angular/common';
import {
    BulletinsTipInput,
    ControllerServiceEntity,
    TextTipInput,
    ValidationErrorsTipInput
} from '../../../../state/shared';
import { NifiTooltipDirective } from '../../nifi-tooltip.directive';
import { TextTip } from '../../tooltips/text-tip/text-tip.component';
import { BulletinsTip } from '../../tooltips/bulletins-tip/bulletins-tip.component';
import { ValidationErrorsTip } from '../../tooltips/validation-errors-tip/validation-errors-tip.component';

@Component({
    selector: 'controller-service-table',
    standalone: true,
    templateUrl: './controller-service-table.component.html',
    imports: [MatButtonModule, MatDialogModule, MatTableModule, MatSortModule, NgIf, NgClass, NifiTooltipDirective],
    styleUrls: ['./controller-service-table.component.scss']
})
export class ControllerServiceTable implements AfterViewInit {
    @Input() set controllerServices(controllerServiceEntities: ControllerServiceEntity[]) {
        this.dataSource = new MatTableDataSource<ControllerServiceEntity>(controllerServiceEntities);
        this.dataSource.sort = this.sort;
        this.dataSource.sortingDataAccessor = (data: ControllerServiceEntity, displayColumn: string) => {
            if (displayColumn == 'name') {
                return this.formatType(data);
            } else if (displayColumn == 'type') {
                return this.formatType(data);
            } else if (displayColumn == 'bundle') {
                return this.formatBundle(data);
            } else if (displayColumn == 'state') {
                return this.formatState(data);
            } else if (displayColumn == 'scope') {
                return this.formatScope(data);
            }
            return '';
        };
    }
    @Input() selectedServiceId!: string;

    @Output() selectControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() deleteControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();
    @Output() configureControllerService: EventEmitter<ControllerServiceEntity> =
        new EventEmitter<ControllerServiceEntity>();

    protected readonly TextTip = TextTip;
    protected readonly BulletinsTip = BulletinsTip;
    protected readonly ValidationErrorsTip = ValidationErrorsTip;

    displayedColumns: string[] = ['moreDetails', 'name', 'type', 'bundle', 'state', 'scope', 'actions'];
    dataSource: MatTableDataSource<ControllerServiceEntity> = new MatTableDataSource<ControllerServiceEntity>();

    @ViewChild(MatSort) sort!: MatSort;

    constructor(private nifiCommon: NiFiCommon) {}

    ngAfterViewInit(): void {
        this.dataSource.sort = this.sort;
    }

    canRead(entity: ControllerServiceEntity): boolean {
        return entity.permissions.canRead;
    }

    canWrite(entity: ControllerServiceEntity): boolean {
        return entity.permissions.canWrite;
    }

    canOperate(entity: ControllerServiceEntity): boolean {
        if (this.canWrite(entity)) {
            return true;
        }
        return !!entity.operatePermissions?.canWrite;
    }

    hasComments(entity: ControllerServiceEntity): boolean {
        return !this.nifiCommon.isBlank(entity.component.comments);
    }

    getCommentsTipData(entity: ControllerServiceEntity): TextTipInput {
        return {
            text: entity.component.comments
        };
    }

    hasErrors(entity: ControllerServiceEntity): boolean {
        return !this.nifiCommon.isEmpty(entity.component.validationErrors);
    }

    getValidationErrorsTipData(entity: ControllerServiceEntity): ValidationErrorsTipInput {
        return {
            isValidating: entity.status.validationStatus === 'VALIDATING',
            validationErrors: entity.component.validationErrors
        };
    }

    hasBulletins(entity: ControllerServiceEntity): boolean {
        return !this.nifiCommon.isEmpty(entity.bulletins);
    }

    getBulletinsTipData(entity: ControllerServiceEntity): BulletinsTipInput {
        return {
            bulletins: entity.bulletins
        };
    }

    getStateIcon(entity: ControllerServiceEntity): string {
        if (entity.status.validationStatus === 'VALIDATING') {
            return 'validating fa fa-spin fa-circle-notch';
        } else if (entity.status.validationStatus === 'INVALID') {
            return 'invalid fa fa-warning';
        } else {
            if (entity.status.runStatus === 'DISABLED') {
                return 'disabled icon icon-enable-false';
            } else if (entity.status.runStatus === 'DISABLING') {
                return 'disabled icon icon-enable-false';
            } else if (entity.status.runStatus === 'ENABLED') {
                return 'enabled fa fa-flash';
            } else if (entity.status.runStatus === 'ENABLING') {
                return 'enabled fa fa-flash';
            }
        }
        return '';
    }

    formatState(entity: ControllerServiceEntity): string {
        if (entity.status.validationStatus === 'VALIDATING') {
            return 'Validating';
        } else if (entity.status.validationStatus === 'INVALID') {
            return 'Invalid';
        } else {
            if (entity.status.runStatus === 'DISABLED') {
                return 'Disabled';
            } else if (entity.status.runStatus === 'DISABLING') {
                return 'Disabling';
            } else if (entity.status.runStatus === 'ENABLED') {
                return 'Enabled';
            } else if (entity.status.runStatus === 'ENABLING') {
                return 'Enabling';
            }
        }
        return '';
    }

    formatType(entity: ControllerServiceEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: ControllerServiceEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    formatScope(entity: ControllerServiceEntity): string {
        // TODO - how to resolve parent pg label (breadcrumb?)
        return 'Controller';
    }

    definedByCurrentGroup(entity: ControllerServiceEntity): boolean {
        // TODO
        return true;
    }

    isDisabled(entity: ControllerServiceEntity): boolean {
        return entity.status.runStatus === 'DISABLED';
    }

    isEnabledOrEnabling(entity: ControllerServiceEntity): boolean {
        return entity.status.runStatus === 'ENABLED' || entity.status.runStatus === 'ENABLING';
    }

    canConfigure(entity: ControllerServiceEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity) && this.isDisabled(entity);
    }

    configureClicked(entity: ControllerServiceEntity): void {
        this.configureControllerService.next(entity);
    }

    canEnable(entity: ControllerServiceEntity): boolean {
        return this.canOperate(entity) && this.isDisabled(entity) && entity.status.validationStatus === 'VALID';
    }

    canDisable(entity: ControllerServiceEntity): boolean {
        return this.canOperate(entity) && this.isEnabledOrEnabling(entity);
    }

    canChangeVersion(entity: ControllerServiceEntity): boolean {
        return (
            this.isDisabled(entity) &&
            this.canRead(entity) &&
            this.canWrite(entity) &&
            entity.component.multipleVersionsAvailable === true
        );
    }

    canDelete(entity: ControllerServiceEntity): boolean {
        const canWriteParent: boolean = true; // TODO canWriteControllerServiceParent(dataContext)
        return this.isDisabled(entity) && this.canRead(entity) && this.canWrite(entity) && canWriteParent;
    }

    deleteClicked(entity: ControllerServiceEntity): void {
        this.deleteControllerService.next(entity);
    }

    canViewState(entity: ControllerServiceEntity): boolean {
        return this.canRead(entity) && this.canWrite(entity) && entity.component.persistsState === true;
    }

    canManageAccessPolicies(): boolean {
        // TODO
        return false;
    }

    select(entity: ControllerServiceEntity): void {
        this.selectControllerService.next(entity);
    }

    isSelected(entity: ControllerServiceEntity): boolean {
        if (this.selectedServiceId) {
            return entity.id == this.selectedServiceId;
        }
        return false;
    }
}
