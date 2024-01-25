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
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { NiFiCommon } from '../../../service/nifi-common.service';
import { MatSortModule, Sort } from '@angular/material/sort';
import { NgIf } from '@angular/common';
import { ControllerServiceApiTipInput, DocumentedType, RestrictionsTipInput } from '../../../state/shared';
import { NifiTooltipDirective } from '../tooltips/nifi-tooltip.directive';
import { RestrictionsTip } from '../tooltips/restrictions-tip/restrictions-tip.component';
import { ControllerServiceApiTip } from '../tooltips/controller-service-api-tip/controller-service-api-tip.component';
import { NifiSpinnerDirective } from '../spinner/nifi-spinner.directive';

@Component({
    selector: 'extension-creation',
    standalone: true,
    templateUrl: './extension-creation.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        MatSortModule,
        NgIf,
        NifiTooltipDirective,
        NifiSpinnerDirective
    ],
    styleUrls: ['./extension-creation.component.scss']
})
export class ExtensionCreation {
    @Input() set documentedTypes(documentedTypes: DocumentedType[]) {
        if (this.selectedType == null && documentedTypes.length > 0) {
            this.selectedType = documentedTypes[0];
        }

        this.dataSource.data = this.sortEntities(documentedTypes, {
            active: this.initialSortColumn,
            direction: this.initialSortDirection
        });
    }

    @Input() componentType!: string;
    @Input() saving!: boolean;
    @Input() initialSortColumn: 'type' | 'version' | 'tags' = 'type';
    @Input() initialSortDirection: 'asc' | 'desc' = 'asc';

    @Output() extensionTypeSelected: EventEmitter<DocumentedType> = new EventEmitter<DocumentedType>();

    protected readonly RestrictionsTip = RestrictionsTip;
    protected readonly ControllerServiceApiTip = ControllerServiceApiTip;

    displayedColumns: string[] = ['type', 'version', 'tags'];
    dataSource: MatTableDataSource<DocumentedType> = new MatTableDataSource<DocumentedType>();
    selectedType: DocumentedType | null = null;

    constructor(private nifiCommon: NiFiCommon) {}

    formatType(documentedType: DocumentedType): string {
        if (documentedType) {
            return this.nifiCommon.substringAfterLast(documentedType.type, '.');
        }
        return '';
    }

    getRestrictionTipData(documentedType: DocumentedType): RestrictionsTipInput {
        return {
            usageRestriction: documentedType.usageRestriction!,
            explicitRestrictions: documentedType.explicitRestrictions!
        };
    }

    formatVersion(documentedType: DocumentedType): string {
        if (documentedType) {
            return documentedType.bundle.version;
        }
        return '';
    }

    implementsControllerService(documentedType: DocumentedType): boolean {
        return !this.nifiCommon.isEmpty(documentedType.controllerServiceApis);
    }

    getControllerServiceApiTipData(documentedType: DocumentedType): ControllerServiceApiTipInput {
        return {
            // @ts-ignore
            controllerServiceApis: documentedType.controllerServiceApis
        };
    }

    formatTags(documentedType: DocumentedType): string {
        if (documentedType?.tags) {
            return documentedType.tags
                .slice()
                .sort((a, b) => this.nifiCommon.compareString(a, b))
                .join(', ');
        }
        return '';
    }

    formatBundle(documentedType: DocumentedType): string {
        if (documentedType) {
            return this.nifiCommon.formatBundle(documentedType.bundle);
        }
        return '';
    }

    formatDescription(documentedType: DocumentedType): string {
        if (documentedType && documentedType.description) {
            return documentedType.description;
        }
        return '';
    }

    filterTypes(event: Event): void {
        const filterText: string = (event.target as HTMLInputElement).value;
        this.dataSource.filter = filterText.trim().toLowerCase();
        this.selectedType = null;
    }

    selectType(documentedType: DocumentedType): void {
        this.selectedType = documentedType;
    }

    isSelected(documentedType: DocumentedType): boolean {
        if (this.selectedType) {
            return documentedType.type == this.selectedType.type;
        }
        return false;
    }

    createExtension(documentedType: DocumentedType | null): void {
        if (documentedType && !this.saving) {
            this.extensionTypeSelected.next(documentedType);
        }
    }

    sortData(sort: Sort) {
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    sortEntities(data: DocumentedType[], sort: Sort): DocumentedType[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'type':
                    retVal = this.nifiCommon.compareString(this.formatType(a), this.formatType(b));
                    break;
                case 'version':
                    retVal = this.nifiCommon.compareString(this.formatVersion(a), this.formatVersion(b));
                    break;
                case 'tags':
                    retVal = this.nifiCommon.compareString(this.formatTags(a), this.formatTags(b));
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }
}
