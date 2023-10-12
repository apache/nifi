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
import { NiFiCommon } from '../../../service/nifi-common.service';
import { MatSort, MatSortModule } from '@angular/material/sort';
import { NgIf } from '@angular/common';
import { DocumentedType, RestrictionsTipInput } from '../../../state/shared';
import { NifiTooltipDirective } from '../nifi-tooltip.directive';
import { RestrictionsTip } from '../tooltips/restrictions-tip/restrictions-tip.component';

@Component({
    selector: 'extension-creation',
    standalone: true,
    templateUrl: './extension-creation.component.html',
    imports: [MatButtonModule, MatDialogModule, MatTableModule, MatSortModule, NgIf, NifiTooltipDirective],
    styleUrls: ['./extension-creation.component.scss']
})
export class ExtensionCreation implements AfterViewInit {
    @Input() set documentedTypes(documentedTypes: DocumentedType[]) {
        if (this.selectedType == null && documentedTypes.length > 0) {
            this.selectedType = documentedTypes[0];
        }

        this.dataSource = new MatTableDataSource<DocumentedType>(documentedTypes);
        this.dataSource.sort = this.sort;
        this.dataSource.sortingDataAccessor = (data: DocumentedType, displayColumn: string) => {
            if (displayColumn == 'type') {
                return this.formatType(data);
            } else if (displayColumn == 'version') {
                return this.formatVersion(data);
            } else if (displayColumn == 'tags') {
                return this.formatTags(data);
            }
            return '';
        };
    }
    @Input() componentType!: string;
    @Output() extensionTypeSelected: EventEmitter<DocumentedType> = new EventEmitter<DocumentedType>();

    protected readonly RestrictionsTip = RestrictionsTip;

    displayedColumns: string[] = ['type', 'version', 'tags'];
    dataSource: MatTableDataSource<DocumentedType> = new MatTableDataSource<DocumentedType>();
    selectedType: DocumentedType | null = null;

    @ViewChild(MatSort) sort!: MatSort;

    constructor(private nifiCommon: NiFiCommon) {}

    ngAfterViewInit(): void {
        this.dataSource.sort = this.sort;
    }

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

    formatTags(documentedType: DocumentedType): string {
        if (documentedType?.tags) {
            return documentedType.tags.join(', ');
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
        if (documentedType) {
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
        if (documentedType) {
            this.extensionTypeSelected.next(documentedType);
        }
    }
}
