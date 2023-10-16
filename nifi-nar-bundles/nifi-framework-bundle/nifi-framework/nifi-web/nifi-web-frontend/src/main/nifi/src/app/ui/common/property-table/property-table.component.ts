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

import { AfterViewInit, ChangeDetectorRef, Component, Input } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { NiFiCommon } from '../../../service/nifi-common.service';
import { NgIf, NgTemplateOutlet } from '@angular/common';
import {
    AllowableValueEntity,
    Properties,
    Property,
    PropertyDependency,
    PropertyDescriptor,
    PropertyTipInput,
    TextTipInput
} from '../../../state/shared';
import { NifiTooltipDirective } from '../nifi-tooltip.directive';
import { TextTip } from '../tooltips/text-tip/text-tip.component';
import { PropertyTip } from '../tooltips/property-tip/property-tip.component';
import { NfEditor } from './editors/nf-editor/nf-editor.component';
import {
    CdkConnectedOverlay,
    CdkOverlayOrigin,
    ConnectionPositionPair,
    OriginConnectionPosition,
    OverlayConnectionPosition
} from '@angular/cdk/overlay';
import { ComboEditor } from './editors/combo-editor/combo-editor.component';

export interface PropertyItem extends Property {
    deleted: boolean;
    type: 'required' | 'userDefined' | 'optional';
}

@Component({
    selector: 'property-table',
    standalone: true,
    templateUrl: './property-table.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        NgIf,
        NifiTooltipDirective,
        NgTemplateOutlet,
        NfEditor,
        CdkOverlayOrigin,
        CdkConnectedOverlay,
        ComboEditor
    ],
    styleUrls: ['./property-table.component.scss']
})
export class PropertyTable implements AfterViewInit {
    @Input() set properties(properties: Properties) {
        this.itemLookup.clear();

        const propertyItems: PropertyItem[] = properties.properties.map((property) => {
            // create the property item
            const item: PropertyItem = {
                ...property,
                deleted: false,
                type: property.descriptor.required
                    ? 'required'
                    : property.descriptor.dynamic
                    ? 'userDefined'
                    : 'optional'
            };

            // store the property item in a map for an efficient lookup later
            this.itemLookup.set(property.property, item);
            return item;
        });

        this.dataSource = new MatTableDataSource<PropertyItem>(propertyItems);
        this.initFilter();
    }

    protected readonly NfEditor = NfEditor;
    protected readonly TextTip = TextTip;
    protected readonly PropertyTip = PropertyTip;

    itemLookup: Map<string, PropertyItem> = new Map<string, PropertyItem>();
    displayedColumns: string[] = ['property', 'value', 'actions'];
    dataSource: MatTableDataSource<PropertyItem> = new MatTableDataSource<PropertyItem>();

    private originPos: OriginConnectionPosition = {
        originX: 'start',
        originY: 'top'
    };
    private editorOverlayPos: OverlayConnectionPosition = {
        overlayX: 'start',
        overlayY: 'top'
    };
    private editorPosition: ConnectionPositionPair = new ConnectionPositionPair(
        this.originPos,
        this.editorOverlayPos,
        0,
        0
    );
    public editorPositions: ConnectionPositionPair[] = [this.editorPosition];
    editorOpen: boolean = false;
    editorTrigger: any = null;
    editorItem!: PropertyItem;

    constructor(
      private changeDetector: ChangeDetectorRef,
      private nifiCommon: NiFiCommon
  ) {}

    ngAfterViewInit(): void {
        this.initFilter();
    }

    initFilter(): void {
        this.dataSource.filterPredicate = (data: PropertyItem, filter: string) => this.isVisible(data);
        this.dataSource.filter = ' ';
    }

    isVisible(item: PropertyItem): boolean {
        if (item.deleted) {
            return false;
        }

        if (this.nifiCommon.isEmpty(item.descriptor.dependencies)) {
            return true;
        }

        return this.arePropertyDependenciesSatisfied(item, item.descriptor.dependencies);
    }

    arePropertyDependenciesSatisfied(item: PropertyItem, dependencies: PropertyDependency[]): boolean {
        for (const dependency of dependencies) {
            if (!this.isPropertyDependencySatisfied(dependency)) {
                return false;
            }
        }
        return true;
    }

    isPropertyDependencySatisfied(dependency: PropertyDependency): boolean {
        const dependentItem: PropertyItem | undefined = this.itemLookup.get(dependency.propertyName);

        // if the dependent item is not found, consider the dependency not met
        if (!dependentItem) {
            return false;
        }

        // if the dependent item is not visible, consider the dependency not met
        if (!this.isVisible(dependentItem)) {
            return false;
        }

        // if the dependent item is visible, but does not require a specific
        // dependent value consider the dependency met
        if (this.nifiCommon.isEmpty(dependency.dependentValues)) {
            return true;
        }

        // TODO resolve parameter value in dependentItem if necessary

        // if the dependent item has a value, see if it is present in the
        // allowed dependent value. if so, consider the dependency met
        if (dependentItem.value) {
            return dependency.dependentValues.includes(dependentItem.value);
        }

        // if the dependent item does not have a value, consider the
        // dependency not met
        return false;
    }

    hasInfo(descriptor: PropertyDescriptor): boolean {
        return (
            !this.nifiCommon.isBlank(descriptor.description) ||
            !this.nifiCommon.isBlank(descriptor.defaultValue) ||
            descriptor.supportsEl
        );
    }

    isSensitiveProperty(descriptor: PropertyDescriptor): boolean {
        return descriptor.sensitive;
    }

    isNull(value: string): boolean {
        return value == null;
    }

    isEmptyString(value: string): boolean {
        return value == '';
    }

    resolvePropertyValue(property: Property): string | null {
        const allowableValues: AllowableValueEntity[] = property.descriptor.allowableValues;
        if (this.nifiCommon.isEmpty(allowableValues)) {
            return property.value;
        } else {
            const allowableValue: AllowableValueEntity | undefined = allowableValues.find(
                (av) => av.allowableValue.value == property.value
            );
            if (allowableValue) {
                return allowableValue.allowableValue.displayName;
            } else {
                return property.value;
            }
        }
    }

    hasExtraWhitespace(value: string): boolean {
        return this.nifiCommon.hasLeadTrailWhitespace(value);
    }

    getExtraWhitespaceTipData(): TextTipInput {
        return {
            text: 'The specified value contains leading and/or trailing whitespace character(s). This could produce unexpected results if it was not intentional.'
        };
    }

    getPropertyTipData(item: PropertyItem): PropertyTipInput {
        return {
            descriptor: item.descriptor
        };
    }

    hasAllowableValues(item: PropertyItem): boolean {
        return Array.isArray(item.descriptor.allowableValues);
    }

    openEditor(editorTrigger: any, item: PropertyItem): void {
        this.editorItem = item;
        this.editorTrigger = editorTrigger;
        this.editorOpen = true;
    }

    savePropertyValue(item: PropertyItem, newValue: string): void {
        item.value = newValue;

        // this is needed to trigger the filter to be reapplied
        this.dataSource._updateChangeSubscription();
        this.changeDetector.markForCheck();

        this.closeEditor();
    }

    closeEditor(): void {
        this.editorOpen = false;
    }

    selectProperty(property: any): void {}

    isSelected(property: any): boolean {
        // if (this.selectedType) {
        //     return documentedType.type == this.selectedType.type;
        // }
        return false;
    }
}
