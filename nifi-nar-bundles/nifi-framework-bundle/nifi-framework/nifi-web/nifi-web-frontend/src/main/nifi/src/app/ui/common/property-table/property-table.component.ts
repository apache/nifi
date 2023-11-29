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

import {
    AfterViewInit,
    ChangeDetectorRef,
    Component,
    DestroyRef,
    forwardRef,
    inject,
    Input,
    QueryList,
    ViewChildren
} from '@angular/core';
import { ControlValueAccessor, NG_VALUE_ACCESSOR } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { NiFiCommon } from '../../../service/nifi-common.service';
import { AsyncPipe, NgIf, NgTemplateOutlet } from '@angular/common';
import {
    AllowableValueEntity,
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    Parameter,
    Property,
    PropertyDependency,
    PropertyDescriptor,
    PropertyTipInput,
    TextTipInput
} from '../../../state/shared';
import { NifiTooltipDirective } from '../tooltips/nifi-tooltip.directive';
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
import { Observable, take } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { RouterLink } from '@angular/router';

export interface PropertyItem extends Property {
    id: number;
    triggerEdit: boolean;
    deleted: boolean;
    dirty: boolean;
    added: boolean;
    type: 'required' | 'userDefined' | 'optional';
    serviceLink?: string[];
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
        ComboEditor,
        RouterLink,
        AsyncPipe
    ],
    styleUrls: ['./property-table.component.scss'],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => PropertyTable),
            multi: true
        }
    ]
})
export class PropertyTable implements AfterViewInit, ControlValueAccessor {
    @Input() createNewProperty!: (existingProperties: string[], allowsSensitive: boolean) => Observable<Property>;
    @Input() createNewService!: (request: InlineServiceCreationRequest) => Observable<InlineServiceCreationResponse>;
    @Input() getParameters!: (sensitive: boolean) => Observable<Parameter[]>;
    @Input() getServiceLink!: (serviceId: string) => Observable<string[]>;
    @Input() supportsSensitiveDynamicProperties: boolean = false;

    private destroyRef = inject(DestroyRef);

    protected readonly NfEditor = NfEditor;
    protected readonly TextTip = TextTip;
    protected readonly PropertyTip = PropertyTip;

    itemLookup: Map<string, PropertyItem> = new Map<string, PropertyItem>();
    displayedColumns: string[] = ['property', 'value', 'actions'];
    dataSource: MatTableDataSource<PropertyItem> = new MatTableDataSource<PropertyItem>();
    selectedItem!: PropertyItem;

    @ViewChildren('trigger') valueTriggers!: QueryList<CdkOverlayOrigin>;

    isDisabled: boolean = false;
    isTouched: boolean = false;
    onTouched!: () => void;
    onChange!: (properties: Property[]) => void;

    private originPos: OriginConnectionPosition = {
        originX: 'center',
        originY: 'center'
    };
    private editorOverlayPos: OverlayConnectionPosition = {
        overlayX: 'center',
        overlayY: 'center'
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

        this.valueTriggers.changes.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => {
            const item: PropertyItem | undefined = this.dataSource.data.find((item) => item.triggerEdit);

            if (item) {
                const valueTrigger: CdkOverlayOrigin | undefined = this.valueTriggers.find(
                    (valueTrigger: CdkOverlayOrigin) => {
                        return this.formatId(item) == valueTrigger.elementRef.nativeElement.getAttribute('id');
                    }
                );

                if (valueTrigger) {
                    // scroll into view
                    valueTrigger.elementRef.nativeElement.scrollIntoView({ block: 'center', behavior: 'instant' });

                    setTimeout(function () {
                        // trigger a click to start editing the new item
                        valueTrigger.elementRef.nativeElement.click();
                    }, 0);
                }

                item.triggerEdit = false;
            }
        });
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

    registerOnChange(onChange: (properties: Property[]) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        // TODO - update component to disable controls accordingly
        this.isDisabled = isDisabled;
    }

    writeValue(properties: Property[]): void {
        this.itemLookup.clear();

        let i: number = 0;
        const propertyItems: PropertyItem[] = properties.map((property) => {
            // create the property item
            const item: PropertyItem = {
                ...property,
                id: i++,
                triggerEdit: false,
                deleted: false,
                added: false,
                dirty: false,
                type: property.descriptor.required
                    ? 'required'
                    : property.descriptor.dynamic
                    ? 'userDefined'
                    : 'optional'
            };

            this.populateServiceLink(item);

            // store the property item in a map for an efficient lookup later
            this.itemLookup.set(property.property, item);
            return item;
        });

        this.setPropertyItems(propertyItems);
    }

    private populateServiceLink(item: PropertyItem): void {
        if (this.canGoTo(item) && item.value) {
            this.getServiceLink(item.value)
                .pipe(take(1))
                .subscribe((serviceLink: string[]) => {
                    item.serviceLink = serviceLink;
                });
        }
    }

    private setPropertyItems(propertyItems: PropertyItem[]): void {
        this.dataSource = new MatTableDataSource<PropertyItem>(propertyItems);
        this.initFilter();
    }

    newPropertyClicked(): void {
        const existingProperties: string[] = this.dataSource.data.map((item) => item.descriptor.name);
        this.createNewProperty(existingProperties, this.supportsSensitiveDynamicProperties)
            .pipe(take(1))
            .subscribe((property) => {
                const currentPropertyItems: PropertyItem[] = this.dataSource.data;

                const i: number = currentPropertyItems.length;
                const item: PropertyItem = {
                    ...property,
                    id: i,
                    triggerEdit: true,
                    deleted: false,
                    added: true,
                    dirty: true,
                    type: property.descriptor.required
                        ? 'required'
                        : property.descriptor.dynamic
                        ? 'userDefined'
                        : 'optional'
                };

                this.populateServiceLink(item);

                this.itemLookup.set(property.property, item);

                const propertyItems: PropertyItem[] = [...currentPropertyItems, item];
                this.setPropertyItems(propertyItems);

                this.handleChanged();
            });
    }

    formatId(item: PropertyItem): string {
        return 'property-' + item.id;
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
        const allowableValues: AllowableValueEntity[] | undefined = property.descriptor.allowableValues;
        if (allowableValues == null || this.nifiCommon.isEmpty(allowableValues)) {
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

    canGoTo(item: PropertyItem): boolean {
        // TODO - add Input() for supportsGoTo? currently only false in summary table

        const descriptor: PropertyDescriptor = item.descriptor;
        if (item.value && descriptor.identifiesControllerService && descriptor.allowableValues) {
            return descriptor.allowableValues.some(
                (entity: AllowableValueEntity) => entity.allowableValue.value == item.value
            );
        }

        return false;
    }

    createNewControllerService(item: PropertyItem): void {
        this.createNewService({ descriptor: item.descriptor })
            .pipe(take(1))
            .subscribe((response) => {
                item.value = response.value;
                item.descriptor = response.descriptor;
                item.dirty = true;

                this.handleChanged();
            });
    }

    deleteProperty(item: PropertyItem): void {
        if (!item.deleted) {
            item.value = null;
            item.deleted = true;
            item.dirty = true;

            this.handleChanged();
        }
    }

    savePropertyValue(item: PropertyItem, newValue: string): void {
        if (item.value != newValue) {
            item.value = newValue;
            item.dirty = true;

            this.populateServiceLink(item);

            this.handleChanged();
        }

        this.closeEditor();
    }

    private handleChanged() {
        // this is needed to trigger the filter to be reapplied
        this.dataSource._updateChangeSubscription();
        this.changeDetector.markForCheck();

        // mark the component as touched if not already
        if (!this.isTouched) {
            this.isTouched = true;
            this.onTouched();
        }

        // emit the changes
        this.onChange(this.serializeProperties());
    }

    private serializeProperties(): Property[] {
        const properties: PropertyItem[] = this.dataSource.data;

        // only include dirty items
        return properties
            .filter((item) => item.dirty)
            .filter((item) => !(item.added && item.deleted))
            .map((item) => {
                return {
                    property: item.property,
                    descriptor: item.descriptor,
                    value: item.value
                };
            });
    }

    closeEditor(): void {
        this.editorOpen = false;
    }

    selectProperty(item: PropertyItem): void {
        this.selectedItem = item;
    }

    isSelected(item: PropertyItem): boolean {
        if (this.selectedItem) {
            return item.property == this.selectedItem.property;
        }
        return false;
    }
}
