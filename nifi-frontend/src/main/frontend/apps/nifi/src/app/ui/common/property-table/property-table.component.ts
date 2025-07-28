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
import { NiFiCommon, NifiTooltipDirective, Parameter, TextTip } from '@nifi/shared';
import { NgTemplateOutlet } from '@angular/common';
import {
    AllowableValueEntity,
    ComponentHistory,
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    ParameterConfig,
    ParameterContextEntity,
    Property,
    PropertyDependency,
    PropertyDescriptor,
    PropertyTipInput
} from '../../../state/shared';
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
import { ConvertToParameterResponse } from '../../../pages/flow-designer/service/parameter-helper.service';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';
import { PropertyItem } from './property-item';

@Component({
    selector: 'property-table',
    templateUrl: './property-table.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        NifiTooltipDirective,
        NgTemplateOutlet,
        NfEditor,
        CdkOverlayOrigin,
        CdkConnectedOverlay,
        ComboEditor,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
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
    @Input() parameterContext: ParameterContextEntity | undefined;
    @Input() goToParameter!: (parameter: string) => void;
    @Input() convertToParameter!: (
        name: string,
        sensitive: boolean,
        value: string | null
    ) => Observable<ConvertToParameterResponse>;
    @Input() goToService!: (serviceId: string) => void;
    @Input() supportsSensitiveDynamicProperties = false;
    @Input() propertyHistory: ComponentHistory | undefined;
    @Input() supportsParameters: boolean = true;

    private static readonly PARAM_REF_REGEX: RegExp = /#{(['"]?)[a-zA-Z0-9-_. ]+\1}/;

    private destroyRef = inject(DestroyRef);

    protected readonly NfEditor = NfEditor;
    protected readonly TextTip = TextTip;
    protected readonly PropertyTip = PropertyTip;

    itemLookup: Map<string, PropertyItem> = new Map<string, PropertyItem>();
    displayedColumns: string[] = ['property', 'value', 'actions'];
    dataSource: MatTableDataSource<PropertyItem> = new MatTableDataSource<PropertyItem>();
    selectedItem!: PropertyItem;

    @ViewChildren('trigger') valueTriggers!: QueryList<CdkOverlayOrigin>;

    isDisabled = false;
    isTouched = false;
    onTouched!: () => void;
    onChange!: (properties: Property[]) => void;
    editorOpen = false;
    editorTrigger: any = null;
    editorItem!: PropertyItem;
    editorParameterConfig!: ParameterConfig;
    editorWidth = 0;
    editorOffsetX = 0;
    editorOffsetY = 0;

    private originPos: OriginConnectionPosition = {
        originX: 'center',
        originY: 'center'
    };
    private editorOverlayPos: OverlayConnectionPosition = {
        overlayX: 'center',
        overlayY: 'center'
    };
    public editorPositions: ConnectionPositionPair[] = [];

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

                    window.setTimeout(function () {
                        // trigger a click to start editing the new item
                        valueTrigger.elementRef.nativeElement.click();
                    }, 0);
                }

                item.triggerEdit = false;
            }
        });
    }

    initFilter(): void {
        this.dataSource.filterPredicate = (data: PropertyItem) => this.isVisible(data);
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

        // if the dependent item is sensitive, in this case we are lenient and
        // consider the dependency met
        if (dependentItem.descriptor.sensitive) {
            return true;
        }

        // ensure the dependent item has a value
        let dependentValue = dependentItem.value;
        if (dependentValue != null) {
            // check if the dependent value is a parameter reference
            if (PropertyTable.PARAM_REF_REGEX.test(dependentValue)) {
                // the dependent value contains parameter reference, if the user can view
                // the parameter context resolve the parameter value to see if it
                // satisfies the dependent values
                if (this.parameterContext?.permissions.canRead && this.parameterContext.component) {
                    const referencedParameter = this.parameterContext.component.parameters
                        .map((parameterEntity) => parameterEntity.parameter)
                        .find((parameter: Parameter) => dependentValue == `#{${parameter.name}}`);

                    // if we found a matching parameter then we'll use its value when determining if the
                    // dependency is satisfied. if a matching parameter was not found we'll continue using
                    // the dependent property value
                    if (referencedParameter) {
                        dependentValue = referencedParameter.value;
                    }
                } else {
                    // the user lacks permissions to the parameter context so we cannot
                    // verify if the dependency is satisfied, in this case we are lenient
                    // and consider the dependency met
                    return true;
                }
            }

            // ensure the dependent item has a value
            if (dependentValue != null) {
                if (this.nifiCommon.isEmpty(dependency.dependentValues)) {
                    // if the dependency does not require a specific value, consider the dependency met
                    return true;
                } else {
                    // see if value is present in the allowed dependent values. if so, consider the dependency met.
                    return dependency.dependentValues.includes(dependentValue);
                }
            }
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
        this.isDisabled = isDisabled;
    }

    writeValue(properties: Property[]): void {
        this.itemLookup.clear();

        let i = 0;
        const propertyItems: PropertyItem[] = properties.map((property) => {
            // create the property item
            const item: PropertyItem = {
                ...property,
                id: i++,
                triggerEdit: false,
                deleted: false,
                added: false,
                dirty: false,
                savedValue: property.value,
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

        this.setPropertyItems(propertyItems);
    }

    private setPropertyItems(propertyItems: PropertyItem[]): void {
        this.dataSource = new MatTableDataSource<PropertyItem>(propertyItems);
        this.initFilter();
    }

    private getParameterConfig(propertyItem: PropertyItem): ParameterConfig {
        return {
            supportsParameters: this.supportsParameters,
            parameters: this.getParametersForItem(propertyItem)
        };
    }

    private getParametersForItem(propertyItem: PropertyItem): Parameter[] | null {
        if (!this.supportsParameters || !this.parameterContext) {
            return null;
        }
        if (this.parameterContext.permissions.canRead && this.parameterContext.component) {
            return this.parameterContext.component.parameters
                .map((parameterEntity) => parameterEntity.parameter)
                .filter((parameter: Parameter) => parameter.sensitive == propertyItem.descriptor.sensitive);
        } else {
            return [];
        }
    }

    newPropertyClicked(): void {
        // filter out deleted properties in case the user needs to re-add one
        const existingProperties: string[] = this.dataSource.data
            .filter((item) => !item.deleted)
            .map((item) => item.descriptor.name);

        // create the new property
        this.createNewProperty(existingProperties, this.supportsSensitiveDynamicProperties)
            .pipe(take(1))
            .subscribe((property) => {
                const currentPropertyItems: PropertyItem[] = this.dataSource.data;

                const itemIndex: number = currentPropertyItems.findIndex(
                    (existingItem: PropertyItem) => existingItem.property == property.property
                );
                if (itemIndex > -1) {
                    const currentItem: PropertyItem = currentPropertyItems[itemIndex];
                    const updatedItem: PropertyItem = {
                        ...currentItem,
                        ...property,
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

                    this.itemLookup.set(property.property, updatedItem);

                    // if the user had previously deleted the property, replace the matching property item
                    currentPropertyItems[itemIndex] = updatedItem;
                } else {
                    const i: number = currentPropertyItems.length;
                    const item: PropertyItem = {
                        ...property,
                        id: i,
                        triggerEdit: true,
                        deleted: false,
                        added: true,
                        dirty: true,
                        savedValue: property.value,
                        type: property.descriptor.required
                            ? 'required'
                            : property.descriptor.dynamic
                              ? 'userDefined'
                              : 'optional'
                    };

                    this.itemLookup.set(property.property, item);

                    // if this is a new property, add it to the list
                    this.setPropertyItems([...currentPropertyItems, item]);
                }

                this.handleChanged();
            });
    }

    formatId(item: PropertyItem): string {
        return 'property-' + item.id;
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

    getPropertyTipData(item: PropertyItem): PropertyTipInput {
        return {
            descriptor: item.descriptor,
            propertyHistory: this.propertyHistory?.propertyHistory[item.property]
        };
    }

    hasAllowableValues(item: PropertyItem): boolean {
        return Array.isArray(item.descriptor.allowableValues);
    }

    openEditor(editorTrigger: any, item: PropertyItem, event: MouseEvent): void {
        if (event.target) {
            const target: HTMLElement = event.target as HTMLElement;

            // find the table cell regardless of the target of the click
            const td: HTMLElement | null = target.closest('td');
            if (td) {
                const { width } = td.getBoundingClientRect();

                this.editorPositions.pop();
                this.editorItem = item;
                this.editorParameterConfig = this.getParameterConfig(this.editorItem);
                this.editorTrigger = editorTrigger;
                this.editorOpen = true;

                if (this.hasAllowableValues(item)) {
                    this.editorWidth = width + 50;
                    this.editorOffsetX = 0;
                    this.editorOffsetY = 24;
                } else {
                    this.editorWidth = width + 100;
                    this.editorOffsetX = 8;
                    this.editorOffsetY = 66;
                }
                this.editorPositions.push(
                    new ConnectionPositionPair(
                        this.originPos,
                        this.editorOverlayPos,
                        this.editorOffsetX,
                        this.editorOffsetY
                    )
                );
                this.changeDetector.detectChanges();
            }
        }
    }

    canGoToService(item: PropertyItem): boolean {
        const descriptor: PropertyDescriptor = item.descriptor;
        if (item.value && descriptor.identifiesControllerService && descriptor.allowableValues) {
            return descriptor.allowableValues.some(
                (entity: AllowableValueEntity) => entity.allowableValue.value == item.value
            );
        }

        return false;
    }

    goToServiceClicked(item: PropertyItem): void {
        // @ts-ignore
        this.goToService(item.value);
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

    canGoToParameter(item: PropertyItem): boolean {
        // TODO - currently parameter context route does not support navigating
        // directly to a specific parameter so the parameter context link
        // is not item specific.
        if (this.parameterContext && item.value) {
            return this.parameterContext.permissions.canRead && PropertyTable.PARAM_REF_REGEX.test(item.value);
        }

        return false;
    }

    goToParameterClicked(item: PropertyItem): void {
        // @ts-ignore
        this.goToParameter(item.value);
    }

    canConvertToParameter(item: PropertyItem): boolean {
        let canUpdateParameterContext = false;
        if (this.parameterContext) {
            canUpdateParameterContext =
                this.parameterContext.permissions.canRead && this.parameterContext.permissions.canWrite;
        }

        let propertyReferencesParameter = false;
        if (canUpdateParameterContext && item.value) {
            propertyReferencesParameter = PropertyTable.PARAM_REF_REGEX.test(item.value);
        }

        return canUpdateParameterContext && !propertyReferencesParameter;
    }

    convertToParameterClicked(item: PropertyItem): void {
        this.convertToParameter(item.descriptor.displayName, item.descriptor.sensitive, item.value)
            .pipe(take(1))
            .subscribe((response) => {
                item.value = response.propertyValue;
                item.dirty = true;

                // update the parameter context which includes any new parameters
                if (this.parameterContext && response.parameterContext) {
                    this.parameterContext.component = response.parameterContext;
                }

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

    savePropertyValue(item: PropertyItem, newValue: string | null): void {
        if (item.value != newValue) {
            if (!item.savedValue) {
                item.savedValue = item.value;
            }

            item.value = newValue;
            item.dirty = true;

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
