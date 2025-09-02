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

import { AfterViewInit, ChangeDetectorRef, Component, forwardRef, Input } from '@angular/core';
import { ControlValueAccessor, FormsModule, NG_VALUE_ACCESSOR } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { NgTemplateOutlet } from '@angular/common';
import { RouterLink } from '@angular/router';
import { EditParameterResponse, ParameterEntity } from '../../../../../state/shared';
import { NifiTooltipDirective, NiFiCommon, TextTip, Parameter } from '@nifi/shared';
import { Observable, take } from 'rxjs';
import { ParameterReferences } from '../../../../../ui/common/parameter-references/parameter-references.component';
import { Store } from '@ngrx/store';
import { ParameterContextListingState } from '../../../state/parameter-context-listing';
import { showOkDialog } from '../../../state/parameter-context-listing/parameter-context-listing.actions';
import { MatSortModule, Sort } from '@angular/material/sort';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';
import { MatCheckbox } from '@angular/material/checkbox';
import { MatLabel } from '@angular/material/select';

export interface ParameterItem {
    deleted: boolean;
    dirty: boolean;
    added: boolean;
    originalEntity: ParameterEntity; // either new or existing from server
    updatedEntity?: ParameterEntity;
}

@Component({
    selector: 'parameter-table',
    templateUrl: './parameter-table.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        MatSortModule,
        NgTemplateOutlet,
        RouterLink,
        NifiTooltipDirective,
        ParameterReferences,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger,
        MatLabel,
        MatCheckbox,
        FormsModule
    ],
    styleUrls: ['./parameter-table.component.scss'],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: forwardRef(() => ParameterTable),
            multi: true
        }
    ]
})
export class ParameterTable implements AfterViewInit, ControlValueAccessor {
    @Input() createNewParameter!: (existingParameters: string[]) => Observable<EditParameterResponse>;
    @Input() editParameter!: (parameter: Parameter) => Observable<EditParameterResponse>;
    @Input() canAddParameters = true;
    @Input() inheritsParameters = false;

    protected readonly TextTip = TextTip;

    initialSortColumn = 'name';
    initialSortDirection: 'asc' | 'desc' = 'asc';

    displayedColumns: string[] = ['name', 'value', 'actions'];
    dataSource: MatTableDataSource<ParameterItem> = new MatTableDataSource<ParameterItem>();
    selectedItem: ParameterItem | null = null;
    activeSort: Sort = {
        active: this.initialSortColumn,
        direction: this.initialSortDirection
    };
    isDisabled = false;
    isTouched = false;
    onTouched!: () => void;
    onChange!: (parameters: ParameterEntity[]) => void;

    showInheritedParameters: boolean = true;

    constructor(
        private store: Store<ParameterContextListingState>,
        private changeDetector: ChangeDetectorRef,
        private nifiCommon: NiFiCommon
    ) {}

    ngAfterViewInit(): void {
        this.initFilter();
    }

    initFilter(): void {
        this.dataSource.filterPredicate = (data: ParameterItem) => this.isVisible(data);
        this.dataSource.filter = ' ';
    }

    isVisible(item: ParameterItem): boolean {
        if (item.deleted) {
            return false;
        }

        if (
            !this.showInheritedParameters &&
            item.originalEntity.parameter.inherited &&
            !item.updatedEntity?.parameter
        ) {
            return false;
        }

        return true;
    }

    registerOnChange(onChange: (parameters: ParameterEntity[]) => void): void {
        this.onChange = onChange;
    }

    registerOnTouched(onTouch: () => void): void {
        this.onTouched = onTouch;
    }

    setDisabledState(isDisabled: boolean): void {
        this.isDisabled = isDisabled;
    }

    writeValue(parameters: ParameterEntity[]): void {
        const propertyItems: ParameterItem[] = parameters.map((entity) => {
            const item: ParameterItem = {
                deleted: false,
                added: false,
                dirty: false,
                originalEntity: {
                    ...entity,
                    parameter: {
                        ...entity.parameter,
                        value: entity.parameter.value === undefined ? null : entity.parameter.value
                    }
                }
            };

            return item;
        });

        this.setPropertyItems(propertyItems);
    }

    sortData(sort: Sort) {
        this.activeSort = sort;
        this.dataSource.data = this.sortEntities(this.dataSource.data, sort);
    }

    private setPropertyItems(parameterItems: ParameterItem[]): void {
        this.dataSource.data = this.sortEntities(parameterItems, this.activeSort);
        this.initFilter();
    }

    private sortEntities(parameters: ParameterItem[], sort: Sort): ParameterItem[] {
        if (!parameters) {
            return [];
        }
        return parameters.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'name':
                    retVal = this.nifiCommon.compareString(
                        a.originalEntity.parameter.name,
                        b.originalEntity.parameter.name
                    );
                    break;
                default:
                    return 0;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    newParameterClicked(): void {
        // get the existing parameters to provide to the new parameter dialog but
        // exclude any items that are currently marked for deletion which can be
        // unmarked for deletion if the user chooses to enter the same name
        const existingParameters: string[] = this.dataSource.data
            .filter((item) => !item.deleted)
            .filter((item) => !item.originalEntity.parameter.inherited)
            .map((item) => item.originalEntity.parameter.name);

        this.createNewParameter(existingParameters)
            .pipe(take(1))
            .subscribe((response: EditParameterResponse) => {
                const parameter: Parameter = response.parameter;

                const currentParameterItems: ParameterItem[] = this.dataSource.data;

                // identify if a parameter with the same name already exists (must have been marked
                // for deletion already)
                const item: ParameterItem | undefined = currentParameterItems.find(
                    (item) => item.originalEntity.parameter.name === parameter.name
                );

                if (item) {
                    // if the item is added that means it hasn't been saved yet. in this case, we
                    // can simply update the existing parameter. if the item has been saved, and the
                    // sensitivity has changed, the user must apply the changes first.
                    if (!item.added && item.originalEntity.parameter.sensitive !== parameter.sensitive) {
                        this.store.dispatch(
                            showOkDialog({
                                title: 'Parameter Exists',
                                message:
                                    'A parameter with this name has been marked for deletion. Please apply this change to delete this parameter from the parameter context before recreating it with a different sensitivity.'
                            })
                        );
                        return;
                    }

                    // update the existing item
                    item.deleted = false;
                    item.dirty = true;

                    // since the item is either new or the sensitivity is the same, accept the sensitivity
                    // from the response to be set on the entities
                    item.originalEntity.parameter.sensitive = parameter.sensitive;
                    if (item.updatedEntity) {
                        item.updatedEntity.parameter.sensitive = parameter.sensitive;
                    }

                    this.applyParameterEdit(item, response);
                } else {
                    const newItem: ParameterItem = {
                        deleted: false,
                        added: true,
                        dirty: true,
                        originalEntity: {
                            canWrite: true,
                            parameter: {
                                ...parameter
                            }
                        }
                    };
                    const parameterItems: ParameterItem[] = [...currentParameterItems, newItem];
                    this.setPropertyItems(parameterItems);
                }

                this.handleChanged();
            });
    }

    hasDescription(item: ParameterItem): boolean {
        return !this.nifiCommon.isBlank(this.getDescription(item));
    }

    getDescription(item: ParameterItem): string {
        if (item.updatedEntity) {
            return item.updatedEntity.parameter.description;
        } else {
            return item.originalEntity.parameter.description;
        }
    }

    getInheritedParameterMessage(item: ParameterItem): string {
        if (item.originalEntity.parameter.inherited) {
            const parameterContext = item.originalEntity.parameter.parameterContext;
            if (parameterContext?.permissions.canRead && parameterContext.component) {
                return `This parameter is inherited from: ${parameterContext.component.name}`;
            } else {
                return 'This parameter is inherited from another Parameter Context.';
            }
        }
        return '';
    }

    isSensitiveParameter(item: ParameterItem): boolean {
        return item.originalEntity.parameter.sensitive;
    }

    getValue(item: ParameterItem): string | null {
        if (item.updatedEntity) {
            if (item.updatedEntity.parameter.valueRemoved) {
                return null;
            } else {
                // if there is an updated value use that, otherwise the original
                if (item.updatedEntity.parameter.value !== null) {
                    return item.updatedEntity.parameter.value;
                } else {
                    return item.originalEntity.parameter.value;
                }
            }
        } else {
            return item.originalEntity.parameter.value;
        }
    }

    isNull(value: string): boolean {
        return value == null;
    }

    isEmptyString(value: string): boolean {
        return value == '';
    }

    hasExtraWhitespace(value: string): boolean {
        return this.nifiCommon.hasLeadTrailWhitespace(value);
    }

    canGoToParameter(item: ParameterItem): boolean {
        return this.canOverride(item) && item.originalEntity.parameter.parameterContext?.permissions.canRead == true;
    }

    getParameterLink(item: ParameterItem): string[] {
        if (item.originalEntity.parameter.parameterContext) {
            // TODO - support routing directly to a parameter
            return ['/parameter-contexts', item.originalEntity.parameter.parameterContext.id, 'edit'];
        }
        return [];
    }

    isOverridden(item: ParameterItem): boolean {
        return item.originalEntity.parameter.inherited === true && item.updatedEntity !== undefined;
    }

    canOverride(item: ParameterItem): boolean {
        return item.originalEntity.parameter.inherited === true && item.updatedEntity === undefined;
    }

    overrideParameter(item: ParameterItem): void {
        const overriddenParameter: Parameter = {
            ...item.originalEntity.parameter,
            value: null
        };

        this.editParameter(overriddenParameter)
            .pipe(take(1))
            .subscribe((response) => {
                item.dirty = true;
                item.updatedEntity = {
                    parameter: {
                        ...response.parameter
                    }
                };

                this.handleChanged();
            });
    }

    canEdit(item: ParameterItem): boolean {
        const canWrite: boolean = item.originalEntity.canWrite == true;
        const provided: boolean = item.originalEntity.parameter.provided == true;

        if (item.originalEntity.parameter.inherited) {
            const overridden: boolean = this.isOverridden(item);
            return canWrite && !provided && overridden;
        }

        return canWrite && !provided;
    }

    editClicked(item: ParameterItem): void {
        const parameterToEdit: Parameter = {
            name: item.originalEntity.parameter.name,
            sensitive: item.originalEntity.parameter.sensitive,
            description: this.getDescription(item),
            value: this.getValue(item)
        };

        this.editParameter(parameterToEdit)
            .pipe(take(1))
            .subscribe((response) => {
                this.applyParameterEdit(item, response);
            });
    }

    private applyParameterEdit(item: ParameterItem, response: EditParameterResponse) {
        const parameter: Parameter = response.parameter;

        // initialize the updated entity if this is the first time this has been edited
        if (!item.updatedEntity) {
            item.updatedEntity = {
                parameter: {
                    ...item.originalEntity.parameter
                }
            };

            // if this parameter is an existing parameter, we want to mark the value as null. a null value
            // for existing parameters will indicate to the server that the value is unchanged. this is needed
            // for sensitive parameters where the value isn't available client side. for new parameters which
            // are not known on the server should not have their value cleared. this is relevant when the user
            // created a new parameter and then subsequents edits it (e.g. to change the description) before
            // submission.
            if (!item.added) {
                item.updatedEntity.parameter.value = null;
            }
        }

        let hasChanged: boolean = response.valueChanged;

        if (response.valueChanged) {
            item.updatedEntity.parameter.value = parameter.value;

            // a value has been specified so record it in the updated entity
            if (parameter.value !== null) {
                item.updatedEntity.parameter.valueRemoved = undefined;
                item.updatedEntity.parameter.referencedAssets = undefined;
            } else {
                // the value is null, this means that the value should be unchanged
                // or that the value has been removed. if removed we should
                // clear our the value.
                if (parameter.valueRemoved === true) {
                    item.updatedEntity.parameter.value = null;
                    item.updatedEntity.parameter.valueRemoved = true;
                    item.updatedEntity.parameter.referencedAssets = undefined;
                }
            }
        }

        if (item.updatedEntity.parameter.description !== parameter.description) {
            hasChanged = true;
            item.updatedEntity.parameter.description = parameter.description;
        }

        // if any aspect of the parameter has changed, mark it dirty and trigger handle changed
        if (hasChanged) {
            item.dirty = true;
            this.handleChanged();
        }
    }

    canDelete(item: ParameterItem): boolean {
        const canWrite: boolean = item.originalEntity.canWrite == true;
        const provided: boolean = item.originalEntity.parameter.provided == true;

        if (item.originalEntity.parameter.inherited) {
            const overridden: boolean = this.isOverridden(item);
            return canWrite && !provided && overridden;
        }

        return canWrite && !provided;
    }

    deleteClicked(item: ParameterItem): void {
        if (!item.deleted) {
            if (!item.updatedEntity) {
                item.updatedEntity = {
                    parameter: {
                        ...item.originalEntity.parameter
                    }
                };
            }

            item.updatedEntity.parameter.value = null;
            item.updatedEntity.parameter.referencedAssets = undefined;

            item.deleted = true;
            item.dirty = true;
            this.selectParameter(null);
            this.handleChanged();
        }
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
        this.onChange(this.serializeParameters());
    }

    /**
     * Serializes the Parameters. Not private for testing purposes.
     */
    serializeParameters(): any[] {
        const parameters: ParameterItem[] = this.dataSource.data;

        // only include dirty items
        return parameters
            .filter((item) => item.dirty)
            .filter((item) => !(item.added && item.deleted))
            .map((item) => {
                if (item.deleted) {
                    // item is deleted
                    return {
                        parameter: {
                            name: item.originalEntity.parameter.name
                        }
                    };
                } else if (item.updatedEntity) {
                    // item has been edited
                    return {
                        parameter: {
                            name: item.originalEntity.parameter.name,
                            sensitive: item.originalEntity.parameter.sensitive,
                            description: item.updatedEntity.parameter.description,
                            value: item.updatedEntity.parameter.value,
                            valueRemoved: item.updatedEntity.parameter.valueRemoved,
                            referencedAssets: item.updatedEntity.parameter.referencedAssets
                        }
                    };
                } else {
                    // item is added (but not subsequently edited)
                    return {
                        parameter: {
                            name: item.originalEntity.parameter.name,
                            sensitive: item.originalEntity.parameter.sensitive,
                            description: item.originalEntity.parameter.description,
                            value: item.originalEntity.parameter.value,
                            valueRemoved: item.originalEntity.parameter.valueRemoved,
                            referencedAssets: item.originalEntity.parameter.referencedAssets
                        }
                    };
                }
            });
    }

    selectParameter(item: ParameterItem | null): void {
        this.selectedItem = item;
    }

    isSelected(item: ParameterItem): boolean {
        if (this.selectedItem) {
            return item.originalEntity.parameter.name == this.selectedItem.originalEntity.parameter.name;
        }
        return false;
    }
}
