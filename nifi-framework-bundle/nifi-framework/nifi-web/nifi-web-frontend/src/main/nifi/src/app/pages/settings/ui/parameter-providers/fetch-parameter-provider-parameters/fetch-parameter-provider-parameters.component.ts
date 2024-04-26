/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, DestroyRef, inject, Inject, Input, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { ErrorBanner } from '../../../../../ui/common/error-banner/error-banner.component';
import { MatButtonModule } from '@angular/material/button';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import {
    FetchedParameterMapping,
    FetchParameterProviderDialogRequest,
    ParameterGroupConfiguration,
    ParameterProviderApplyParametersRequest,
    ParameterProviderEntity,
    ParameterProviderParameterApplicationEntity,
    ParameterProvidersState,
    ParameterSensitivity,
    ParameterStatusEntity
} from '../../../state/parameter-providers';
import { debounceTime, Observable, Subject } from 'rxjs';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { NifiTooltipDirective } from '../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import { ParameterGroupsTable } from './parameter-groups-table/parameter-groups-table.component';
import { MatCheckboxChange, MatCheckboxModule } from '@angular/material/checkbox';
import { MatInputModule } from '@angular/material/input';
import { ParameterReferences } from '../../../../../ui/common/parameter-references/parameter-references.component';
import { AffectedComponentEntity } from '../../../../../state/shared';
import * as ParameterProviderActions from '../../../state/parameter-providers/parameter-providers.actions';
import { Store } from '@ngrx/store';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { PipesModule } from '../../../../../pipes/pipes.module';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';

@Component({
    selector: 'fetch-parameter-provider-parameters',
    standalone: true,
    imports: [
        CommonModule,
        MatDialogModule,
        ReactiveFormsModule,
        ErrorBanner,
        MatButtonModule,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        MatTableModule,
        MatSortModule,
        ParameterGroupsTable,
        MatCheckboxModule,
        MatInputModule,
        ParameterReferences,
        PipesModule
    ],
    templateUrl: './fetch-parameter-provider-parameters.component.html',
    styleUrls: ['./fetch-parameter-provider-parameters.component.scss']
})
export class FetchParameterProviderParameters implements OnInit {
    fetchParametersForm: FormGroup;
    parameterProvider: ParameterProviderEntity;
    selectedParameterGroup: ParameterGroupConfiguration | null = null;
    parameterGroupConfigurations: ParameterGroupConfiguration[];
    parameterGroupNames: string[] = [];
    parameterContextsToCreate: { [key: string]: string } = {};
    parameterContextsToUpdate: string[] = [];

    displayedColumns = ['sensitive', 'name', 'indicators'];

    // each group has a different set of parameters, map by groupName
    dataSources: { [key: string]: MatTableDataSource<FetchedParameterMapping> } = {};

    // each group's parameter table can have a different sort active, map by groupName
    activeSorts: { [key: string]: Sort } = {};

    // each group can have a selected parameter, map by groupName
    selectedParameters: { [key: string]: FetchedParameterMapping } = {};

    // as the selected parameter of the current group changes
    selectParameterChanged: Subject<FetchedParameterMapping | null> = new Subject<FetchedParameterMapping | null>();

    parameterReferences: AffectedComponentEntity[] = [];

    @Input() saving$!: Observable<boolean>;
    @Input() updateRequest!: Observable<ParameterProviderApplyParametersRequest | null>;

    protected readonly TextTip = TextTip;
    protected readonly Object = Object;

    private destroyRef: DestroyRef = inject(DestroyRef);

    constructor(
        private formBuilder: FormBuilder,
        private clusterConnectionService: ClusterConnectionService,
        private nifiCommon: NiFiCommon,
        private store: Store<ParameterProvidersState>,
        @Inject(MAT_DIALOG_DATA) public request: FetchParameterProviderDialogRequest
    ) {
        this.parameterProvider = request.parameterProvider;

        this.fetchParametersForm = this.formBuilder.group({});
        this.parameterGroupConfigurations = this.parameterProvider.component.parameterGroupConfigurations.slice();
        this.parameterGroupConfigurations.forEach((parameterGroupConfig) => {
            const params = this.getParameterSensitivitiesAsFormControls(parameterGroupConfig);
            this.fetchParametersForm.addControl(
                parameterGroupConfig.groupName,
                this.formBuilder.group({
                    createParameterContext: new FormControl(),
                    parameterContextName: new FormControl(
                        parameterGroupConfig.parameterContextName,
                        Validators.required
                    ),
                    parameterSensitivities: this.formBuilder.group(params)
                })
            );
            this.parameterGroupNames.push(parameterGroupConfig.groupName);
        });

        if (this.parameterProvider.component.referencingParameterContexts) {
            this.parameterContextsToUpdate = this.parameterProvider.component.referencingParameterContexts
                .map((parameterContext) => parameterContext.component?.name ?? '')
                .filter((name) => name.length > 0);
        }
    }

    ngOnInit(): void {
        this.selectParameterChanged.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((selectedParameter) => {
            const parameterGroupName = this.selectedParameterGroup?.groupName;
            if (selectedParameter) {
                // keep track of the currently selected parameter for each group
                if (parameterGroupName) {
                    this.selectedParameters[parameterGroupName] = selectedParameter;

                    this.parameterReferences =
                        selectedParameter.status?.parameter?.parameter.referencingComponents ?? [];
                }
            } else {
                if (parameterGroupName) {
                    delete this.selectedParameters[parameterGroupName];
                    this.parameterReferences = [];
                }
            }
        });

        if (this.parameterGroupConfigurations.length > 0) {
            // select the first parameter group
            const initialParamGroup = this.parameterGroupConfigurations[0];

            // preload the first datasource into the map
            this.getParameterMappingDataSource(initialParamGroup);
            this.autoSelectParameter();

            // watch for changes to the parameter context name inputs, update the local map
            this.parameterGroupConfigurations.forEach((groupConfig) => {
                this.fetchParametersForm
                    .get(`${groupConfig.groupName}.parameterContextName`)
                    ?.valueChanges.pipe(debounceTime(200), takeUntilDestroyed(this.destroyRef))
                    .subscribe((name) => {
                        if (Object.hasOwn(this.parameterContextsToCreate, groupConfig.groupName)) {
                            this.parameterContextsToCreate[groupConfig.groupName] = name;
                        }
                    });
            });
        }
    }

    submitForm() {
        const data = this.getFormData();
        this.store.dispatch(
            ParameterProviderActions.submitParameterProviderParametersUpdateRequest({
                request: data
            })
        );
    }

    parameterGroupSelected(parameterGroup: ParameterGroupConfiguration) {
        this.selectedParameterGroup = parameterGroup;
        this.autoSelectParameter();
    }

    private autoSelectParameter() {
        if (this.selectedParameterGroup) {
            const selectedParam = this.selectedParameters[this.selectedParameterGroup.groupName];
            if (selectedParam) {
                this.selectParameterChanged.next(selectedParam);
            } else {
                // select the first param
                const paramsDs = this.dataSources[this.selectedParameterGroup.groupName];
                if (paramsDs) {
                    this.selectParameterChanged.next(paramsDs.data[0]);
                }
            }
        }
    }

    canCreateParameterContext(parameterGroupConfig: ParameterGroupConfiguration): boolean {
        // the passed in parameter group could have changed its sync status due to user input,
        // check the original parameter group from the server.
        const originalParameterGroupConfig = this.parameterProvider.component.parameterGroupConfigurations.find(
            (g) => g.groupName === parameterGroupConfig.groupName
        );
        if (originalParameterGroupConfig) {
            return !this.isSynced(originalParameterGroupConfig);
        }
        return false;
    }

    canEditParameterContextName(parameterGroupConfig: ParameterGroupConfiguration): boolean {
        // can only edit the context name if the create parameter context checkbox is checked.
        return this.fetchParametersForm.get(`${parameterGroupConfig.groupName}.createParameterContext`)?.value;
    }

    showParameterList(parameterGroupConfig: ParameterGroupConfiguration): boolean {
        // show only a list of parameters if the group is not synced with a parameter context and the user isn't actively trying to create a context for it
        return (
            !this.isSynced(parameterGroupConfig) &&
            !this.fetchParametersForm.get(`${parameterGroupConfig.groupName}.createParameterContext`)?.value
        );
    }

    isSynced(parameterGroupConfig: ParameterGroupConfiguration): boolean {
        return !!parameterGroupConfig.synchronized;
    }

    getParameterMappingDataSource(parameterGroupConfig: ParameterGroupConfiguration) {
        if (!this.dataSources[parameterGroupConfig.groupName]) {
            const ds = new MatTableDataSource<FetchedParameterMapping>();

            ds.data = this.sortEntities(
                this.parameterMappingArray(parameterGroupConfig),
                this.getActiveSort(parameterGroupConfig)
            );

            this.dataSources[parameterGroupConfig.groupName] = ds;
        }
        return this.dataSources[parameterGroupConfig.groupName];
    }

    getActiveSort(parameterGroupConfig: ParameterGroupConfiguration): Sort {
        if (!this.activeSorts[parameterGroupConfig.groupName]) {
            this.activeSorts[parameterGroupConfig.groupName] = {
                active: 'name',
                direction: 'asc'
            };
        }
        return this.activeSorts[parameterGroupConfig.groupName];
    }

    setActiveSort(sort: Sort, parameterGroupConfig: ParameterGroupConfiguration) {
        this.activeSorts[parameterGroupConfig.groupName] = sort;
    }

    getFormControl(parameter: ParameterSensitivity, parameterGroupConfig: ParameterGroupConfiguration): FormControl {
        return this.fetchParametersForm.get(
            `${parameterGroupConfig.groupName}.parameterSensitivities.${parameter.name}`
        ) as FormControl;
    }

    private getParameterMapping(parameterGroupConfig: ParameterGroupConfiguration): {
        [key: string]: FetchedParameterMapping;
    } {
        const map: { [key: string]: FetchedParameterMapping } = {};

        // get all the parameter status for the selected group, add them to the map by param name
        if (this.parameterProvider.component.parameterStatus) {
            this.parameterProvider.component.parameterStatus
                .filter((parameterStatus: ParameterStatusEntity) => {
                    if (!parameterStatus?.parameter?.parameter) {
                        return false;
                    }
                    const param = parameterStatus.parameter.parameter;
                    return param.parameterContext?.component?.name === parameterGroupConfig.parameterContextName;
                })
                .forEach((parameterStatus: ParameterStatusEntity) => {
                    if (parameterStatus.parameter) {
                        const parameterName = parameterStatus.parameter.parameter.name;
                        map[parameterName] = {
                            name: parameterName,
                            status: parameterStatus,
                            sensitivity: {
                                name: parameterName,
                                sensitive: parameterStatus.parameter.parameter.sensitive
                            }
                        } as FetchedParameterMapping;
                    }
                });
        }

        // get all the known parameter sensitivities, add them to the map by param name
        if (parameterGroupConfig.parameterSensitivities) {
            Object.entries(parameterGroupConfig.parameterSensitivities).forEach((entry) => {
                const parameterName = entry[0];
                if (map[parameterName]) {
                    map[parameterName].sensitivity = {
                        name: parameterName,
                        sensitive: entry[1] !== 'NON_SENSITIVE'
                    };
                } else {
                    map[parameterName] = {
                        name: parameterName,
                        sensitivity: {
                            name: parameterName,
                            sensitive: entry[1] !== 'NON_SENSITIVE'
                        }
                    } as FetchedParameterMapping;
                }
            });
        }

        Object.entries(map).forEach((entry) => {
            const paramName = entry[0];
            const mapping: FetchedParameterMapping = entry[1];

            mapping.name = paramName;
            if (!mapping.sensitivity) {
                // no known sensitivity, provide one
                mapping.sensitivity = {
                    name: paramName,
                    sensitive: true
                };
            }

            if (!mapping.status) {
                mapping.status = {
                    status: 'UNCHANGED'
                };
            }
        });

        return map;
    }

    private parameterMappingArray(parameterGroupConfig: ParameterGroupConfiguration): FetchedParameterMapping[] {
        return Object.values(this.getParameterMapping(parameterGroupConfig));
    }

    private getParameterSensitivitiesAsFormControls(parameterGroupConfig: ParameterGroupConfiguration): {
        [key: string]: FormControl;
    } {
        const data: { [key: string]: FormControl } = {};
        Object.entries(this.getParameterMapping(parameterGroupConfig)).forEach((entry) => {
            const parameterName = entry[0];
            const param: FetchedParameterMapping = entry[1];
            if (parameterName && param?.sensitivity) {
                data[parameterName] = new FormControl({
                    value: param.sensitivity.sensitive,
                    disabled: this.isReferenced(param)
                });
            }
        });
        return data;
    }

    sort(sort: Sort) {
        this.setActiveSort(sort, this.selectedParameterGroup!);
        const dataSource: MatTableDataSource<FetchedParameterMapping> = this.getParameterMappingDataSource(
            this.selectedParameterGroup!
        );
        dataSource.data = this.sortEntities(dataSource.data, sort);
    }

    private sortEntities(data: FetchedParameterMapping[], sort: Sort): FetchedParameterMapping[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            const retVal = this.nifiCommon.compareString(a.name, b.name);
            return retVal * (isAsc ? 1 : -1);
        });
    }

    selectAllChanged(event: MatCheckboxChange) {
        const checked: boolean = event.checked;
        const currentParamGroup = this.selectedParameterGroup!;
        const dataSource = this.getParameterMappingDataSource(currentParamGroup);
        dataSource.data.forEach((p) => {
            if (p.sensitivity) {
                const formControl = this.getFormControl(p.sensitivity, currentParamGroup);
                if (formControl && !formControl.disabled) {
                    formControl.setValue(checked);
                    formControl.markAsDirty();
                }
            }
        });
    }

    areAllSelected(parameterGroupConfig: ParameterGroupConfiguration): boolean {
        const dataSource = this.getParameterMappingDataSource(parameterGroupConfig);
        let allSensitive = true;
        dataSource.data.forEach((p) => {
            if (p.sensitivity) {
                const formControl = this.getFormControl(p.sensitivity, parameterGroupConfig);
                if (formControl) {
                    allSensitive = allSensitive && formControl.value;
                }
            }
        });

        return allSensitive;
    }

    areAnySelected(parameterGroupConfig: ParameterGroupConfiguration): boolean {
        const dataSource = this.getParameterMappingDataSource(parameterGroupConfig);
        let anySensitive = false;
        let allSensitive = true;
        dataSource.data.forEach((p) => {
            if (p.sensitivity) {
                const formControl = this.getFormControl(p.sensitivity, parameterGroupConfig);
                if (formControl) {
                    anySensitive = anySensitive || formControl.value;
                    allSensitive = allSensitive && formControl.value;
                }
            }
        });

        return anySensitive && !allSensitive;
    }

    isReferenced(item: FetchedParameterMapping): boolean {
        const parameterStatus = item.status;
        if (!parameterStatus?.parameter) {
            return false;
        }
        const hasReferencingComponents = parameterStatus?.parameter.parameter.referencingComponents?.length;
        return !!hasReferencingComponents;
    }

    isAffected(item: FetchedParameterMapping): boolean {
        const parameterStatus = item.status;
        if (!parameterStatus) {
            return false;
        }
        return parameterStatus.status !== 'UNCHANGED';
    }

    getAffectedTooltip(item: FetchedParameterMapping): string | null {
        switch (item.status?.status) {
            case 'NEW':
                return 'Newly discovered parameter.';
            case 'CHANGED':
                return 'Value has changed.';
            case 'REMOVED':
                return 'Parameter has been removed from its source. Apply to remove from the synced parameter context.';
            case 'MISSING_BUT_REFERENCED':
                return 'Parameter has been removed from its source and is still being referenced in a component. To remove the parameter from the parameter context, first un-reference the parameter, then re-fetch and apply.';
        }
        return null;
    }

    createParameterContextToggled(event: MatCheckboxChange) {
        const checked: boolean = event.checked;
        const currentParamGroup = this.selectedParameterGroup!;
        this.parameterGroupConfigurations = this.parameterGroupConfigurations.map((config) => {
            if (config.groupName === currentParamGroup.groupName) {
                config = {
                    ...config,
                    synchronized: checked
                };
                if (checked) {
                    this.parameterContextsToCreate[config.groupName] = config.parameterContextName;

                    // preload the datasource into the map
                    this.getParameterMappingDataSource(currentParamGroup);
                    this.autoSelectParameter();
                } else {
                    delete this.parameterContextsToCreate[config.groupName];
                    // set the selected parameter to nothing
                    this.removeParameterSelection();
                }
            }
            return config;
        });
    }

    selectParameter(item: FetchedParameterMapping) {
        this.selectParameterChanged.next(item);
    }

    isParameterSelected(item: FetchedParameterMapping): boolean {
        const parameterGroupName = this.selectedParameterGroup?.groupName;
        if (!parameterGroupName) {
            return false;
        }

        const selectedParameter = this.selectedParameters[parameterGroupName];
        if (!selectedParameter) {
            return false;
        }

        if (
            selectedParameter.name === item.name &&
            selectedParameter.status?.parameter?.parameter.parameterContext?.id ===
                item.status?.parameter?.parameter.parameterContext?.id
        ) {
            return true;
        }
        return false;
    }

    private removeParameterSelection() {
        this.selectParameterChanged.next(null);
    }

    canSubmitForm(): boolean {
        // user needs to have read/write permissions on the component
        const referencingParameterContexts = this.parameterProvider.component.referencingParameterContexts;
        if (referencingParameterContexts?.length > 0) {
            // disable the submit if one of the referenced parameter contexts is not readable/writeable
            const canReadWriteAllParamContexts = referencingParameterContexts.every(
                (paramContextRef) => paramContextRef.permissions.canRead && paramContextRef.permissions.canWrite
            );
            if (!canReadWriteAllParamContexts) {
                return false;
            }
        }

        const affectedComponents = this.parameterProvider.component.affectedComponents;
        if (affectedComponents?.length > 0) {
            // disable the submit if one of the affected components is not readable/writeable
            const canReadWriteAllAffectedComponents = affectedComponents.every(
                (affected) => affected.permissions.canRead && affected.permissions.canWrite
            );
            if (!canReadWriteAllAffectedComponents) {
                return false;
            }
        }

        // check if a parameter is new, removed, missing but referenced, or has a changed value
        const parameterStatus = this.parameterProvider.component.parameterStatus;
        let anyParametersChangedInProvider = false;
        if (parameterStatus && parameterStatus.length > 0) {
            anyParametersChangedInProvider = parameterStatus.some((paramStatus) => {
                return paramStatus.status !== 'UNCHANGED';
            });
        }

        // if a fetched parameter is new, removed, missing but referenced, or has a changed value... consider the form dirty.
        const isDirty = anyParametersChangedInProvider || this.fetchParametersForm.dirty;

        return isDirty && !this.fetchParametersForm.invalid;
    }

    private getFormData(): ParameterProviderParameterApplicationEntity {
        const groupConfigs: ParameterGroupConfiguration[] = this.parameterGroupConfigurations
            .filter((initialGroup) => {
                // filter out any non-synchronized groups that the user hasn't decided to create a parameter context for
                const createParameterContext = this.fetchParametersForm.get(
                    `${initialGroup.groupName}.createParameterContext`
                );
                return initialGroup.synchronized || !!createParameterContext?.value;
            })
            .map((initialGroup) => {
                const parameterSensitivities: { [key: string]: null | 'SENSITIVE' | 'NON_SENSITIVE' } = {};

                const parameterContextName = this.fetchParametersForm.get(
                    `${initialGroup.groupName}.parameterContextName`
                )?.value;

                // convert to the backend model for sensitivities
                Object.entries(initialGroup.parameterSensitivities).forEach(([key, value]) => {
                    const formParamSensitivity = this.fetchParametersForm.get(
                        `${initialGroup.groupName}.parameterSensitivities.${key}`
                    );
                    if (formParamSensitivity) {
                        parameterSensitivities[key] = formParamSensitivity.value ? 'SENSITIVE' : 'NON_SENSITIVE';
                    } else {
                        // if there is no value defined by the form, send the known value
                        parameterSensitivities[key] = value;
                    }
                });

                return {
                    ...initialGroup,
                    parameterContextName,
                    parameterSensitivities
                };
            });

        return {
            id: this.parameterProvider.id,
            revision: this.parameterProvider.revision,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            parameterGroupConfigurations: groupConfigs
        };
    }
}
