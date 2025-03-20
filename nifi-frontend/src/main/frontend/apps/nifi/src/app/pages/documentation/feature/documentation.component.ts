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
    afterRender,
    AfterViewInit,
    Component,
    DestroyRef,
    ElementRef,
    inject,
    OnInit,
    viewChild
} from '@angular/core';
import { NiFiState } from '../../../state';
import { Store } from '@ngrx/store';
import { loadExtensionTypesForDocumentation } from '../../../state/extension-types/extension-types.actions';
import {
    selectControllerServiceTypes,
    selectFlowAnalysisRuleTypes,
    selectParameterProviderTypes,
    selectProcessorTypes,
    selectReportingTaskTypes
} from '../../../state/extension-types/extension-types.selectors';
import { ComponentType, isDefinedAndNotNull, NiFiCommon, selectCurrentRoute } from '@nifi/shared';
import { MatAccordion } from '@angular/material/expansion';
import { FormBuilder, FormControl, FormGroup } from '@angular/forms';
import { debounceTime, distinctUntilChanged, map } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { DocumentedType } from '../../../state/shared';
import {
    selectDefinitionCoordinatesFromRoute,
    selectOverviewFromRoute
} from '../state/documentation/documentation.selectors';
import { DefinitionCoordinates } from '../state';
import { navigateToOverview } from '../state/documentation/documentation.actions';
import { concatLatestFrom } from '@ngrx/operators';

@Component({
    selector: 'documentation',
    templateUrl: './documentation.component.html',
    styleUrls: ['./documentation.component.scss'],
    standalone: false
})
export class Documentation implements OnInit, AfterViewInit {
    private destroyRef: DestroyRef = inject(DestroyRef);

    processorTypes$ = this.store
        .select(selectProcessorTypes)
        .pipe(map((extensionTypes) => this.sortExtensions(extensionTypes)));
    controllerServiceTypes$ = this.store
        .select(selectControllerServiceTypes)
        .pipe(map((extensionTypes) => this.sortExtensions(extensionTypes)));
    reportingTaskTypes$ = this.store
        .select(selectReportingTaskTypes)
        .pipe(map((extensionTypes) => this.sortExtensions(extensionTypes)));
    parameterProviderTypes$ = this.store
        .select(selectParameterProviderTypes)
        .pipe(map((extensionTypes) => this.sortExtensions(extensionTypes)));
    flowAnalysisRuleTypes$ = this.store
        .select(selectFlowAnalysisRuleTypes)
        .pipe(map((extensionTypes) => this.sortExtensions(extensionTypes)));

    accordion = viewChild.required(MatAccordion);

    filterForm: FormGroup;
    filter: string | null = null;

    private selectedCoordinates: DefinitionCoordinates | null = null;
    private isOverviewRoute: boolean = false;
    private scrolledIntoView = false;

    generalExpanded = true;
    processorsExpanded = false;
    controllerServicesExpanded = false;
    reportingTasksExpanded = false;
    parameterProvidersExpanded = false;

    constructor(
        private store: Store<NiFiState>,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon,
        private documentation: ElementRef
    ) {
        this.store
            .select(selectDefinitionCoordinatesFromRoute)
            .pipe(
                distinctUntilChanged(
                    (a, b) =>
                        a?.group === b?.group &&
                        a?.artifact === b?.artifact &&
                        a?.version === b?.version &&
                        a?.type === b?.type
                ),
                isDefinedAndNotNull(),
                takeUntilDestroyed(),
                concatLatestFrom(() => this.store.select(selectCurrentRoute))
            )
            .subscribe(([coordinates, currentRoute]) => {
                this.selectedCoordinates = coordinates;
                this.isOverviewRoute = false;

                // ensure the panel that the defined component is in is expanded
                switch (currentRoute.url[0].path) {
                    case ComponentType.Processor:
                        this.processorsExpanded = true;
                        break;
                    case ComponentType.ControllerService:
                        this.controllerServicesExpanded = true;
                        break;
                    case ComponentType.ReportingTask:
                        this.reportingTasksExpanded = true;
                        break;
                    case ComponentType.ParameterProvider:
                        this.parameterProvidersExpanded = true;
                        break;
                }
            });

        this.store
            .select(selectCurrentRoute)
            .pipe(
                isDefinedAndNotNull(),
                takeUntilDestroyed(),
                distinctUntilChanged((a, b) => a === b)
            )
            .subscribe(() => {
                this.scrolledIntoView = false;
            });

        this.store
            .select(selectOverviewFromRoute)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((isOverviewRoute) => {
                this.isOverviewRoute = isOverviewRoute;
                this.selectedCoordinates = null;
                this.generalExpanded = isOverviewRoute;
            });

        this.filterForm = this.formBuilder.group({
            filter: new FormControl(null)
        });

        afterRender(() => {
            if (!this.scrolledIntoView) {
                const selectedType = this.documentation.nativeElement.querySelector('a.selected');
                if (selectedType) {
                    selectedType.scrollIntoView({
                        block: 'center'
                    });
                    this.scrolledIntoView = true;
                }
            }
        });
    }

    ngOnInit(): void {
        this.filterForm
            .get('filter')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filter: string) => {
                this.applyFilter(filter);
            });

        this.store.dispatch(loadExtensionTypesForDocumentation());
    }

    ngAfterViewInit(): void {
        if (!this.isOverviewRoute && !this.selectedCoordinates) {
            this.scrolledIntoView = true;
            this.store.dispatch(navigateToOverview());
        }
    }

    private applyFilter(filter: string | null): void {
        this.accordion().openAll();
        this.filter = filter;
    }

    extensionTypeTrackBy(index: number, extensionType: DocumentedType): string {
        const coordinates: DefinitionCoordinates = {
            group: extensionType.bundle.group,
            artifact: extensionType.bundle.artifact,
            version: extensionType.bundle.version,
            type: extensionType.type
        };
        return JSON.stringify(coordinates);
    }

    filterGeneral(name: string): boolean {
        if (this.filter) {
            return this.nifiCommon.stringContains(name, this.filter, true);
        }
        return true;
    }

    private sortExtensions(extensionTypes: DocumentedType[]): DocumentedType[] {
        return extensionTypes.slice().sort((a, b) => {
            return this.nifiCommon.compareString(this.getExtensionName(a.type), this.getExtensionName(b.type));
        });
    }

    filterExtensions(extensionTypes: DocumentedType[]): DocumentedType[] {
        if (this.filter) {
            return extensionTypes.filter((extensionType) =>
                this.nifiCommon.stringContains(extensionType.type, this.filter, true)
            );
        } else {
            return extensionTypes;
        }
    }

    isOverviewSelected(): boolean {
        return this.isOverviewRoute;
    }

    isComponentSelected(extensionType: DocumentedType): boolean {
        if (this.selectedCoordinates) {
            return (
                this.selectedCoordinates.group === extensionType.bundle.group &&
                this.selectedCoordinates.artifact === extensionType.bundle.artifact &&
                this.selectedCoordinates.version === extensionType.bundle.version &&
                this.selectedCoordinates.type === extensionType.type
            );
        }
        return false;
    }

    getExtensionName(extensionType: string): string {
        return this.nifiCommon.getComponentTypeLabel(extensionType);
    }

    protected readonly ComponentType = ComponentType;
}
