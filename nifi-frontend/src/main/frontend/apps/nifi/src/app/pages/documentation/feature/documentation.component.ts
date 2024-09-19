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
import { ComponentType, isDefinedAndNotNull, NiFiCommon } from '@nifi/shared';
import { MatAccordion } from '@angular/material/expansion';
import { FormBuilder, FormControl, FormGroup } from '@angular/forms';
import { debounceTime, distinctUntilChanged } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { DocumentedType } from '../../../state/shared';
import {
    selectDefinitionCoordinatesFromRoute,
    selectOverviewFromRoute
} from '../state/documentation/documentation.selectors';
import { DefinitionCoordinates } from '../state';
import { navigateToOverview } from '../state/documentation/documentation.actions';

@Component({
    selector: 'documentation',
    templateUrl: './documentation.component.html',
    styleUrls: ['./documentation.component.scss']
})
export class Documentation implements OnInit, AfterViewInit {
    private destroyRef: DestroyRef = inject(DestroyRef);

    processorTypes = this.store.selectSignal(selectProcessorTypes);
    controllerServiceTypes = this.store.selectSignal(selectControllerServiceTypes);
    reportingTaskTypes = this.store.selectSignal(selectReportingTaskTypes);
    parameterProviderTypes = this.store.selectSignal(selectParameterProviderTypes);
    flowAnalysisRuleTypes = this.store.selectSignal(selectFlowAnalysisRuleTypes);

    accordion = viewChild.required(MatAccordion);

    filterForm: FormGroup;
    filter: string | null = null;

    private selectedCoordinates: DefinitionCoordinates | null = null;
    private isOverviewRoute: boolean = false;
    private scrolledIntoView = false;

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
                takeUntilDestroyed()
            )
            .subscribe((coordinates) => {
                this.selectedCoordinates = coordinates;
                this.isOverviewRoute = false;
            });

        this.store
            .select(selectOverviewFromRoute)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((isOverviewRoute) => {
                this.isOverviewRoute = isOverviewRoute;
                this.selectedCoordinates = null;
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
        this.applyFilter(null);
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
        return this.nifiCommon.substringAfterLast(extensionType, '.');
    }

    protected readonly ComponentType = ComponentType;
}
